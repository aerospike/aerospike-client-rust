-- Copyright 2026 Aerospike, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License"); you may not
-- use this file except in compliance with the License. You may obtain a copy of
-- the License at http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
-- License for the specific language governing permissions and limitations under
-- the License.

-- ===========================================================================
-- vector.lua - the Lua counterpart of the native `vector` module.
--
-- The same job in the other engine: exact top-k over packed float vectors,
-- bounded ordering, and the same three record functions. It exists so the two
-- can be timed against each other on the same records, so everything here is
-- deliberately the *same algorithm* rather than a Lua-flavoured rewrite of it:
--
--   * the query vector, the format string and the metric closure are resolved
--     once per stream, which is what `configure` does on the native side;
--   * scores are inverted for `dot` exactly as `Metric::score` does, so the
--     bounded structure only ever knows that larger is worse;
--   * ties break on the digest, so a top-k over equal distances is a function
--     of the data and not of which thread got there first;
--   * a candidate that cannot beat the worst kept hit never has its digest
--     read, which is the rejection that makes the whole thing O(k).
--
-- Two things could not be mirrored, and both are properties of the engine
-- rather than choices:
--
-- **There is no per-record float decode.** Lua sees a blob bin as `bytes`, and
-- `bytes` has integer getters only - no `get_float`. So a packed vector has to
-- come across as a Lua string (`bytes.get_string`, one alloc and one copy per
-- record) and be decoded with `string.unpack`, which needs Lua 5.4. `topk`
-- unpacks a whole vector in one call; `topk_scan` unpacks element by element,
-- which is what anyone writes first. The gap between those two rows is call
-- overhead alone, on identical arithmetic.
--
-- **There is no finalize.** A Lua stream aggregation ends at `reduce`, and the
-- value that leaves the server has to be an `as_val` - so the accumulator's
-- plain Lua table is converted by a `map` stage, and what crosses the wire is a
-- *partial* (`score`, not `distance`), one per stream the server ran. The
-- caller merges them. The native module's `finalize` runs after its own
-- `merge`, so it ships one answer per node instead.
--
--   topk("v", <query blob>, "cosine", 10)   the 10 nearest, exactly
--   orderby("p", "asc", 20)                 the 20 smallest by a numeric bin
--   vec_distance("v", <query blob>, "dot")  one record, one number
--
-- Element types are f32 and f64 only. The native module also does f16, bf16, i8
-- and i32; half precision cannot be decoded in Lua without doing IEEE-754 by
-- hand, and pretending otherwise would make a benchmark row that measured the
-- wrong thing.
-- ===========================================================================

local sqrt = math.sqrt
local unpack_ = string.unpack
local rep = string.rep
local pack = table.pack

-- Bytes per element and the `string.unpack` code for it.
local DTYPES = {
    f32 = { code = "f", size = 4 },
    float = { code = "f", size = 4 },
    float32 = { code = "f", size = 4 },
    f64 = { code = "d", size = 8 },
    double = { code = "d", size = 8 },
    float64 = { code = "d", size = 8 },
}

-- A blob bin or a blob argument, as a Lua string.
--
-- One `cf_calloc` plus one `memcpy` of the whole vector per record, inside
-- `bytes.get_string`. The native module borrows the particle in place instead,
-- and this is the copy it avoids - see the `probe_read` rows.
local function to_string(b)
    if b == nil then
        return nil
    end

    if type(b) == "string" then
        return b
    end

    local n = bytes.size(b)

    if n == nil or n == 0 then
        return nil
    end

    return bytes.get_string(b, 1, n)
end

-- A digest string back into a blob, for the wire.
local function to_blob(s)
    local b = bytes(#s)

    bytes.set_string(b, 1, s)

    return b
end

-- Whether a *smaller* number means a better match. `dot` is a similarity; every
-- other metric here is a distance. Getting this backwards returns the k worst
-- matches and looks entirely plausible doing it.
local function canonical_metric(metric)
    if metric == nil or metric == "cosine" then
        return "cosine"
    elseif metric == "dot" or metric == "ip" or metric == "inner_product" then
        return "dot"
    elseif metric == "sqeuclidean" or metric == "l2sq" then
        return "sqeuclidean"
    elseif metric == "euclidean" or metric == "l2" then
        return "euclidean"
    end

    error("unknown metric " .. tostring(metric) ..
          " - expected cosine, dot, sqeuclidean or euclidean")
end

-- ---------------------------------------------------------------------------
-- Everything constant for one query, resolved once.
--
-- The returned table holds the unpacked query, its norm, the expected wire
-- length and *one* measure function chosen by metric and element type - so the
-- per-record path branches on neither, which is the property the native
-- module's resolved function pointer gives it.
-- ---------------------------------------------------------------------------
local function configure(query, metric, dtype, per_element)
    local t = DTYPES[dtype or "f32"]

    if t == nil then
        error("unknown element type " .. tostring(dtype) .. " - expected f32 or f64")
    end

    local qs = to_string(query)

    if qs == nil then
        error("the query vector must be a blob of packed floats")
    end

    if #qs % t.size ~= 0 then
        error("the query blob is " .. #qs .. " bytes, not a whole number of " ..
              t.size .. "-byte elements")
    end

    local dim = #qs // t.size
    local one = "<" .. t.code
    local all = "<" .. rep(t.code, dim)
    local wire = dim * t.size

    local q = pack(unpack_(all, qs))
    local qnorm = 0.0

    for i = 1, dim do
        qnorm = qnorm + q[i] * q[i]
    end

    qnorm = sqrt(qnorm)

    local name = canonical_metric(metric)

    -- Two ways to read the record's vector, and the difference between them is
    -- one of the things this module is here to measure.
    --
    --   batch: one `string.unpack` for the whole vector, one table per record
    --   scan:  one `string.unpack` per element, no table at all
    --
    -- Same arithmetic in both, so the row-to-row delta is call overhead.
    local measure

    if name == "cosine" then
        if per_element then
            measure = function(s)
                local dot, nb, pos = 0.0, 0.0, 1

                for i = 1, dim do
                    local x

                    x, pos = unpack_(one, s, pos)
                    dot = dot + x * q[i]
                    nb = nb + x * x
                end

                return 1.0 - dot / (qnorm * sqrt(nb))
            end
        else
            measure = function(s)
                local v = pack(unpack_(all, s))
                local dot, nb = 0.0, 0.0

                for i = 1, dim do
                    local x = v[i]

                    dot = dot + x * q[i]
                    nb = nb + x * x
                end

                return 1.0 - dot / (qnorm * sqrt(nb))
            end
        end
    elseif name == "dot" then
        if per_element then
            measure = function(s)
                local dot, pos = 0.0, 1

                for i = 1, dim do
                    local x

                    x, pos = unpack_(one, s, pos)
                    dot = dot + x * q[i]
                end

                return dot
            end
        else
            measure = function(s)
                local v = pack(unpack_(all, s))
                local dot = 0.0

                for i = 1, dim do
                    dot = dot + v[i] * q[i]
                end

                return dot
            end
        end
    else
        local root = (name == "euclidean")

        if per_element then
            measure = function(s)
                local acc, pos = 0.0, 1

                for i = 1, dim do
                    local x

                    x, pos = unpack_(one, s, pos)

                    local d = x - q[i]

                    acc = acc + d * d
                end

                if root then
                    return sqrt(acc)
                end

                return acc
            end
        else
            measure = function(s)
                local v = pack(unpack_(all, s))
                local acc = 0.0

                for i = 1, dim do
                    local d = v[i] - q[i]

                    acc = acc + d * d
                end

                if root then
                    return sqrt(acc)
                end

                return acc
            end
        end
    end

    -- A vector held as a list of numbers rather than a packed blob: no decode at
    -- all, but one C call per element to walk the list, and 8 bytes per element
    -- on storage instead of 4.
    local measure_list = function(l)
        local dot, nb, acc, i = 0.0, 0.0, 0.0, 0

        for x in list.iterator(l) do
            i = i + 1

            local y = q[i]

            if y == nil then
                return nil
            end

            dot = dot + x * y
            nb = nb + x * x

            local d = x - y

            acc = acc + d * d
        end

        if i ~= dim then
            return nil
        end

        if name == "cosine" then
            return 1.0 - dot / (qnorm * sqrt(nb))
        elseif name == "dot" then
            return dot
        elseif name == "euclidean" then
            return sqrt(acc)
        end

        return acc
    end

    return {
        dim = dim,
        wire = wire,
        metric = name,
        lower = name ~= "dot",
        measure = measure,
        measure_list = measure_list,
    }
end

-- ---------------------------------------------------------------------------
-- The bounded structure: an array of at most k hits, worst last.
--
-- A heap would be O(log k) per kept candidate against this O(k), but on a scan
-- of any size almost nothing is kept - the threshold check rejects it first - so
-- the constant matters more than the exponent, and an array allocates no nodes.
-- ---------------------------------------------------------------------------

-- Whether a candidate is worth reading its digest for. Not full yet means yes.
local function beats(hits, k, score)
    local n = #hits

    return n < k or score < hits[n].score
end

local function offer(hits, k, score, digest, key)
    local n = #hits
    local i = n

    -- Ties break on the digest, ascending, exactly as `Hit::cmp` does.
    while i > 0 do
        local h = hits[i]

        if h.score < score or (h.score == score and h.digest < digest) then
            break
        end

        hits[i + 1] = h
        i = i - 1
    end

    hits[i + 1] = { score = score, digest = digest, key = key }

    if n + 1 > k then
        hits[n + 1] = nil
    end
end

-- ---------------------------------------------------------------------------
-- The wire shape: a partial, one per stream the server ran.
-- ---------------------------------------------------------------------------
local function to_wire(state)
    local out = map()
    local hits = list()

    for _, h in ipairs(state.hits or {}) do
        local entry = map()

        entry["score"] = h.score
        entry["digest"] = to_blob(h.digest)

        if h.key ~= nil then
            entry["key"] = h.key
        end

        list.append(hits, entry)
    end

    out["hits"] = hits
    out["k"] = state.k or 10
    out["skipped_dim"] = state.skipped_dim or 0
    out["skipped_none"] = state.skipped_none or 0

    return out
end

local function from_wire(m, hits, k)
    local l = m["hits"]

    if l == nil then
        return
    end

    for entry in list.iterator(l) do
        local score = entry["score"]

        if beats(hits, k, score) then
            offer(hits, k, score, to_string(entry["digest"]) or "", entry["key"])
        end
    end
end

-- Merging partials. Associative and commutative, which is what makes the k best
-- of the merge the k best overall.
local function merge_wire(a, b)
    if a == nil then
        return b
    end

    if b == nil then
        return a
    end

    local k = a["k"] or b["k"] or 10
    local hits = {}

    from_wire(a, hits, k)
    from_wire(b, hits, k)

    return to_wire({
        hits = hits,
        k = k,
        skipped_dim = (a["skipped_dim"] or 0) + (b["skipped_dim"] or 0),
        skipped_none = (a["skipped_none"] or 0) + (b["skipped_none"] or 0),
    })
end

-- ---------------------------------------------------------------------------
-- Record functions.
-- ---------------------------------------------------------------------------

-- The distance from this record's vector to `query`. Nil when the record has no
-- vector or one of a different dimensionality - a miss, not an error.
function vec_distance(rec, bin, query, metric, dtype)
    local cfg = configure(query, metric, dtype, false)
    local s = to_string(rec[bin])

    if s == nil or #s ~= cfg.wire then
        return nil
    end

    return cfg.measure(s)
end

-- How many dimensions this record's vector has, or nil if it has none.
function vec_dim(rec, bin, dtype)
    local t = DTYPES[dtype or "f32"]

    if t == nil then
        error("unknown element type " .. tostring(dtype) .. " - expected f32 or f64")
    end

    local s = to_string(rec[bin])

    if s == nil then
        return nil
    end

    return #s // t.size
end

-- The L2 norm of this record's vector.
function vec_norm(rec, bin, dtype)
    local t = DTYPES[dtype or "f32"]

    if t == nil then
        error("unknown element type " .. tostring(dtype) .. " - expected f32 or f64")
    end

    local s = to_string(rec[bin])

    if s == nil then
        return nil
    end

    local one = "<" .. t.code
    local acc, pos = 0.0, 1

    for _ = 1, #s // t.size do
        local x

        x, pos = unpack_(one, s, pos)
        acc = acc + x * x
    end

    return sqrt(acc)
end

-- ---------------------------------------------------------------------------
-- topk - exact nearest neighbours.
-- ---------------------------------------------------------------------------

local function topk_stream(stream, bin, query, metric, k, dtype, per_element, from_list)
    local cfg = configure(query, metric, dtype, per_element)

    k = math.floor(k or 10)

    if k < 1 then
        k = 1
    elseif k > 10000 then
        k = 10000
    end

    local lower = cfg.lower
    local measure = from_list and cfg.measure_list or cfg.measure
    local wire = cfg.wire

    local function accumulate(state, rec)
        local hits = state.hits

        if hits == nil then
            hits = {}
            state.hits = hits
            state.k = k
            state.skipped_dim = 0
            state.skipped_none = 0
        end

        local v = rec[bin]

        if v == nil then
            state.skipped_none = state.skipped_none + 1

            return state
        end

        local measured

        if from_list then
            measured = measure(v)

            if measured == nil then
                state.skipped_dim = state.skipped_dim + 1

                return state
            end
        else
            local s = to_string(v)

            if s == nil then
                state.skipped_none = state.skipped_none + 1

                return state
            end

            if #s ~= wire then
                state.skipped_dim = state.skipped_dim + 1

                return state
            end

            measured = measure(s)
        end

        -- The same inversion the native module uses: below here, larger is
        -- always worse.
        local score = lower and measured or -measured

        -- The cheap rejection. Almost every record on a large scan stops here,
        -- without its digest ever being read.
        if not beats(hits, k, score) then
            return state
        end

        offer(hits, k, score, to_string(record.digest(rec)), nil)

        return state
    end

    return stream : aggregate({}, accumulate) : map(to_wire) : reduce(merge_wire)
end

-- Packed blob vectors, one `string.unpack` per vector. The fast Lua path.
function topk(stream, bin, query, metric, k, dtype)
    return topk_stream(stream, bin, query, metric, k, dtype, false, false)
end

-- The same thing with one `string.unpack` per element - what the obvious Lua
-- implementation looks like before anyone measures it.
function topk_scan(stream, bin, query, metric, k, dtype)
    return topk_stream(stream, bin, query, metric, k, dtype, true, false)
end

-- Vectors held as a list of numbers, which needs no decode but costs a C call
-- per element and twice the storage.
function topk_list(stream, bin, query, metric, k, dtype)
    return topk_stream(stream, bin, query, metric, k, dtype, false, true)
end

-- ---------------------------------------------------------------------------
-- orderby - the same machinery, a scalar key.
--
-- The case where Lua has no float decoding to do at all: one bin read and one
-- comparison per record. Whatever separates the engines here is the engine.
-- ---------------------------------------------------------------------------
function orderby(stream, bin, direction, limit)
    local ascending

    if direction == nil or direction == "asc" or direction == "ascending" then
        ascending = true
    elseif direction == "desc" or direction == "descending" then
        ascending = false
    else
        error("direction must be asc or desc, got " .. tostring(direction))
    end

    limit = math.floor(limit or 10)

    if limit < 1 then
        limit = 1
    elseif limit > 100000 then
        limit = 100000
    end

    local function accumulate(state, rec)
        local hits = state.hits

        if hits == nil then
            hits = {}
            state.hits = hits
            state.k = limit
            state.skipped_dim = 0
            state.skipped_none = 0
        end

        local v = rec[bin]

        if type(v) ~= "number" then
            state.skipped_none = state.skipped_none + 1

            return state
        end

        local score = ascending and v or -v

        if not beats(hits, limit, score) then
            return state
        end

        offer(hits, limit, score, to_string(record.digest(rec)), nil)

        return state
    end

    return stream : aggregate({}, accumulate) : map(to_wire) : reduce(merge_wire)
end

-- ---------------------------------------------------------------------------
-- Stage probes - where the per-record time actually goes.
--
-- Each does strictly less work than the next, and each is its own function
-- rather than one function with a mode argument, so nothing in the loop being
-- measured branches on the mode. Same names, same arguments and same deltas as
-- the native module's probes, so the two attribution tables line up row for
-- row.
--
--   probe_count            the query, the record pipeline and the Lua call
--   probe_read  - count    bytes.get_string: one alloc and one copy per record
--   probe_unpack - read    string.unpack of the whole vector into a table
--   probe_dist  - read     unpack and measure - topk without the bounded array
-- ---------------------------------------------------------------------------

local function probe_stream(stream, accumulate)
    local function wire(state)
        local out = map()

        out["seen"] = state.seen or 0
        out["bytes"] = state.bytes or 0

        return out
    end

    local function merge(a, b)
        if a == nil then
            return b
        end

        if b == nil then
            return a
        end

        local out = map()

        out["seen"] = (a["seen"] or 0) + (b["seen"] or 0)
        out["bytes"] = (a["bytes"] or 0) + (b["bytes"] or 0)

        return out
    end

    return stream : aggregate({}, accumulate) : map(wire) : reduce(merge)
end

function probe_count(stream, bin, query, metric, dtype)
    return probe_stream(stream, function(state, rec)
        state.seen = (state.seen or 0) + 1

        return state
    end)
end

-- The same as probe_count, except that it *configures* first - unpacking the
-- query vector and building the format string. The native module resolves that
-- once per query thread; a Lua stream function runs once per stream, and the
-- server runs one stream per partition slice, so this row prices the setup
-- 4096 times over. Whatever it costs, `topk` is paying it too.
function probe_configure(stream, bin, query, metric, dtype)
    local cfg = configure(query, metric, dtype, false)

    return probe_stream(stream, function(state, rec)
        state.seen = (state.seen or 0) + 1
        state.bytes = (state.bytes or 0) + cfg.dim

        return state
    end)
end

function probe_read(stream, bin, query, metric, dtype)
    return probe_stream(stream, function(state, rec)
        state.seen = (state.seen or 0) + 1

        local s = to_string(rec[bin])

        if s ~= nil then
            state.bytes = (state.bytes or 0) + #s
        end

        return state
    end)
end

function probe_unpack(stream, bin, query, metric, dtype)
    local cfg = configure(query, metric, dtype, false)
    local t = DTYPES[dtype or "f32"]
    local all = "<" .. rep(t.code, cfg.dim)

    return probe_stream(stream, function(state, rec)
        state.seen = (state.seen or 0) + 1

        local s = to_string(rec[bin])

        if s ~= nil and #s == cfg.wire then
            local v = pack(unpack_(all, s))

            -- Used, so nothing here can be dead.
            state.bytes = (state.bytes or 0) + v.n
        end

        return state
    end)
end

function probe_dist(stream, bin, query, metric, dtype)
    local cfg = configure(query, metric, dtype, false)

    return probe_stream(stream, function(state, rec)
        state.seen = (state.seen or 0) + 1

        local s = to_string(rec[bin])

        if s ~= nil and #s == cfg.wire then
            state.bytes = (state.bytes or 0) + cfg.measure(s)
        end

        return state
    end)
end
