-- The Lua Interface to Aerospike
--
-- ======================================================================
-- Copyright [2014] Aerospike, Inc.. Portions may be licensed
-- to Aerospike, Inc. under one or more contributor license agreements.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- ======================================================================
--
-- Ported from the Java client's aerospike.lua for the Rust client's
-- embedded Lua 5.4 interpreter. Differences from upstream:
--   * No `setfenv` sandboxing of the user function: Lua 5.4 removed
--     `setfenv` and the safe standard library loaded by mlua does not
--     include the `debug` library needed for the 5.2-style shim. The
--     client only ever runs the application's own local UDF source, so
--     the sandbox adds no security boundary here.
--   * `require("stream_ops")` dropped: stream_ops is pre-loaded into
--     every interpreter instance before this file runs.

-- ############################################################################
--
-- LOG FUNCTIONS
--
-- ############################################################################

function trace(m, ...)
    return aerospike:log(4, string.format(m, ...))
end

function debug(m, ...)
    return aerospike:log(3, string.format(m, ...))
end

function info(m, ...)
    return aerospike:log(2, string.format(m, ...))
end

function warn(m, ...)
    return aerospike:log(1, string.format(m, ...))
end

-- ############################################################################
--
-- APPLY FUNCTIONS
--
-- ############################################################################

--
-- Apply function to an iterator and arguments.
--
-- @param f the fully-qualified name of the function.
-- @param scope 1 for the server-side ops, 2 for the client-side ops.
-- @param istream the input stream of values produced by the server nodes.
-- @param ostream the output stream receiving the final values.
-- @return 0 on success, otherwise an error is raised.
--
function apply_stream(f, scope, istream, ostream, ...)

    if f == nil then
        error("function not found", 2)
    end

    local stream_ops = StreamOps_create();

    local success, result = pcall(f, stream_ops, ...)

    if success then

        local ops = StreamOps_select(result.ops, scope);

        -- Apply the scope's operations to the stream
        local values = StreamOps_apply(stream_iterator(istream), ops);

        -- Iterate the stream of values from the computation
        -- then pipe it to the ostream
        for value in values do
            stream.write(ostream, value)
        end

        -- 0 is success
        return 0
    else
        error(result, 2)
    end
end
