# Round-robin time series: a tutorial

How to use the `timeseries` module. Every output below is real, produced by
`cargo test --test tutorial -- --ignored --nocapture`.

Throughout, `T0` is `1767225600000` — 2026-01-01T00:00:00Z — and a "minute" is 60,000 ms.

**Contents**

1. [The idea in sixty seconds](#1-the-idea-in-sixty-seconds)
2. [Creating a series — `ts_init`](#2-creating-a-series--ts_init)
3. [Appending — `ts_append`](#3-appending--ts_append)
4. [Appending in bulk — `ts_append_many`](#4-appending-in-bulk--ts_append_many)
5. [When an append is refused](#5-when-an-append-is-refused)
6. [Choosing an aggregation](#6-choosing-an-aggregation)
7. [Integer series](#7-integer-series)
8. [Counters and rates](#8-counters-and-rates)
9. [Reading — `ts_report`](#9-reading--ts_report)
10. [Summarising — `ts_stats`](#10-summarising--ts_stats)
11. [Several series in one record](#11-several-series-in-one-record)
12. [Rolling up a fleet — `ts_rollup`](#12-rolling-up-a-fleet--ts_rollup)
13. [Long retention with archives](#13-long-retention-with-archives)
14. [Sizing a series](#14-sizing-a-series)
15. [Use cases](#15-use-cases)
16. [Error and result reference](#16-error-and-result-reference)

---

## 1. The idea in sixty seconds

A series is a **fixed number of slots**, each covering a fixed span of time. Slot for a timestamp `t` is
`(t / step_ms) mod slots`. When time moves past the end, it wraps around and the oldest slots are recycled. The series
never grows, never needs trimming, and always holds the most recent `slots × step_ms` of history.

Two bins hold it: a small **config** bin with the shape, and a **data** bin with the samples. You name the data bin; the
config defaults to `rrcfg`.

```text
ts_init("latency", 60000, 1440)        create: one-minute slots, 1440 of them (24 hours)
ts_append("latency", T0, 12.4)         write one sample
ts_report("latency", nil, nil, 200)    read it back as 200 points
ts_stats("latency")                    or just the summary numbers
```

---

## 2. Creating a series — `ts_init`

```text
ts_init(bin, step_ms, slots, [opts], [cfg_bin], [now_ms])
```

| parameter | meaning |
|---|---|
| `bin` | the bin the samples live in. You choose it — `latency`, `cpu`, `rx_bytes`. |
| `step_ms` | how much time one slot covers, in milliseconds. Must be positive. Ignored with `archives`. |
| `slots` | how many slots. 1 to 65,536. Total history is `step_ms × slots`. Ignored with `archives`. |
| `opts` | optional `key=value` string, comma-separated. See below. |
| `cfg_bin` | optional; where the shape is stored. Defaults to `rrcfg`. |
| `now_ms` | optional; the creation timestamp recorded in the config. Defaults to the server clock. Pass it when you want reproducible bytes. |

```text
ts_init("latency", 60000, 1440)
  -> result=created  bin=latency  config_bin=rrcfg  ring_bytes=11576
     step_ms=60000  slots=1440  agg=last  kind=f64  mode=gauge
```

`ring_bytes` is what the data bin will occupy: `56 + slots × 8`, or `56 + slots × 16` for `agg=mean`.

**Calling it again is safe.** With the same shape it does nothing and tells you so:

```text
ts_init("latency", 60000, 1440)   -- again, same shape
  -> result=exists  slots=1440
```

**Changing the shape is refused**, because the existing samples were laid out against the old one:

```text
ts_init("latency", 1000, 1440)    -- a different step
  -> ERROR: bin 'rrcfg' already describes a different series (step 60000 slots 1440
     last/f64/gauge); every ring in this record was built against it, so changing it
     would silently reinterpret them all
```

To reshape, delete the bins (or the record) and create it again — there is no in-place resize.

**A second series in the same record shares the config**, which is how you keep several metrics sampled together:

```text
ts_init("cpu", 60000, 1440)       -- a second ring, same config bin
  -> result=created  bin=cpu  config_bin=rrcfg  ring_bytes=11576
```

### The `opts` string

| key | values | default | what it does |
|---|---|---|---|
| `agg` | `last`, `first`, `min`, `max`, `sum`, `count`, `mean` | `last` | how two samples landing in the same slot combine |
| `kind` | `f64`, `i64` | `f64` | how a sample is stored |
| `mode` | `gauge`, `counter` | `gauge` | whether a value is the measurement, or a running total to difference |
| `backfill` | `0`/`1` | `1` | whether a sample for an older slot may still be merged in |
| `wipe` | `0`/`1` | `0` | whether an append that would erase the whole series is allowed |
| `wrap` | `0`, `32`, `64` | `0` | counter width, for reconstructing a wraparound |
| `archives` | `default`, `none`, or a list | `none` | keep several resolutions instead of one ring — see [§13](#13-long-retention-with-archives) |
| `heartbeat` | milliseconds | `0` | longest gap a counter reading may span before it becomes a new baseline |
| `xff` | `0`–`100` | `0` | how full an archive's bucket must be before it reports; needs `archives` |

```text
ts_init("rx_bytes", 1000, 3600, "kind=i64,mode=counter,agg=sum,wrap=32")
```

**A typo is an error, not a shrug:**

```text
ts_init("latency", 60000, 1440, "agg=maxx")
  -> ERROR: unknown aggregation 'maxx'
```

---

## 3. Appending — `ts_append`

```text
ts_append(bin, ts_ms, value, [cfg_bin])
```

| parameter | meaning |
|---|---|
| `bin` | the data bin |
| `ts_ms` | when the sample was taken, in milliseconds since the Unix epoch |
| `value` | the sample. An integer for an `i64` series, a double for an `f64` one |
| `cfg_bin` | optional; where to find the shape |

```text
ts_append("latency", T0, 12.4)
  -> result=placed  accepted=1  bucket=29453760  bucket_ms=1767225600000
     stored=12.4  cleared=0  last_bucket=29453760
```

`bucket` is the absolute slot index (`t / step_ms`) and `bucket_ms` is the first millisecond of that slot. `stored` is
what the slot holds **after** combining.

**A second sample in the same slot combines** rather than being rejected or appended:

```text
ts_append("latency", T0 + 30s, 19.9)   -- same minute
  -> result=combined  accepted=1  bucket=29453760  stored=19.9
```

The default `agg=last`, so the newer value won.

**Skipping ahead retires the slots you passed over.** `cleared` says how many:

```text
ts_append("latency", T0 + 5min, 13.1)  -- four minutes skipped
  -> result=placed  accepted=1  bucket=29453765  cleared=4  last_bucket=29453765
```

Those four minutes are now *unknown*, not zero.

**A late sample is merged into its own slot** if it's still inside the window:

```text
ts_append("latency", T0 + 2min, 11.0)  -- a late sample
  -> result=backfilled  accepted=1  bucket=29453762  stored=11.0  last_bucket=29453765
```

Note `last_bucket` did not move backwards.

`result` is one of `placed`, `combined`, `backfilled`, `too_old`, `backfill_refused`, `would_wipe`, and `accepted` is
`1` or `0`. **Always check one of them** — a refused append is a successful call that changed nothing.

---

## 4. Appending in bulk — `ts_append_many`

```text
ts_append_many(bin, times, values, [cfg_bin])
```

`times` and `values` are two lists of equal length. Samples are applied **in the order given**, which matters because
they compose through the aggregation.

```text
ts_append_many("latency", [60 timestamps], [60 values])
  -> offered=60  accepted=60  placed=60  combined=0  backfilled=0  too_old=0
     cleared=0  last_bucket=29453819
```

**Prefer this whenever you have more than one sample in hand.** It is roughly 480× faster per sample than one call
each, because the series is borrowed once for the whole batch instead of once per sample.

Replaying the same batch shows how the outcomes break down:

```text
ts_append_many("latency", [the same 60], [the same 60])  -- replayed
  -> offered=60  accepted=60  placed=0  combined=1  backfilled=59  too_old=0
```

Every slot already existed, so nothing was `placed`. The newest slot `combined`; the other 59 were `backfilled`.

Per-outcome counts always come back, so a batch that was half rejected says so.

---

## 5. When an append is refused

**A refused append changes nothing at all** — not one byte. The data is still there to be salvaged once the timestamp is
fixed.

Given a 10-slot, one-minute series with samples in every slot:

```text
ts_append("v", T0 - 1h, 5.0)          -- older than the window
  -> result=too_old  accepted=0  bucket=29453700
```

```text
ts_append("v", T0_in_seconds, 5.0)    -- seconds, not milliseconds
  -> result=too_old  accepted=0
```

```text
ts_append("v", T0 * 1000, 5.0)        -- microseconds, not milliseconds
  -> result=would_wipe  accepted=0
```

Those last two are the same bug in both directions, and it's the most common one: **the timestamp must be
milliseconds.** Seconds land far in the past; microseconds land far in the future, where accepting them would erase the
whole series. `would_wipe` fires whenever the jump forward is at least `slots` wide. If your sampling interval really is
wider than your whole window, set `wipe=1` — but check the step first.

With `backfill=0`, late samples are refused instead of merged:

```text
ts_append("v", T0 + 1min, 2.0)        -- backfill=0
  -> result=backfill_refused  accepted=0
```

Use `backfill=0` when late data would be wrong rather than merely late — for example when each slot is a count that has
already been read and acted on.

### An outage longer than the window wedges the series

**Worth knowing before it happens to you.** A refused append does not move the series forward — that is what makes it
non-destructive — so the *next* sample is still a whole window away and is also refused. A series that stops reporting for
longer than it can hold never accepts anything again by itself:

```text
-- a 60-slot minute archive: one hour of history. Two hours of silence, then 30 buffered samples:
ts_append_many("v", [30 timestamps 2h later], [30 values])
  -> offered=30  accepted=0  would_wipe=30
```

All thirty, for the same reason: the first was refused, so the position never moved.

A one-hour window and a two-hour outage is an ordinary Tuesday, not a misconfiguration, so this is the case the guard was
*not* designed for. Three ways out:

- **`wipe=1` on the series**, if gaps that long are expected. The old data is discarded when the new arrives, and the
  outage shows as a gap rather than as zeroes.
- **Re-initialise** into a fresh config bin, keeping the old one until you have read what you needed from it.
- **Keep the window wider than your worst plausible outage** — which is what the archive hierarchy is for. A series with
  a day of minutes and a year of days survives a two-hour outage without noticing.

Check `accepted`, or `would_wipe` on a batch, and this is visible the first time rather than as a series that quietly
stopped updating.

---

## 6. Choosing an aggregation

`agg` decides what happens when two samples share a slot. Appending `5.0`, `1.0` then `3.0` to the same minute:

```text
agg=last     -> values=[nil, nil, nil, 3.0]   the newest wins
agg=first    -> values=[nil, nil, nil, 5.0]   the oldest wins, later ones ignored
agg=min      -> values=[nil, nil, nil, 1.0]
agg=max      -> values=[nil, nil, nil, 5.0]
agg=sum      -> values=[nil, nil, nil, 9.0]   5 + 1 + 3
agg=count    -> values=[nil, nil, nil, 3.0]   three samples, values ignored
```

Which to pick:

| you are storing | use | why |
|---|---|---|
| a gauge sampled periodically — CPU %, queue depth, temperature | `last` | the most recent reading is the state |
| a latency or size you care about the worst of | `max` | peaks survive; a mean would hide them |
| a floor — free disk, available connections | `min` | same reasoning, other direction |
| events or bytes per interval | `sum` | the slot becomes a per-interval total |
| how many times something happened | `count` | the value is irrelevant, the arrival is the data |
| the earliest observation in each interval | `first` | e.g. an opening price |

`agg` also drives **downsampling** in `ts_report`, so a `max` series stays a max when you ask for fewer points. The one
exception is `count`, which downsamples by **summing** — two slots holding 3 and 4 samples become one holding 7, not 2.

---

## 7. Integer series

`kind=i64` stores samples as 64-bit integers and never converts them to floating point. Use it for byte counters,
request counts, ids — anything where exactness matters above 2⁵³.

```text
ts_append("bytes", T0, 9007199254740993)   -- 2^53 + 1
  -> result=placed  stored=9007199254740993
ts_append("bytes", T0, 9007199254740993)   -- again, agg=sum
  -> result=combined  stored=18014398509481986
ts_stats("bytes")
  -> count=1  sum=18014398509481986  min=18014398509481986  max=18014398509481986
```

An `f64` series would have lost the low bit of each of those on the way in.

**A double offered to an integer series is refused, not truncated:**

```text
ts_append("bytes", T0 + 1min, 1.5)         -- a double into an i64 ring
  -> ERROR: this ring stores i64 and Double was appended
```

An `f64` series does accept an integer, because widening it loses nothing.

---

## 8. Counters and rates

With `mode=counter` you append the **running total** and the series stores the **difference** — so you feed it what the
kernel or the application reports and read out per-interval rates.

```text
ts_init("rx", 60000, 8, "kind=i64,mode=counter,agg=sum")
```

```text
ts_append("rx", T0, 1000000)          -- the first reading
  -> result=placed  counter=baseline
```

**The first reading stores nothing**, because one reading is not a rate. `counter=baseline` says so and no `stored`
comes back.

```text
ts_append("rx", T0 + 1min, 1240000)
  -> result=placed  counter=delta  stored=240000
```

240,000 bytes in that minute.

```text
ts_append("rx", T0 + 2min, 4000)      -- the process restarted
  -> result=placed  counter=reset  stored is absent  counter_resets=1
ts_append("rx", T0 + 3min, 260000)    -- measured from the new baseline
  -> result=placed  counter=delta  stored=256000
```

**A decrease is treated as a restart**, the slot is left unknown, and `counter_resets` counts it. The alternative would
have been to store `4000 − 1240000 = −1236000`, a negative rate that never happened.

```text
ts_report("rx")
  -> values=[nil, nil, nil, nil, nil, 240000, nil, 256000]
     known=2  unknown=6  counter_resets=1
```

`counter` on each append is one of `baseline`, `delta`, `wrapped`, `reset`.

### Wraparound

A fixed-width counter that rolls over also *decreases*, and it is not a restart. Declare the width and it is
reconstructed:

```text
ts_init("rx", 60000, 8, "kind=i64,mode=counter,agg=sum,wrap=32")

wrap=32, previous reading 4294967196, next reading 50:
ts_append("rx", T0 + 1min, 50)
  -> result=placed  counter=wrapped  stored=150
```

100 to the ceiling plus 50 past it. A reconstructed wrap is only accepted when the result is under half the declared
width — otherwise it is still called a reset, because a restart from a low value would otherwise reconstruct to nearly
four billion.

`wrap` has no effect on an `f64` counter: there is no defined wraparound for a float, so a decrease is always a reset.

---

## 9. Reading — `ts_report`

```text
ts_report(bin, [from_ms], [to_ms], [max_points], [cfg_bin], [res], [now_ms])
```

| parameter | meaning |
|---|---|
| `from_ms` | start of the window. `nil` means the oldest live slot. Clamped to it. |
| `to_ms` | end of the window. `nil` means the newest. Clamped. |
| `max_points` | combine adjacent slots until at most this many points remain. `nil` means full resolution. |
| `cfg_bin` | optional; where to find the shape |
| `res` | which archive to read — `s`, `m`, `h`, `d`, `w`, `mo`, `y`. Only for a series with [archives](#13-long-retention-with-archives); `nil` lets the server choose. |
| `now_ms` | optional; what "now" means when choosing an archive. `nil` uses the server clock. |

A series with 12 one-minute slots, two of which nobody reported:

```text
ts_report("latency")
  -> result=ok  points=12  known=10  unknown=2  bucket_ms=60000
     from_ms=1767225600000  to_ms=1767226260000
     values=[12.4, 15.1, 11.8, 45.2, nil, nil, 98.6, 14.0, 13.2, 12.9, 16.4, 12.1]
```

**Timestamps are not sent** — value `i` covers `from_ms + i × bucket_ms`. That halves the payload and is exactly
reconstructible.

**Gaps are `nil`, not `0.0`.** A minute nobody reported and a minute that reported zero are different facts.

### Downsampling

```text
ts_report("latency", nil, nil, 4)     -- downsample to 4 points
  -> points=4  bucket_ms=180000  downsampled_with=last
     values=[11.8, 45.2, 13.2, 12.1]
```

Twelve slots into four, three to a group, combined with the series' own `agg`. `bucket_ms` rose to 180,000 so you can
see the resolution you got, and `downsampled_with` says how they were combined.

A group containing both values and gaps keeps its values — an unknown never erases a known one. That is why the group
holding buckets 3, 4 and 5 still reports 45.2.

Use `max_points` whenever you are drawing a graph: asking for 200 points from a 1,440-slot day gives you exactly what a
200-pixel-wide chart can show.

**One sharp edge.** The factor is `ceil(span / max_points)` and it is an integer, so a `max_points` just below the number
of slots halves the resolution rather than shaving it:

| slots | `max_points` | factor | points you get |
|---|---|---|---|
| 65,536 | 1,000 | 66 | 993 |
| 65,536 | 4,095 | 17 | 3,856 |
| 65,536 | **65,535** | **2** | **32,768** |
| 65,536 | 65,536 | 1 | 65,536 |

Pass `max_points >= slots`, or leave it `nil`, when you want every bucket. `points` and `bucket_ms` always tell you what
you actually got, so nothing here is silent — but asking for one point fewer than the ring holds and receiving half of
them is surprising the first time.

Zero and negative values mean "no limit" rather than "no points".

### Windows

```text
ts_report("latency", T0+6min, T0+8min)
  -> points=3  from_ms=1767225960000  to_ms=1767226080000
     values=[98.6, 14.0, 13.2]
```

`from_ms` and `to_ms` are clamped to the live window, and the returned `from_ms`/`to_ms` tell you what you actually got.

### The empty and no-overlap cases

```text
ts_report("latency")                  -- before any sample
  -> result=empty  points=0  known=0

ts_report("latency", T0+1h, T0+2h)    -- a window past the data
  -> result=no_overlap  points=0
```

`result` is `ok`, `empty` or `no_overlap`. Neither of the latter two is an error — they mean there was nothing to
report, and `values` is absent rather than an empty list.

---

## 10. Summarising — `ts_stats`

```text
ts_stats(bin, [from_ms], [to_ms], [cfg_bin])
```

Same window parameters, but **no list comes back at all** — which is the point when the series is large.

```text
ts_stats("latency")
  -> count=10  unknown=2  sum=251.7  mean=25.169999999999998
     min=11.8  max=98.6  first=12.4  first_ms=1767225600000
     last=12.1  last_ms=1767226260000
```

| key | meaning |
|---|---|
| `buckets` | slots in the window |
| `count` | slots that hold a sample |
| `unknown` | `buckets − count` |
| `sum`, `mean`, `min`, `max` | over the known slots only |
| `first`, `first_ms` | oldest known sample and its slot's start |
| `last`, `last_ms` | newest known sample and its slot's start |

`mean` is `sum / count`, so gaps do not drag it toward zero.

It takes a window too:

```text
ts_stats("latency", T0+6min, T0+8min)
  -> count=3  sum=125.8  mean=41.93333333333333  min=13.2  max=98.6
```

On an `i64` series, `sum`, `min`, `max`, `first` and `last` come back as **integers**, exactly.

> Note: `last_ms` means the newest slot **holding a sample** here, whereas in `ts_report` it means the newest slot of
> the series. They differ when the most recent slots are empty.

---

## 11. Several series in one record

Rings in the same record share the config bin, so metrics sampled together stay together:

```text
ts_init("cpu", 60000, 6)
ts_init("mem", 60000, 6)
ts_init("net", 60000, 6)
```

A series with a **different shape** needs its own config bin — pass it as the 5th argument to `ts_init` and thereafter
to every call on that series:

```text
ts_init("errors", 1000, 60, "agg=sum", "errcfg")
ts_append("errors", T0, 1, "errcfg")
ts_report("errors", nil, nil, nil, "errcfg")
  -> step_ms=1000  slots=60  agg=sum  known=1
```

After those calls the record holds:

```text
bins in the record after four ts_init calls
  -> cpu, errcfg, errors, mem, net, rrcfg
```

Two config bins, four data bins. Note the positional `nil`s in `ts_report("errors", nil, nil, nil, "errcfg")` — the
config bin is the fifth parameter, so the three optional ones before it have to be filled in.

### `ts_append_all` — every metric in one call

Metrics sampled together should be written together. `ts_append_all` takes a list of bins and a matching list of values,
and commits the record **once**:

```text
ts_append_all(["cpu", "mem", "net"], T0, [1.0, 2.0, 3.0])
  -> result=ok  metrics=3  rings_accepted=6  rings_refused=0
     per_metric={cpu: placed, mem: placed, net: placed}
```

`rings_accepted=6` because each of the three metrics has two archives here.

The same rings are written either way; what disappears is the extra round trips and record commits. Measured at three
archives per metric:

| metrics | N × `ts_append` | `ts_append_all` | gain |
|---|---|---|---|
| 1 | 81.2 µs | 66.9 µs | 1.21× |
| 4 | 509.4 µs | 110.1 µs | 4.63× |
| 8 | 1,292.7 µs | 189.8 µs | 6.81× |
| **16** | **4,666.2 µs** | **359.2 µs** | **12.99×** |

Sixteen is roughly how many archived metrics fit a record, so **13×** is the figure for an agent reporting a host.

All the metrics have to share one config bin — which they do by default. `result` is `ok`, `partial`, `refused` or
`empty`, and `per_metric` gives each one's outcome. Lists of different lengths are refused outright rather than half
applied.

---

## 12. Rolling up a fleet — `ts_rollup`

A stream aggregation rather than a record function: it runs over **every record a query matches** and returns one
combined series. This is the part that cannot be done client-side without transferring every series.

```text
ts_rollup(bin, [op], [max_points], [cfg_bin], [step_ms], [slots])
```

| parameter | meaning |
|---|---|
| `bin` | the data bin, in every record |
| `op` | how values from different records combine: `sum`, `min`, `max`, `last`. Default `sum`. |
| `max_points` | downsample the combined series before returning it |
| `cfg_bin` | optional; where each record's shape lives |
| `step_ms`, `slots` | optional; **pin the expected shape**. Give both or neither. |

Series align by absolute slot index, so records that agree on `step_ms` line up with no timestamp matching at all.

Three hosts reporting `10+b`, `20+b`, `30+b` across six minutes, with the shape pinned:

```text
ts_rollup("rps", "sum", nil, nil, 60000, 6)
  -> result=ok  op=sum  records=3  samples=18  skipped_shape=1  shape_declared=1
     step_ms=60000  points=6  bucket_ms=60000
     values=[60.0, 63.0, 66.0, 69.0, 72.0, 75.0]  counts=[3, 3, 3, 3, 3, 3]
```

`60 + 3b`, as it should be. There is also a fourth record in that set with a different step; it was skipped and counted
in `skipped_shape`.

**`counts` is the second list, and it is how you get a mean across records:** divide `values` by `counts`. Here that is
20, 21, 22, 23, 24, 25 — the average host.

```text
ts_rollup("rps", "max", nil, nil, 60000, 6)
  -> values=[30.0, 31.0, 32.0, 33.0, 34.0, 35.0]  counts=[3, 3, 3, 3, 3, 3]

ts_rollup("rps", "min", nil, nil, 60000, 6)
  -> values=[10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
```

The worst and best host in each minute. `counts` still reports three contributors — it counts what was *seen*, not what
won.

Downsampling applies after combining:

```text
ts_rollup("rps", "sum", 3, nil, 60000, 6)
  -> points=3  bucket_ms=120000  values=[123.0, 135.0, 147.0]  counts=[6, 6, 6]
```

`60+63`, `66+69`, `72+75`, with six record-slots behind each point.

### Always pin the shape when the set might be mixed

Without `step_ms`/`slots`, the aggregation adopts the shape of whichever record it reads **first**, and there is no
single "first" across query threads. On a mixed set the result is a subset and which subset varies between runs:

```text
ts_rollup("rps")            -- same four records, shape not pinned
  -> result=shape_conflict  records=2  skipped_shape=2
     values=[50.0, 52.0, 54.0, 56.0, 58.0, 60.0]

ts_rollup("rps", "min")     -- again, a different run
  -> result=shape_conflict  records=1  skipped_shape=3
     values=[10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
```

`result` is `shape_conflict` rather than `ok` precisely so a caller testing for `ok` never receives one of these.
**Pinning the shape makes it deterministic**; narrowing the query so the set is homogeneous works too.

### Counters that only make sense for records

```text
ts_rollup("rps", "count")
  -> ERROR: roll up with sum, min, max or last - 'count' is always reported anyway,
     and 'first' has no meaning across records because they have no order
```

Always check these before trusting an answer:

| key | why |
|---|---|
| `result` | `ok`, `shape_conflict` or `empty` |
| `records` | how many records contributed |
| `samples` | how many individual slot values went in |
| `skipped_absent` | records with no such bin, or no config |
| `skipped_shape` | records whose shape disagreed |
| `skipped_unreadable` | records whose config or series would not decode |
| `dropped_old` | samples that fell outside the combined window |

A rollup that quietly saw two records out of ten thousand and returned a confident-looking series is the failure these
exist to prevent.

---

## 13. Long retention with archives

A single ring holds at most 65,536 slots, so one second of resolution reaches 18 hours and one minute reaches 45 days.
**A year is out of reach at any useful detail.** Archives are the answer: several rings at coarsening resolution, all fed
from the same appends.

```text
ts_init("cpu", 0, 0, "archives=default")
  -> result=created  archives=7  rings_created=7  rings_existing=0  ring_bytes=62232  step_ms=1000  slots=3600
```

That makes seven bins:

| resolution | bin | slots | covers | agg | bytes |
|---|---|---|---|---|---|
| second | `cpu~s` | 3,600 | 1 hour | last | 28,856 |
| minute | `cpu~m` | 1,440 | 1 day | mean | 23,096 |
| hour | `cpu~h` | 336 | 14 days | mean | 5,432 |
| day | `cpu~d` | 100 | ~3 months | mean | 1,656 |
| week | `cpu~w` | 104 | 2 years | mean | 1,720 |
| month | `cpu~mo` | 60 | 5 years | mean | 1,016 |
| year | `cpu~y` | 25 | 25 years | mean | 456 |

**60.8 KiB per metric, so about sixteen metrics fit one record.** The `step_ms` and `slots` you pass are ignored and taken
from the finest archive instead, so pass anything — `0, 0` reads clearly as "not my decision here".

With archives, the base bin `cpu` holds **nothing**. The rings are the `~` bins. You still name the series `cpu` in every
call; the verb picks the ring.

### Appending is unchanged

Ten samples, one per second, values 1 through 10. The tenth:

```text
ts_append("cpu", T0 + 9000, 10.0)
  -> result=placed  accepted=1  archives=7  archives_accepted=7  archives_refused=0
     per_archive={day: combined, hour: combined, minute: combined, month: combined,
                  second: placed, week: combined, year: combined}
```

One sample goes to all seven. `placed` once and `combined` six times is the normal shape: a new second, the same minute,
hour, day, week, month and year. `result` is the finest archive's outcome.

Read back at four of the seven:

```text
ts_report("cpu", T0, T0 + 9000, nil, nil, "s")
  -> resolution=second  archive_bin=cpu~s  step_ms=1000  points=10  known=10
     values=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
ts_report("cpu", T0, T0 + 9000, nil, nil, "m")
  -> resolution=minute  archive_bin=cpu~m  step_ms=60000  points=1  known=1  values=[5.5]
ts_report("cpu", T0, T0 + 9000, nil, nil, "h")
  -> resolution=hour  archive_bin=cpu~h  step_ms=3600000  points=1  known=1  values=[5.5]
ts_report("cpu", T0, T0 + 9000, nil, nil, "y")
  -> resolution=year  archive_bin=cpu~y  step_ms=31556952000  points=1  known=1  values=[5.5]
```

**A coarse bucket is the mean of the raw samples**, not a mean of means: 5.5 is the average of 1 through 10, and it is the
same number at every resolution because all ten fell inside one minute. Where it matters is uneven density — ten samples
of 100 in one minute and one sample of 0 in the next give an hourly mean of 1000/11, not 50.

### Reading picks an archive for you

The rule is **the finest archive that still reaches back to `from_ms`**, and the answer says which and why:

```text
ts_report("cpu", T0 - ten minutes, nil, 4, nil, nil, T0)
  -> result=ok  resolution=second  archive_bin=cpu~s
     archive_chosen_because=the finest archive that reaches back that far
ts_report("cpu", T0 - two hours, nil, 4, nil, nil, T0)         -> resolution=minute  archive_bin=cpu~m
ts_report("cpu", T0 - three days, nil, 4, nil, nil, T0)        -> resolution=hour    archive_bin=cpu~h
ts_report("cpu", T0 - thirty days, nil, 4, nil, nil, T0)       -> resolution=day     archive_bin=cpu~d
ts_report("cpu", T0 - two hundred days, nil, 4, nil, nil, T0)  -> resolution=week    archive_bin=cpu~w
ts_report("cpu", T0 - three years, nil, 4, nil, nil, T0)       -> resolution=month   archive_bin=cpu~mo
```

The last argument is `now_ms`; leave it off and the server clock is used. Ask for more history than anything holds and the
coarsest answers, with `result=truncated` rather than `ok` — the numbers are real, the window is not the one you asked
for:

```text
ts_report("cpu", T0 - fifty years, nil, 4, nil, nil, T0)
  -> result=truncated  resolution=year  archive_bin=cpu~y
     archive_chosen_because=no archive reaches back that far, so the coarsest
```

Asking for a resolution that does not exist is an error, not a substitution:

```text
ts_report("cpu", nil, nil, nil, nil, "nanosecond")
  -> ERROR: unknown resolution 'nanosecond'
```

and so is asking for an archive this series does not keep — the message lists the ones it does.

`ts_stats` selects the same way, with `res` as its 5th argument.

### `ts_info` — what actually exists

```text
ts_info("cpu")
  -> result=ok  archives=7  rings_present=7  ring_bytes=62232  created_ms=1767225600000
```

plus a `table` with one entry per ring:

```text
{index: 0, bin: "cpu~s", resolution: "second", slots: 3600, agg: "last", bytes: 28856,
 span_ms: 3600000, step_ms: 1000, state: "ok", from_ms: …, to_ms: …, appends: 10}
```

`state` is `ok`, `empty` (initialised, never appended to), `missing` or `unreadable`. One unreadable ring is reported in
place rather than failing the call, so the other six still answer.

### Choosing your own set

`archives=` also takes a list of `resolution:slots[:agg[:xff]]`, **semicolon** separated:

```text
ts_init("q", 0, 0, "archives=m:1440;h:336;d:365")
```

`agg` defaults to `mean` and `xff` to 0. Semicolons because the surrounding options string is already comma-separated.

Rules: finest first, each resolution at most once, `slots` from 1 to 65,536. Bin names must fit 15 characters including
the suffix, so a base name is at most 12 — checked at `ts_init`, not discovered when six of seven rings already exist.

### `xff` — refusing to average almost nothing

A yearly mean built from two readings during an outage should not look as trustworthy as one built from a year of them.
`xff` is the percentage of a bucket's *possible* samples that must be present:

```text
ts_init("s", 0, 0, "archives=m:60;h:24:mean:50")
  -> result=created  archives=2  ring_bytes=1456
```

`ts_info("s")` then shows `xff=50` and `xff_needs_samples=30` on the hour entry. The denominator is the finest archive's
step — an hourly bucket over per-minute samples could hold 60, so half is 30. Feed it five samples:

```text
ts_report("s", T0, T0 + 4 * MIN, nil, nil, "m")
  -> resolution=minute  points=5  known=5  unknown=0  values=[7.0, 7.0, 7.0, 7.0, 7.0]
ts_report("s", T0, T0 + 4 * MIN, nil, nil, "h")
  -> resolution=hour  points=1  known=0  unknown=1  xff_suppressed=1  values=[nil]
```

The minute archive reports all five; the hour archive refuses to average five readings out of sixty.

`xff_suppressed=1` rather than `unknown=1` alone: "no data" and "too sparse to trust" are different facts. The samples
are not lost — the 30th one makes the bucket report normally.

### `heartbeat` — a counter that went quiet

Without it, a counter reading arriving three hours after the previous one has its entire delta charged to a single
bucket, as though three hours of traffic happened in one interval:

```text
ts_init("b", 0, 0, "kind=i64,mode=counter,archives=m:60:sum,heartbeat=120000")
  -> result=created  archives=1  heartbeat_ms=120000

ts_append("b", T0, 1000)           -> result=placed  counter=baseline
ts_append("b", T0 + 1 min, 1100)   -> result=placed  counter=delta  stored=100
ts_append("b", T0 + 6 min, 5000)   -> result=placed  counter=stale
ts_append("b", T0 + 7 min, 5050)   -> result=placed  counter=delta  stored=50
```

The five-minute gap exceeds the two-minute heartbeat, so that reading becomes a new baseline and the bucket stays
unknown — instead of a 3,900 spike at exactly the moment the data is least trustworthy. `stale` is distinct from `reset`:
nothing went backwards.

Default `0` means unlimited.

---

## 14. Sizing a series

For a single ring, `step_ms × slots` is your retention and `56 + slots × 8` bytes is the cost.

| step | slots | retention | bytes |
|---|---|---|---|
| 1 s | 3,600 | 1 hour | 28 KB |
| 10 s | 8,640 | 1 day | 69 KB |
| 1 min | 1,440 | 1 day | 11.6 KB |
| 1 min | 10,080 | 1 week | 79 KB |
| 5 min | 8,064 | 4 weeks | 63 KB |
| 1 hour | 8,760 | 1 year | 68 KB |

`slots` is capped at 65,536, which is 512 KB of samples. A `mean` ring's slots are 16 bytes rather than 8, so it costs
twice the table above.

Three practical notes:

- **Match the step to how often you actually sample.** A one-second step fed a sample a minute leaves 59 of every 60
  slots unknown, and wastes 59/60 of the space.
- **Append cost grows with the series size** above about 64 KB, because a write rewrites the bin. A 512 KB series costs
  roughly 187 µs per single append against 66 µs for a small one — so for large series, batch with `ts_append_many`.
- **Wanting both detail and history is what [archives](#13-long-retention-with-archives) are for.** They cost 17% more
  per append than a single ring and reach 25 years instead of 18 hours. Two hand-rolled series with separate config bins
  still work, and there is no longer much reason to do it that way.

---

## 15. Use cases

**Per-host metrics for a dashboard.** One record per host, one ring per metric, `agg=last`, 1-minute step, 1,440 slots.
Draw with `ts_report(bin, nil, nil, 200)` so the server downsamples to what the chart can show.

**Request latency with peaks preserved.** `agg=max`. A mean would hide exactly the spikes you are looking for, and a
`max` series still reports maxima after downsampling.

**Bytes or requests per interval.** `agg=sum` with a gauge, appending the per-interval delta yourself; or
`mode=counter` with `agg=sum`, appending the running total and letting the series difference it.

**Interface or disk counters that reset.** `mode=counter`, `kind=i64`, and `wrap=32` or `64` if the counter is a fixed
width. Watch `counter_resets` — a series full of them means the process is restarting, which is itself the finding.

**Error and event counting.** `agg=count`, and append anything at all; the value is ignored and the arrival is the data.
Combine with `backfill=0` if a late event should be rejected rather than folded into a count already acted on.

**Rate limiting or quota windows.** A short series — 60 one-second slots — with `agg=sum`, and `ts_stats` to get the
total over the window in one call and no list.

**SLO burn.** Two rings sharing a config: `good` and `total`, both `agg=sum`. `ts_stats` on each gives the ratio over
any window.

**Fleet totals and averages.** `ts_rollup(bin, "sum", nil, nil, step, slots)` for the total, and `values / counts` for
the average host. `max` finds the worst host per interval without fetching anything.

**Capacity planning.** An hourly series with 8,760 slots is a year of history in 68 KB per metric, and
`ts_report(bin, nil, nil, 365)` gives a daily view of it.

**Anomaly triage.** `ts_stats` over the last hour against `ts_stats` over the last day is two calls and no data
transfer.

**Sensor readings that arrive late.** Leave `backfill=1` (the default) so a device that buffers while offline can flush
its samples into the right slots when it reconnects — as long as they are still inside the window.

**Multiple resolutions.** A 1-minute ring and a 1-hour ring in the same record, each with its own config bin, appended
to in the same call sequence.

---

## 16. Error and result reference

### `result` values

| value | verb | meaning |
|---|---|---|
| `created` | `ts_init` | the series was made |
| `exists` | `ts_init` | it already existed with this shape; nothing changed |
| `completed` | `ts_init` | some rings existed and the missing ones were created |
| `placed` | `ts_append` | written into a newer slot than any before it |
| `combined` | `ts_append` | merged into the newest slot, which already had a sample |
| `backfilled` | `ts_append` | merged into an older slot, still inside the window |
| `too_old` | `ts_append` | older than the window; **nothing changed** |
| `backfill_refused` | `ts_append` | inside the window but `backfill=0`; **nothing changed** |
| `would_wipe` | `ts_append` | would erase every slot and `wipe=0`; **nothing changed** |
| `ok` | `ts_append_all` | every ring of every metric took the sample |
| `partial` | `ts_append_all` | some rings took it and some refused; `per_metric` says which |
| `refused` | `ts_append_all` | no ring took it; **nothing changed** |
| `ok` | read verbs | there was data to report |
| `truncated` | `ts_report` | no archive reaches back to `from_ms`, so the coarsest answered |
| `empty` | read verbs | the series has never been appended to |
| `no_overlap` | read verbs | the window does not intersect the live slots |
| `shape_conflict` | `ts_rollup` | records were dropped for disagreeing about a shape nobody pinned |

### Counter events

| value | meaning |
|---|---|
| `baseline` | the first reading; one reading is not a rate, so the slot stays unknown |
| `delta` | an ordinary increase |
| `wrapped` | a decrease reconstructed as a wraparound, because `wrap` was declared and the result is plausible |
| `reset` | a decrease that is not a wrap; the slot stays unknown and `counter_resets` goes up |
| `stale` | the gap exceeded `heartbeat`, so this reading is a new baseline rather than a delta |

### Ring states in `ts_info`

| value | meaning |
|---|---|
| `ok` | the ring exists and holds data |
| `empty` | initialised, never appended to |
| `missing` | the bin is not there; `ts_init` again to create it |
| `unreadable` | the bin exists but does not validate; `error` on the same entry says why |

### Common errors

| message begins | cause | fix |
|---|---|---|
| `no time-series config in bin …` | never initialised, or the wrong `cfg_bin` | call `ts_init`, or pass the right config bin |
| `no ring in bin …` | the config exists but this series does not | `ts_init` for that bin |
| `already describes a different series` | `ts_init` with a changed shape | delete the bins, or use another config bin |
| `unknown option` / `unknown aggregation` | a typo in `opts` | check the table in §2 |
| `this ring stores i64 and Double was appended` | a double into an integer series | pass an integer |
| `a sample has to be a finite number` | `NaN` or an infinity was appended | `NaN` is how a gap is marked, so it cannot also be a value; send nothing instead |
| `sample N of this batch is …` | a non-finite value inside `ts_append_many` | the whole batch is refused before anything is written, so fix it and resend |
| `a timestamp that lands in bucket i64::MIN` | a 1 ms step with a timestamp of exactly `i64::MIN` | that value marks a ring as never written |
| `was built against a different config` | the config bin was edited after the series was created | restore the config, or reinitialise |
| `the config describes N slots … but this ring is M bytes` | `slots` changed underneath an existing series | same |
| `roll up with sum, min, max or last` | `op=count` or `op=first` | those have no meaning across records |
| `declare both step_ms and slots, or neither` | half a pinned shape | give both |
| `this series has no … archive - it keeps …` | `res` naming a resolution the series does not keep | use one it does, or leave `res` off |
| `this series keeps a single ring, so it has no … archive` | `res` on a series without archives | initialise with `archives=` |
| `this ring holds … buckets but it was opened as …` | one archive's ring is in another's bin | check the bin name |
| `archive bin '…' is N characters` | the base name is too long for a suffix | 12 characters at most with archives |
| `archives must be ordered finest first` | an out-of-order or repeated resolution in `archives=` | order them second → year |
| `xff describes an archive's density` | `xff=` without `archives=` | add `archives=`, or drop `xff` |
| `ring layout version 1, this build understands 2` | a series created before archives existed | reinitialise it |

### Things worth checking in production

- `accepted` on every append, or the per-outcome counts on every batch.
- `archives_refused` on an archived series — a nonzero value means some resolutions rejected a sample the finest took.
- `counter_resets` on a counter series, and the `stale` event if you set a `heartbeat`.
- `saturated`, which appears if an integer sum hit `i64::MAX`.
- `result == "truncated"` on a report, which means you asked for more history than any archive holds.
- `xff_suppressed` if you set an `xff`, so you can tell a sparse bucket from an empty one.
- `state` on every `ts_info` entry, which is how a `missing` or `unreadable` ring surfaces.
- `result`, `records` and the `skipped_*` counters on every rollup.
