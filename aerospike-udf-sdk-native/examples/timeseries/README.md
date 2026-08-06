# A round-robin time series native UDF

**New here? Read [TUTORIAL.md](TUTORIAL.md)** — this file is the design and the measurements; the tutorial is how to use it.

A fixed-size circular buffer of samples in a **blob bin**, with its shape in a second bin. Initialise, append, report,
summarise, and roll up a fleet of series into one — all server-side. Optionally an **RRDtool-style archive hierarchy**:
one hour of per-second samples through 25 years of yearly means, in 60.8 KiB.

```text
ts_init("latency", 60000, 1440)                  one day at one-minute resolution
ts_init("rx_bytes", 1000, 3600, "kind=i64,mode=counter,agg=sum,wrap=32")
ts_init("cpu", 0, 0, "archives=default")         seven archives: second .. year
ts_append("latency", 1767225600000, 12.4)        one sample, into every archive it has
ts_append_many("latency", times, values)         a batch, one borrow of each bin for all of it
ts_append_all(bins, ts, values)                  every metric in the record, one commit
ts_report("latency", nil, nil, 200)              the last day in 200 points, downsampled here
ts_report("cpu", three_years_ago)                picks the archive that reaches back that far
ts_stats("latency")                              count/sum/min/max/first/last, no list at all
ts_info("cpu")                                   which bins exist and how far back each goes
ts_rollup("latency", "sum")                      every record's series, aligned and combined
```

## Why a blob and not a CDT list

A list of 1,440 doubles is msgpack: appending walks and rewrites the packed form, and reading unpacks all of it to hand
back something addressed by index. A ring in a blob is `slots × 8` bytes at computable offsets, and the sample for a
timestamp is found by arithmetic rather than traversal.

**That argument holds for reading and not much else — see [what it costs](#what-it-costs).** The measurements are at the
bottom rather than the top because they do not all favour this design, and putting them last would look like hiding
them.

## Storage

A config bin and one ring bin per archive — two bins for a plain series, eight for the default hierarchy. No msgpack
anywhere, every field at a fixed offset, little-endian throughout.

**Config — `rrcfg` by default, 40 bytes plus 16 per archive. The immutable shape.**

| off | sz | field | notes |
|---|---|---|---|
| 0 | 4 | magic | `"RRC1"` |
| 4 | 1 | version | 2; a version 1 config still decodes and means "no archives" |
| 5 | 1 | agg | 0 last, 1 first, 2 min, 3 max, 4 sum, 5 count, 6 mean |
| 6 | 1 | kind | 0 f64, 1 i64 |
| 7 | 1 | mode | 0 gauge, 1 counter |
| 8 | 8 | step_ms | i64, > 0 |
| 16 | 8 | slots | i64, 1..=65536 |
| 24 | 8 | created_ms | |
| 32 | 1 | flags | bit0 allow_backfill, bit1 allow_wipe |
| 33 | 1 | wrap_bits | 0, 32 or 64 |
| 34 | 1 | n_archives | 0..=7 |
| 35 | 1 | reserved | |
| 36 | 4 | heartbeat_ms | u32; 0 means unlimited |
| 40 | 16n | archives | one descriptor each, finest first |

**Archive descriptor — 16 bytes.**

| off | sz | field | notes |
|---|---|---|---|
| 0 | 1 | resolution | 0 second, 1 minute, 2 hour, 3 day, 4 week, 5 month, 6 year |
| 1 | 1 | agg | as above |
| 2 | 1 | xff_pct | 0..=100; 0 reports whatever is there |
| 3 | 1 | reserved | |
| 4 | 8 | slots | i64, 1..=65536 |
| 12 | 4 | reserved | |

**Ring — the bin you name, `56 + slots × 8` bytes (× 16 for a `mean` archive).**

| off | sz | field | notes |
|---|---|---|---|
| 0 | 4 | magic | `"RRD1"` |
| 4 | 1 | version | 2 |
| 5 | 1 | kind | a copy, so the ring is self-describing |
| 6 | 1 | mode | a copy |
| 7 | 1 | flags | bit0 saturated, bit1 has_raw |
| 8 | 4 | cfg_fp | FNV-1a 32 of the config's canonical encoding |
| 12 | 4 | resets | u32 |
| 16 | 8 | last_bucket | i64; `i64::MIN` means never written |
| 24 | 8 | last_raw | the previous counter reading (bit pattern for f64) |
| 32 | 8 | appends | i64 |
| 40 | 1 | resolution | which archive this is; `0xFF` for a ring with no archives |
| 41 | 1 | agg | a copy |
| 42 | 1 | xff_pct | a copy |
| 43 | 5 | reserved | |
| 48 | 8 | last_raw_ms | when the previous counter reading arrived — what the heartbeat tests |
| 56 | … | slots × 8/16 | the samples |

**Version 2 does not read a version 1 ring.** The archive fields were added, and there is no rule that recovers them
from bytes v1 never stored — so an old series is refused with a message saying so rather than misread. Re-initialise it.

### Why the ring carries its own resolution

Because the seven archives of one series share one config bin and therefore **one fingerprint**. Before the resolution
was in the header, opening `cpu~h` against the minute archive's shape passed every check there was — same magic, same
fingerprint, and the same length whenever the slot counts happened to agree — and reported values an hour from where
they belonged. Length and aggregation cannot distinguish two archives that differ only in resolution, which is exactly
the pair a mistyped bin name produces.

`one_archive_cannot_be_read_as_another` builds precisely that case: two archives, 60 slots each, both `mean`, and then
swaps the rings between their bins.

### Why the shape is in its own bin

So a client can read what a series looks like **without pulling the series**. `operate()` fetching one 40-to-152-byte bin is a
different transaction from fetching half a megabyte. Inside a UDF the whole record is resident either way, so the
benefit is entirely client-side — worth saying plainly rather than implying the split buys something it does not.

One config serves **every ring in the record**, so `cpu`, `mem` and `net` can share a shape while each keeps its own
position. A record is a series, or a few sampled together.

Both bins are written inside one UDF invocation, under the record lock, so they cannot be observed out of step.

## Archives — the RRDtool part

A single ring is capped at 65,536 slots, so a one-second series reaches 18 hours and a one-minute series 45 days. **A
year is simply unreachable.** Seven archives at coarsening resolution reach 25 years in 60.8 KiB.

`archives=default` is:

| resolution | slots | covers | agg | bytes |
|---|---|---|---|---|
| second | 3,600 | 1 hour | last | 28,856 |
| minute | 1,440 | 1 day | mean | 23,096 |
| hour | 336 | 14 days | mean | 5,432 |
| day | 100 | ~3 months | mean | 1,656 |
| week | 104 | 2 years | mean | 1,720 |
| month | 60 | 5 years | mean | 1,016 |
| year | 25 | 25 years | mean | 456 |
| | | | | **62,232** |

**60.8 KiB per metric, so about sixteen metrics share a record** under a 1 MB write block. Both figures are asserted in
`the_default_archive_set_is_the_size_it_claims` rather than left in a comment that can rot — the number in the design
notes was wrong twice before that test existed.

Rings live in `<base>~<resolution>`: `cpu~s`, `cpu~m`, `cpu~h`, `cpu~d`, `cpu~w`, `cpu~mo`, `cpu~y`. The suffix is a
string `Resolution::parse` accepts, so the two directions are not separate tables that can drift. **With archives the
base bin holds nothing** — the finest archive already *is* the ring it would hold, and a duplicate would cost 28 KiB per
metric to say the same thing twice. `step_ms` and `slots` are taken from the finest archive rather than from `ts_init`'s
arguments, for the same reason: two numbers that disagree with the table beside them are a bug waiting to happen.

### Consolidation needs no accumulator, and comes out better than RRDtool's

RRDtool keeps a consolidated-data-point accumulator per archive because its archives do not self-align. Ours do: a
bucket is `floor(t / step)` derived from the timestamp and the resolution alone, so an hourly archive's buckets are
exactly 3,600 of the second archive's wide and aligned to them, **for free**. Consolidation is therefore just *appending
the same sample to every archive* — each one's own `agg` does the rest, with no extra state and nothing to flush.

That also makes it more accurate. RRDtool feeds a coarse archive the fine archive's already-averaged values, so a yearly
mean is a mean of monthly means — which equals the true mean only if every month held the same number of samples. Here
every archive sees the raw sample, so **a coarse mean is the true mean**. `a_coarse_mean_is_the_true_mean_of_the_raw_samples`
pins it with deliberately lopsided density: ten samples of 100 in one minute and one sample of 0 in the next, where a
mean of means says 50 and the right answer is 1000/11.

A `mean` slot is 16 bytes — a running `sum` and a `count` — because a mean cannot be folded into one number. The count
doubles as the `xff` input, and as the reason a mean slot needs no unknown sentinel: `count == 0` *is* empty.

### Month and year are not durations

Every other resolution is a fixed multiple of a millisecond. A month is 28 to 31 days, so `Resolution::step_ms()` returns
**`None`** for month and year rather than handing back an approximation a caller might store against. Their buckets go
through calendar arithmetic — Howard Hinnant's `civil_from_days`/`days_from_civil`, about twenty lines and no dependency.

Approximating a month as 30 days keeps the division and drifts: twelve 30-day "months" land five days short of a year, so
a bucket labelled *March* wanders through the calendar until it covers most of April. A chart with the wrong month on it
is worse than no month archive.

Weeks begin on **Monday**. 1970-01-01 was a Thursday, so `ms / MS_PER_WEEK` produces Thursday-to-Wednesday weeks, which
is almost never what anyone means; a three-day shift fixes it, verified by weekday over 3,600 weeks rather than by
construction.

### `xff` and the heartbeat

Both are RRDtool's, and both close real holes.

**`xff`** is the share of a bucket's possible samples that must be known for it to report a value. A yearly average built
from two readings during an outage should not look as authoritative as one built from a year of them. The denominator is
the finest archive's step, so an hourly bucket over per-minute samples could hold 60 and `xff=50` demands 30.

It filters on the way **out**, not in: an append has to combine with a bucket that does not currently report, or a
suppressed bucket could never become unsuppressed. A report separates `xff_suppressed` from `unknown`, because "no data"
and "too sparse to trust" are different facts.

**The heartbeat** is the longest gap a counter may span before a reading becomes a new baseline instead of a delta.
Without it, a reading arriving three hours after the previous one has its whole delta charged to one bucket, as though
three hours of traffic happened in one interval — a spike exactly where the data is thinnest. The event comes back as
`stale`, distinct from `reset`. Default 0, meaning unlimited, so existing series keep their behaviour.

## The API

```
ts_init(bin, step_ms, slots, [opts], [cfg_bin], [now_ms])
ts_append(bin, ts_ms, value, [cfg_bin])
ts_append_all(bins, ts_ms, values, [cfg_bin])
ts_append_many(bin, times, values, [cfg_bin])
ts_report(bin, [from_ms], [to_ms], [max_points], [cfg_bin], [res], [now_ms])
ts_stats(bin, [from_ms], [to_ms], [cfg_bin], [res], [now_ms])
ts_info(bin, [cfg_bin])
ts_rollup(bin, [op], [max_points], [cfg_bin], [step_ms], [slots], [res])   -- a stream aggregation
```

`opts` is comma-separated `key=value`: `agg`, `kind`, `mode`, `backfill`, `wipe`, `wrap`, `archives`, `heartbeat`, `xff`.
**An unknown key is an error**, because a typo that is ignored gives you a series you did not ask for and will not notice
until the numbers are wrong.

`archives` is `default`, `none`, or a semicolon-separated list of `resolution:slots[:agg[:xff]]` —
`s:3600:last;m:1440;h:336:mean:50`. Semicolons rather than commas because the options string is already
comma-separated, and an options parser that needs escaping is one people get wrong. `agg` defaults to `mean` and `xff`
to 0. `xff=` as a top-level option sets every archive's, and applies **whichever side of `archives=` it is written on** —
a string whose meaning depends on key order is a trap.

`res` selects an archive: `s`, `m`, `h`, `d`, `w`, `mo`, `y`. Without it, `ts_report` and `ts_stats` pick **the finest
archive that still reaches back to `from_ms`** and say which and why in `resolution` and `archive_chosen_because`.
Coarser archives exist to cover longer ranges, not to save work — downsampling in the UDF costs a few thousand additions,
so there is never a reason to answer from a coarser archive than the data allows. If nothing reaches far enough the
coarsest answers and `result` is **`truncated`**, not `ok`: the numbers are real but the window is not the one asked for.
Naming an archive the series does not keep is an error rather than a quiet substitution.

Every result map carries the shape — `step_ms`, `slots`, `agg`, `kind`, `mode` — plus:

| verb | keys |
|---|---|
| `ts_init` | `bin`, `config_bin`, `archives`, `rings_created`, `rings_existing`, `ring_bytes`, `table`, optional `heartbeat_ms`, `result` = created \| exists \| completed |
| `ts_append` | `result` = placed \| combined \| backfilled \| too_old \| backfill_refused \| would_wipe; `accepted`, `bucket`, `bucket_ms`, `cleared`, `last_bucket`; with archives `archives`, `archives_accepted`, `archives_refused`, `per_archive`; and when they apply `counter` = baseline \| delta \| wrapped \| reset \| stale, `stored`, `counter_resets`, `saturated` |
| `ts_append_all` | `metrics`, `rings_accepted`, `rings_refused`, `per_metric`, `result` = ok \| partial \| refused \| empty |
| `ts_append_many` | `offered`, `accepted`, `placed`, `combined`, `backfilled`, `too_old`, `backfill_refused`, `would_wipe`, `cleared`, `last_bucket`, optional `archives`, `counter_resets`, `saturated` |
| `ts_report` | `from_ms`, `to_ms`, `bucket_ms`, `points`, `known`, `unknown`, `downsampled_with`, `values`, `appends`, `last_ms`, optional `resolution`, `archive_bin`, `archive_chosen_because`, `xff_suppressed`, `result` = ok \| truncated \| empty \| no_overlap |
| `ts_stats` | `from_ms`, `to_ms`, `buckets`, `count`, `unknown`, `sum`, `mean`, `min`, `max`, `first`, `first_ms`, `last`, `last_ms` — **and no list** |
| `ts_info` | `created_ms`, `archives`, `rings_present`, `ring_bytes`, `allow_backfill`, `allow_wipe`, optional `wrap_bits`, `heartbeat_ms`, and `table`: one entry per ring with `index`, `bin`, `resolution`, `slots`, `agg`, `bytes`, `span_ms`, `step_ms`, `state` = ok \| empty \| missing \| unreadable, plus `from_ms`/`to_ms`/`appends` where they apply |
| `ts_rollup` | `op`, `records`, `samples`, `skipped_absent`, `skipped_shape`, `skipped_unreadable`, `from_ms`, `to_ms`, `bucket_ms`, `points`, `known`, `unknown`, `values`, `counts`, `shape_declared`, optional `resolution`, `archive_bin`, `dropped_old`, `result` = ok \| shape_conflict \| empty |

The appending verbs and `ts_info` take `&mut Record`; `ts_report`, `ts_stats` and the rollup take `&Record`. `ts_info`
needs the mutable one only because the zero-copy read path does — it writes nothing.

With archives, `ts_append`'s `result` is the **finest** archive's outcome, the closest analogue of the single ring, and
`per_archive` gives all seven. One sample normally reads `placed` on the second archive and `combined` on every coarser
one; that pattern *is* consolidation happening, not something to act on.

One wrinkle worth knowing: `last_ms` means the *ring's* newest bucket in `ts_report`, and the newest bucket that
actually holds a **sample** in `ts_stats`. They differ whenever the newest slots are unknown.

## What the calls actually return

Everything below is real output from `tests/demo.rs`, not a sketch of it:

```sh
AEROSPIKE_HOSTS=… cargo test --test demo -- --ignored --nocapture
```

### A gauge series, with a gap

Twelve one-minute buckets of request latency. Buckets 4 and 5 were never reported — the host was down.

```text
ts_report("latency")

    bin                "latency"
    step_ms            60000
    slots              12
    kind               "f64"
    mode               "gauge"
    agg                "last"
    result             "ok"
    bucket_ms          60000
    from_ms            1767225600000
    to_ms              1767226260000
    last_ms            1767226260000
    points             12
    known              10
    unknown            2
    downsampled_with   "last"
    values             [12.4, 15.1, 11.8, 45.2, nil, nil, 98.6, 14.0, 13.2, 12.9, 16.4, 12.1]
    appends            10
```

**The two gaps are `nil`, not `0.0`.** A minute nobody reported and a minute that reported zero latency are different
facts, and a series that cannot tell them apart makes every outage look like a very fast one.

Timestamps are **implied rather than sent**: value `i` covers `from_ms + i × bucket_ms`. That halves the wire against a
list of pairs and is exactly reconstructible.

### Downsampled on the server

```text
ts_report("latency", nil, nil, 4)

    bucket_ms          180000
    from_ms            1767225600000
    to_ms              1767226260000
    points             4
    known              4
    unknown            0
    downsampled_with   "last"
    values             [11.8, 45.2, 13.2, 12.1]
```

Twelve buckets into four, three to a group, combined with the configured `last`. `bucket_ms` rose to 180,000 so the
caller can see the resolution it got.

Note `unknown` fell to 0: the group holding buckets 3, 4 and 5 still has a value, because **an unknown combines as a
no-op rather than an eraser** — 45.2 survives the two nils beside it.

### A window

```text
ts_report("latency", 1767225960000, 1767226080000)

    points             3
    from_ms            1767225960000
    to_ms              1767226080000
    last_ms            1767226260000
    values             [98.6, 14.0, 13.2]
```

`from_ms`/`to_ms` are clamped to the live window, and `last_ms` still reports where the ring actually ends.

### `ts_stats` — the cheapest read

```text
ts_stats("latency")

    result             "ok"
    from_ms            1767225600000
    to_ms              1767226260000
    last_ms            1767226260000
    buckets            12
    unknown            2
    count              10
    sum                251.7
    mean               25.169999999999998
    min                11.8
    max                98.6
    first              12.4
    first_ms           1767225600000
    last               12.1
```

**No `values` key at all.** When the question is "what is the p100 and the mean", shipping twelve numbers to compute
two is the wrong trade — and at 65,536 slots it is 512 KB to compute two.

### A counter series

An interface byte counter sampled every minute, which restarts at bucket 4 because the process was bounced.

```text
ts_report("rx_bytes")

    kind               "i64"
    mode               "counter"
    agg                "sum"
    points             8
    known              6
    unknown            2
    values             [nil, 240000, 265000, 215000, nil, 256000, 245000, 235000]
    appends            8
    counter_resets     1
```

Two things are being reported honestly here.

**Bucket 0 is `nil` because one reading is not a rate.** The first sample is recorded as a baseline and nothing else; a
delta needs two.

**Bucket 4 is `nil` because the counter restarted**, and `counter_resets` says so. The naive answer would have been
`4000 − 1720000 = −1716000`, and the naive *fix* — declaring `wrap=32` and reconstructing — would have given
`4000 + 2³² − 1720000 = 4293251296`, a four-billion-byte spike that never happened. A reconstructed wrap is only
accepted when the delta is under half the declared width, because a real wrap sits near the ceiling and rolls just past
zero while a restart from a low value reconstructs to nearly the whole range.

```text
ts_stats("rx_bytes")

    count              6
    sum                1456000
    mean               242666.66666666666
    min                215000
    max                265000
    first              240000
    first_ms           1767225660000
    last               235000
```

`sum` and the extremes come back as **integers**, exactly. An `i64` ring never passes through an `f64`, so a byte
counter above 2⁵³ keeps every bit — the same reason the `summary` example keeps a separate integer accumulator.
`first_ms` is bucket 1, not bucket 0, because bucket 0 holds no sample.

### An archive hierarchy

Ten samples, one per second, values 1 through 10.

```text
ts_init("cpu", 1000, 3600, "archives=default")

    bin                "cpu"
    step_ms            1000
    slots              3600
    kind               "f64"
    mode               "gauge"
    agg                "last"
    config_bin         "rrcfg"
    ring_bytes         62232
    result             "created"
    archives           7
    rings_created      7
    rings_existing     0
```

The `table` it returns, one entry per line rather than the single long value the demo prints:

| index | bin | resolution | slots | agg | bytes | span_ms |
|---|---|---|---|---|---|---|
| 0 | `cpu~s` | second | 3,600 | last | 28,856 | 3,600,000 |
| 1 | `cpu~m` | minute | 1,440 | mean | 23,096 | 86,400,000 |
| 2 | `cpu~h` | hour | 336 | mean | 5,432 | 1,209,600,000 |
| 3 | `cpu~d` | day | 100 | mean | 1,656 | 8,640,000,000 |
| 4 | `cpu~w` | week | 104 | mean | 1,720 | 62,899,200,000 |
| 5 | `cpu~mo` | month | 60 | mean | 1,016 | 157,784,760,000 |
| 6 | `cpu~y` | year | 25 | mean | 456 | 788,923,800,000 |

The tenth sample, and where it landed:

```text
ts_append("cpu", base + 9000, 10.0)

    result             "placed"
    accepted           1
    bucket             1767225609
    bucket_ms          1767225609000
    cleared            0
    stored             10.0
    last_bucket        1767225609
    archives           7
    archives_accepted  7
    archives_refused   0
    per_archive        {day: "combined", hour: "combined", minute: "combined", month: "combined",
                        second: "placed", week: "combined", year: "combined"}
```

**`placed` once and `combined` six times is the normal shape of an append** — a new second, the same minute, hour, day,
week, month and year. That line is consolidation happening.

Read back at two resolutions:

```text
ts_report("cpu", base, base + 9000, nil, nil, "s")

    step_ms            1000
    bucket_ms          1000
    points             10
    known              10
    downsampled_with   "last"
    values             [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    resolution         "second"
    archive_bin        "cpu~s"
    archive_chosen_because "asked for by name"

ts_report("cpu", base, base + 9000, nil, nil, "h")

    step_ms            3600000
    bucket_ms          3600000
    points             1
    known              1
    downsampled_with   "mean"
    values             [5.5]
    resolution         "hour"
    archive_bin        "cpu~h"
```

`5.5` is the mean of all ten raw samples, not a mean of per-minute means.

And with no resolution named, asking for three years back:

```text
ts_report("cpu", base - 3y, nil, 4, nil, nil, base)

    step_ms            2629746000
    slots              60
    agg                "mean"
    result             "ok"
    bucket_ms          26297460000
    from_ms            1672531200000
    to_ms              1767225600000
    points             4
    known              1
    unknown            3
    values             [nil, nil, nil, 5.5]
    resolution         "month"
    archive_bin        "cpu~mo"
    archive_chosen_because "the finest archive that reaches back that far"
```

The month archive, because the week archive covers two years and this asks for three. `ts_info("cpu")` reports the same
table with each ring's current window and `appends` alongside it.

### `ts_rollup` — three hosts into one series

Three records reporting `10 + b`, `20 + b` and `30 + b` requests per second across six buckets.

```text
ts_rollup("rps")

    bin                "rps"
    op                 "sum"
    step_ms            60000
    slots              6
    kind               "f64"
    result             "ok"
    bucket_ms          60000
    from_ms            1767225600000
    to_ms              1767225900000
    points             6
    known              6
    unknown            0
    values             [60.0, 63.0, 66.0, 69.0, 72.0, 75.0]
    counts             [3, 3, 3, 3, 3, 3]
    records            3
    samples            18
    skipped_absent     0
    skipped_shape      0
    skipped_unreadable 0
```

`60 + 3b`, which is what `(10 + b) + (20 + b) + (30 + b)` should be. **`counts` is the second list, and it is what makes
a mean across records possible** without doubling every stored ring: divide `values` by `counts`.

```text
ts_rollup("rps", "max")

    op                 "max"
    values             [30.0, 31.0, 32.0, 33.0, 34.0, 35.0]
    counts             [3, 3, 3, 3, 3, 3]
```

The worst host in each bucket. `counts` still reports three contributors — it counts what was *seen*, not what won.

```text
ts_rollup("rps", "sum", 3)

    bucket_ms          120000
    points             3
    values             [123.0, 135.0, 147.0]
    counts             [6, 6, 6]
```

Summed across records and then downsampled two buckets to a point: `60+63`, `66+69`, `72+75`. `counts` becomes 6,
because six record-buckets went into each answer.

**`records`, `samples` and the three `skipped_*` counters are always present.** A rollup that quietly saw two records
out of ten thousand and returned a confident-looking series is the failure this example is most concerned with.

## Assumptions

**Time.** Milliseconds since the Unix epoch. A bucket is `floor(t / step)` anchored to the epoch, so **nothing stores an
epoch** — and that is exactly what lets the rollup align records by index, with no timestamp matching, no interpolation
and nothing to sort. Division is `div_euclid`, so a pre-1970 timestamp floors instead of truncating toward zero and
landing in the epoch's own bucket.

**Liveness.** A slot is live iff its bucket is in `(last_bucket − slots, last_bucket]`. Outside that the bytes are a
retired bucket's leftovers, and `at()` reports them as unknown.

**Alignment — the ring is never 8-byte aligned and cannot be made so.** A blob particle is a type byte plus a 4-byte
length, so its data starts five bytes in and the pointer is always `≡ 1 (mod 4)`. Padding at write time does not help:
the reader would have to know how much to skip. Every sample therefore goes through `from_le_bytes`/`to_le_bytes` and
**never** a cast to `&[f64]`, which is undefined behaviour in Rust even though aarch64 and x86-64 both tolerate
unaligned loads. Explicit little-endian also makes a ring written on one architecture readable on another.

**Unknown is not zero.** `NaN` for f64, `i64::MIN` for i64, reported as nil. No validity bitmap needed, and an unknown
never overwrites a known value when combining.

**Which is why a non-finite sample is refused.** `NaN` *is* the unknown marker, so storing one would report a successful
write of a gap — the one failure a caller cannot detect. An infinity fails the other way: every aggregation treats it as
a real value, so one of them poisons a `sum` or `mean` bucket permanently and no later sample can recover it. Both are
refused on the single-sample path and on the bulk path, and the bulk path checks the **whole batch before writing
anything**, so a bad value halfway through does not leave the first half applied.

**Two positions are not representable and are refused rather than approximated.** `i64::MIN` is the "never written"
sentinel, so a sample whose bucket is `i64::MIN` — reachable only at a one-millisecond step with a timestamp of exactly
`i64::MIN` — would be indistinguishable from an empty ring. And a **one-slot ring is exempt from the wipe guard**: its
window is a single bucket, so every forward advance erases the whole window, and the guard would otherwise forbid every
append after the first on a shape `ts_init` happily creates.

**Every bucket difference is saturating.** A ring anchored near one end of `i64` and a sample near the other produce a gap
that does not fit an `i64`; in release a wrapped subtraction turns "impossibly far apart" into a small number, which is
the one answer that lets the append proceed. `window()` clamps for the same reason — there are no buckets below
`i64::MIN` to include.

**The config fingerprint** catches a config edited behind a ring's back. The dangerous case is a *same-shape* edit —
change `step_ms` from 60,000 to 1,000 and every byte still decodes while every value now means something else. A ring
stores the fingerprint of the config it was built against and refuses to open against another.

It is a hash of the config's **canonical encoding**, not of the bin's bytes. Hashing the first 40 bytes was correct while
that was the whole config and silently became *the header only* when the archive table was appended — so every ring in an
archived series failed to validate against the config it had just been built from. Re-encoding also makes two bins that
decode to the same config agree, whatever trailing bytes one of them carries.

The fingerprint is checked **last**, after the magic, version, length, kind, mode, resolution and aggregation. Those say
*what* is wrong; the fingerprint says only that something moved. Running it first answered "check the bin name" with "the
fingerprint differs" — true, unhelpful, and the first version of that function.

`created_ms` is part of the config and therefore part of the fingerprint, so two records initialised at different
instants carry different fingerprints. That is harmless, because a ring is only ever checked against **its own record's**
config — but it does mean comparing two records' rings byte for byte needs a pinned `now_ms`.

**Refusals change nothing at all** — not one byte, including a counter's baseline, which would otherwise be advanced by
an append that never happened.

**An append that would erase the whole window is refused by default.** If the gap reaches `slots`, every slot dies, and
that is the shape of a seconds-versus-milliseconds mistake. A rejection is recoverable and a wipe is not, so it takes
`wipe=1` to opt in.

**And the refusal cascades, which is a sharper edge than it looks.** A rejected append does not move `last_bucket` — that
is what makes it non-destructive — so every later sample is still a whole window away and is also refused. A series that
goes quiet for longer than it can hold therefore never accepts anything again on its own: `wipe=1`, a re-initialise, or a
different config bin is required. A one-hour window and a two-hour outage is an ordinary Tuesday rather than a
misconfiguration, so this bites in a case the guard was not designed for. `an_outage_longer_than_the_window_needs_an_explicit_decision`
pins the behaviour and both recoveries; whether the default should change is a live question and not one this example
decides quietly.

**Exactness.** An `i64` ring stays in `i64`; slot sums saturate and set a flag rather than wrapping, and a double
offered to an integer ring is refused rather than truncated.

**Bounds.** `MAX_SLOTS` is 65,536 — 512 KB of samples — validated on the way in, with `slots × 8` overflow-checked. An
unclamped `slots` would let a caller's integer drive an allocation.

**Rollup.** Rings that disagree on shape are refused and counted, never resampled: resampling would produce a plausible
number from series that were never comparable. State is bounded to a window anchored on the highest bucket seen, and
anything older is counted in `dropped_old`. `op` is restricted to `sum`, `min`, `max` and `last`, because `count` comes
back regardless and `first` has no meaning across records — records have no order, so "first" would be whichever
partition answered soonest. Aggregation records are read-only, so the rollup only ever reads. Cost is O(records × slots).

**`mean` exists only where a count is worth its bytes.** It needs a `count` per slot, which makes the slot 16 bytes
instead of 8 — not worth doubling a single stored ring, which is why there was no `mean` at all while there was one ring.
Archives changed that: a coarse archive is small, RRDtool's default consolidation is AVERAGE, and the count doubles as the
`xff` input. The default set therefore uses `last` for the per-second archive — the largest one, where a bucket normally
holds a single sample and the two are the same number — and `mean` for every coarser one. Without archives, `sum` plus a
second ring configured `count` still gives the same answer with the cost visible in the record.

The rollup *accumulator* keeps a parallel count ring regardless — it is transient and there is one per query thread — and
that asymmetry is the point rather than an inconsistency.

## What the scale tests establish

`cargo test --release --test scale` drives `RingMut` directly, so it can afford millions of samples; every expectation is
recomputed by a second implementation that shares no code with the ring. `--test livescale` does the same through real
particles. Both take `TS_SCALE`.

| claim | how it is checked |
|---|---|
| every bucket of every resolution holds exactly the right aggregate | 10M one-second samples, each archive verified bucket by bucket against an independent accumulator |
| a 16-slot ring lapping 625,000 times leaks nothing | 64 checkpoints, each asserting the whole window plus the bucket below it |
| every slot of a 65,536-slot ring is reachable and distinct | a distinct value per bucket, then a second lap replacing all of them |
| three calendar years is 36 months, 1,096 days and 3 years | an hourly run across a leap year, every bucket boundary 28–31 days |
| arrival order does not change an order-free aggregation | the same samples in order and scattered within each bucket |
| `xff` flips exactly at its threshold | 3,600 appends, asserting the report changes at 1,800 |
| a counter's deltas add up across resets | 2M readings restarting 19 times; the live window's deltas summed independently |
| a declared 32-bit counter reconstructs every lap | 166,666 wraps, none recorded as a reset |
| an `i64` sum saturates rather than wrapping | `3 × (i64::MAX / 3)` fits; the fourth clamps and sets the flag |

**Precision.** A `Mean` slot keeps a running `f64` sum, so it accumulates naive-summation error. Measured against Neumaier
compensated summation, worst case — one bucket, values of 0.1:

| samples in one bucket | relative error |
|---|---|
| 1,000,000 | 1.3 × 10⁻¹¹ |
| 10,000,000 | 1.6 × 10⁻¹⁰ |
| **31,536,000** (a year of per-second samples) | **5.7 × 10⁻¹⁰** |

That last row is the production worst case for the year archive, and eleven significant digits survive it. Integer-valued
samples are **exact** at any of these scales — a sum of 31 million values under 1,000 is about 3 × 10¹⁰, well inside an
`f64`'s exact integer range — which is why every other test uses them and a logic error cannot hide behind rounding.

**Through real particles**, at 5M samples:

| | |
|---|---|
| archived series, 7 rings | 3.76M samples/s in 50,000-sample batches, 0.3 µs/sample |
| 512 KB single ring, 76 laps | 6.35M samples/s |
| record after 5M samples | 8 bins, 62,411 bytes — a ring does not grow |

**Sixteen metrics per record, verified rather than computed:** 113 bins and **996,375 bytes**, 52,201 short of a 1 MB write
block, and a seventeenth is refused with `RecordTooBig`. The figure is a real limit, not an estimate. An impossible archive
set — seven archives at the slot ceiling, 7.3 MB — is refused **atomically**: `ts_init` stages every ring and commits once,
so the record is left with nothing rather than six rings and a config describing seven.

**One sharp edge worth knowing:** `max_points` just below `slots` halves the resolution, because the downsampling factor is
an integer. Asking for 65,535 points of a 65,536-bucket window gives 32,768 — `ceil(65536/65535)` is 2. Ask for `slots` or
more to get full resolution.

## What it costs

### The archive hierarchy

`AEROSPIKE_HOSTS=… cargo test --release --test archbench -- --ignored --nocapture`:

| archives | ring bytes | coarsest | µs/round trip | vs 1 | µs/sample batched | vs 1 |
|---|---|---|---|---|---|---|
| 1 | 28,856 | second | 82.6 | 1.00× | 0.65 | 1.00× |
| 2 | 51,952 | minute | 94.5 | 1.14× | 0.64 | 0.98× |
| 3 | 57,384 | hour | 93.7 | 1.13× | 0.73 | 1.11× |
| 4 | 59,040 | day | 94.2 | 1.14× | 0.72 | 1.10× |
| 5 | 60,760 | week | 99.7 | 1.21× | 0.71 | 1.09× |
| 6 | 61,776 | month | 96.3 | 1.17× | 0.78 | 1.19× |
| **7** | **62,232** | **year** | **96.9** | **1.17×** | **0.78** | **1.19×** |

**Twenty-five years of retention costs 17% more per append than one hour of it.** The design notes carried an *estimate*
of about 400 µs for a seven-archive append, reasoned from the ring sizes; the measurement is 97 µs, so the estimate was
four times too pessimistic and is deleted rather than left standing beside the number.

The two columns say where the cost is. Batched — 200 samples in one call — the marginal cost of six extra archives is
0.13 µs per sample, because the extra work is six slot writes. Per round trip it is 14 µs, because the record commit goes
from 29 KB to 62 KB. **The hierarchy is paid for in bytes committed, not in archives walked**, which is why the last four
rows are nearly flat: they add 3 KB between them.

The first version of this benchmark produced a curve that *fell* as archives were added — seven apparently cheaper than
one, which cannot happen when the only difference is more work. The cold row was the first one, paying for the process's
first round trip rather than for its single ring. Warming the connection outside the sweep fixed it.

### `ts_append_all` against one call per metric

| metrics | archives each | N × `ts_append` | `ts_append_all` | gain |
|---|---|---|---|---|
| 1 | 3 | 81.2 µs | 66.9 µs | 1.21× |
| 4 | 3 | 509.4 µs | 110.1 µs | 4.63× |
| 8 | 3 | 1,292.7 µs | 189.8 µs | 6.81× |
| **16** | 3 | **4,666.2 µs** | **359.2 µs** | **12.99×** |

Sixteen metrics is the number that fits a record, so 13× is the realistic figure for an agent sampling a host. The same
rings are written either way; what disappears is fifteen round trips, fifteen config reads and fifteen record commits.

At one metric it is still slightly ahead rather than level, because `ts_append` builds the larger result — the
per-archive outcome map that `ts_append_all` does not return. Cold, this row read 0.76× and said batching a single metric
was a loss; the sign was an artifact too.

### The in-place append

One append against ring size, before and after the ring was made to mutate in place:

| slots | ring bytes | copying | in place |
|---|---|---|---|
| 64 | 552 B | 71 µs | 66 µs |
| 1,024 | 8 KB | 77 µs | 74 µs |
| 8,192 | 64 KB | 97 µs | 84 µs |
| **65,536** | **512 KB** | **390 µs** | **187 µs** |

**2.1× on a 512 KB ring, and nothing measurable below about 64 KB** — under that the round trip dominates and the copies
are noise.

An append used to copy the ring **four** times: the host materialises the particle into a private value,
this code copied that into a `Vec`, `set_blob` copied it back into another value, and the write copied that into the new
particle. `Record::with_bytes_mut` removes the middle two. The two that remain are the floor, not an oversight — the
private copy is what makes mutating safe, and building the new particle is what makes it durable.

### Against the alternative anyone actually has

`AEROSPIKE_HOSTS=… cargo test --release --test bench -- --ignored --nocapture`, one node over a LAN:

| operation | this | CDT list |
|---|---|---|
| append, per sample (4,096 slots) | 96 µs | **75 µs** |
| batched, per sample | 0.2 µs | **0.1 µs** |
| read 512 KB, client-side | **388 µs** | 689 µs |
| `ts_report(200)` server-side, 512 KB | 421 µs | — |
| `ts_rollup`, 200 × 4,096 slots | 5,299 µs | **4,163 µs** client-side |

**Read this honestly: on a single-node LAN, a CDT list wins on writes and the server-side computation does not pay.**
Appending is 1.28× the list per sample and 2.35× batched. Downsampling on the server is 0.92× fetching the blob and
doing it locally — *even at 512 KB*, where the wire cost should have favoured it. The rollup loses too, at 0.79×.

What this design does win:

- **Reading is 1.8× the CDT list** at 512 KB, 388 µs against 689, because there is no msgpack to parse. That is the
  blob representation earning its place, not the server-side computation.
- **Batching is worth 484×** over one call per sample, whichever representation you use.
- Bounded, fixed-size storage with no trim to remember.
- Semantics a CDT cannot express at all: counter deltas with reset detection, per-step aggregation, unknown ≠ zero, and
  refusing an append that would destroy the window.

If the deciding factor is append throughput on one node, a CDT list is the better answer and this README should say so.
The case for a ring is the read path, the bounded storage, and the correctness properties.

## Requires a host with `rec_get_bytes_mut`

In-place mutation needs an ABI addition — `rec_get_bytes_mut`, appended to the end of the host vtable — plus
`Record::with_bytes_mut` in the SDK. `Record::supports_bytes_mut()` reports whether the host has it, and
`with_bytes_mut` returns an error rather than guessing.

Nothing the storage engine owns is touched: the host materialises the particle into the call's **own** private value and
hands back a pointer into that, having marked the bin so the write path picks the bytes up. The stored particle is
untouched, so there is no replication, durability or visibility hazard.

`with_bytes_mut` takes `&mut self` and a closure, and both are load-bearing. `&mut self` is what stops a slice obtained
earlier from `get_bytes` — which borrows `&self` — from still being alive and aliasing the mutable one; the compiler
enforces it. The closure stops the slice escaping. **That is the distinction an earlier borrowed *read* did not have**,
and why it was reverted: it handed out a slice into server-owned memory from an `&self` method, so nothing prevented a
caller reading a bin, writing it, and then using the first slice.

## No dependencies

A ring buffer, a fixed-layout header and some slot arithmetic. One dependency: the SDK.

## Running it

Configure the module in the server, then:

```sh
cargo test                                              # the arithmetic and the layouts, no server
cargo test --release --test scale                       # millions of samples, no server
AEROSPIKE_HOSTS=… cargo test --test against_a_server -- --test-threads=1
AEROSPIKE_HOSTS=… cargo test --test demo -- --ignored --nocapture
AEROSPIKE_HOSTS=… cargo test --release --test bench -- --ignored --nocapture
AEROSPIKE_HOSTS=… cargo test --release --test archbench -- --ignored --nocapture
AEROSPIKE_HOSTS=… cargo test --release --test livescale -- --ignored --nocapture --test-threads=1
```

`scale` and `livescale` take `TS_SCALE` (and `livescale` a `TS_BATCH`), so `TS_SCALE=10000000` is a heavier run of the same
assertions — see [what the scale tests establish](#what-the-scale-tests-establish).

Unit tests cover what is silently wrong when it is wrong: slot arithmetic, retire-on-advance, counter resets versus
wraps, calendar bucketing, and the refusals that protect a ring from a config that changed underneath it — including one
archive being opened as another. `ts_report` and `ts_stats` build their maps through the host, so they are asserted
against a real server instead.

**A connection failure with `AEROSPIKE_HOSTS` set is a failure, not a skip.** Elsewhere in this project a suite reported
"11 passed" in 0.07 seconds because every test quietly returned early when the client could not connect.

`--test-threads=1` is not decoration: nine tests each building a client concurrently reliably fails to connect against
this development cluster, while the same tests pass serially. That looks like a client-side problem rather than a
server one and is not yet explained.
