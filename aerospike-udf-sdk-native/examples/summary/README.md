# A summary-statistics native UDF

Twelve-odd statistics over a numeric bin, in **one pass and bounded state**, over any number of records.

```text
summary("latency_ms")                 everything below
summary("latency_ms", 0.005)          tighter percentiles: 0.5% relative error
summary("latency_ms", 0.01, 256)      and a wider frequency table for the mode
```

| statistic | how | exact? |
|---|---|---|
| count, min, max, range | running | **exact** |
| sum | two accumulators, `i128` and compensated `f64` | **exact** for an integer bin |
| mean | that sum over the count | one rounding |
| variance, stddev, CV | Welford | **exact** |
| skewness, excess kurtosis | Welford, 3rd and 4th moments | **exact** |
| geometric mean, harmonic mean | Σ log, Σ 1/x | **exact** |
| p25, median, p75, p90, p95, p99, IQR | relative-error sketch | within **ε**, default 1% |
| mode, mode count | Space-Saving | bounded — and it reports when it is certain |
| distinct | HyperLogLog, 1 KB | ~1.6% standard error |

## What it costs

100,000 records, `AEROSPIKE_HOSTS=… cargo test --release --test bench -- --ignored --nocapture`:

| how | ms | ns/record |
|---|---|---|
| query + dispatch, no bin read | 7 | 65 |
| **summary: every statistic above** | **15** | **147** |
| client: fetch every record, compute locally | 101 | 1,009 |

**6.8× faster than fetching**, and it ships a handful of partials — one per query thread — instead of 100,000 records.
The database costs 1.6× the arithmetic — 147 ns against that same run's own in-memory figure of 93, not against the
table below, which comes from quieter server-free runs. The biggest single line item is the 65 ns the query and
dispatch cost before this plugin does anything at all.

### Where the arithmetic goes

Attributed **in memory**, single-threaded, not from the server — and that is a correction rather than a shortcut. The
first version of this benchmark attributed components from server-side probes and produced deltas of +74, +0, −3, +16
and **−22** ns. A negative delta is impossible when each stage does strictly more than the last: four query threads
divide the per-record CPU cost into wall clock, so a component worth a few ns disappears under the ~10% spread between
runs.

| accumulating | ns/record | delta |
|---|---|---|
| moments (variance, skewness, kurtosis) | 7.4 | — |
| + quantile sketch | 18.9 | +11.5 |
| + Space-Saving | 80.3 | +61.4 |
| + HyperLogLog | 82.6 | +2.2 |
| + the two sums | 84.2 | +1.6 |
| + geometric and harmonic means | 87.4 | +3.2 |

The two halves of the benchmark now run independently, and this one needs **no server at all** — it is in-process
arithmetic over a `Vec<f64>`. It used to sit behind the connection anyway, so a machine that could not reach a cluster
got no attribution table either. That mattered exactly once, and it is the next section.

### The benchmark found a real hotspot, and then caught a regression

Space-Saving was **127 ns/record** — 83% of the arithmetic, against 11.6 for every moment. The cause was visible in the
benchmark's own output: 103,759 distinct values against a 64-row table, so every observation missed, and each miss did
*two* linear scans, one to look the key up and one to find the weakest row. Combining them into a single pass and
comparing bit patterns rather than floats halved it to 63.5.

Later, HyperLogLog's row jumped from **+7.8 ns/record to +258**, and that one was self-inflicted. A safety fix had added
a clamp over all 1024 registers inside `slots()` — which `push()` calls **per record** — turning an O(1) update into a
1024-byte scan. It was reasoned about and shipped without re-measuring, and the reasoning was even correct: a hostile
register byte really can reach a `1u64 << r`. The clamp simply belonged where the value is *used* rather than where it
is reached. It now sits in `estimate()`, which runs once per query, and removing it still makes
`a_hostile_register_byte_cannot_overflow_the_shift` panic with `attempt to shift left with overflow` — so the guarantee
is unchanged and the scan is gone.

That regression also exposed a flaw in the harness that had been hiding its size: the final row was hand-timed with no
warm-up while every row above it warmed on 1000 values, so the noisiest number in the table was the one a regression
would show up in first. Every row goes through the same timing path now, and the two components that were on the
per-record path but in *no* row — the sums and the multiplicative means — are in the table, so "= everything" is true.

But the number that matters more is the other one:

| Space-Saving alone | ns/record |
|---|---|
| ~104,000 distinct values — its worst case | 68.2 |
| 4 distinct values — what mode is actually for | **3.7** |

**Mode is expensive exactly when it is meaningless.** On the low-cardinality data anybody actually asks the mode of,
the table hits early and never evicts, and it costs almost nothing. On data where every value is unique it costs the
most and returns whatever the tie-break picked. `distinct_values_tracked` in the result is how a caller tells which
situation they are in.

## The split that shapes everything

**Some statistics collapse into a handful of numbers no matter how many records there are.** Count, sum, min, max and
every moment do, and they merge exactly, so they fit the aggregation contract with nothing lost.

**Order statistics cannot.** Any algorithm that reports an exact median must be able to distinguish every value it
might return, so exact medians, modes and distinct counts need state proportional to the data. That is
information-theoretic, not an implementation shortcoming, and no cleverness removes it.

So the second group is approximate **with a stated, provable error** — and each one reports enough for a caller to
know how far to trust it: `percentile_error`, `mode_is_certain`, `mode_count_error`, `distinct_error`,
`percentile_collapsed`. A number that might be wrong and does not say so is worse than no number.

## Two sums, combined once

An Aerospike integer bin is an `i64`. Widening it to `f64` on the way in throws bits away **before any arithmetic
happens**: past 2⁵³ consecutive integers stop being representable, so `2⁵³ + 1` *becomes* `2⁵³` at the cast and nothing
downstream can recover it. Nanosecond timestamps, byte counters and snowflake ids all live up there.

So there are two accumulators. Integers are summed as integers in an `i128`; doubles are summed as doubles with Neumaier
compensation. Neither ever sees the other's values, and the two totals meet **exactly once**, in `finalize`, because the
answer has to leave as a single Aerospike double. Folding them together per record would round 100,000 times to save
carrying one extra field.

| key | when | what |
|---|---|---|
| `sum` | always | both sides as an `f64` — useful, and lossy past 2⁵³ |
| `sum_exact` | integer bin, total fits `i64` | **the exact total** |
| `sum_exact_str` | integer bin, total exceeds `i64` | the exact total as a decimal string |

A mixed-type bin gets neither exact key, which is the honest answer: the combination went through a rounding. There is
no 128-bit numeric anywhere in the path — not in `PartialField`, not in the native ABI, not in Aerospike — so the `i128`
travels as its two halves and is reassembled on read. A live test sums 2,000 records of `2⁵³ + i` and gets
`18014398509483983000` back through a real query and a real merge, exact, as a string because no Aerospike integer is
wide enough.

**The integer side provably cannot overflow.** Values are `i64` so `|v| ≤ 2⁶³`, and the count is an `i64` so
`n ≤ 2⁶³ − 1`; the largest reachable total is `2¹²⁶ − 2⁶³` against `i128::MAX = 2¹²⁷ − 1`, a factor of two spare. It is
still checked, because **a partial is not a dataset** — `from_partial` accepts whatever two `i64`s arrive, so the
accumulator can be *handed* a total at `i128::MAX` by a malformed one. Overflow saturates and sets a flag the result
reports; the same reasoning put a clamp on the HyperLogLog registers. Removing the check makes the hostile-partial test
panic with `attempt to add with overflow`, which is how it is known to be load-bearing rather than decorative.

### What this replaced, and what it cost

The sum used to be `mean · n`, on the reasoning that it is the same number to within the rounding Welford already
accepts. Measured over 100,000 values near 10¹⁷, it is not:

| summing | ns/value | error |
|---|---|---|
| plain `f64` loop | 1.04 | 34 ulps |
| `mean · n`, which this replaced | 5.44 | 18 ulps |
| **Neumaier compensated** | **1.25** | **0 ulps** |
| Shewchuk expansion, exact for any length | 4.83 | 0 ulps |

An 18-ulp sum is a perfectly good sum. The problem was that this README's own table called it **exact**. The whole
change costs **+1.5 to +3 ns/record** across runs, about 2% of the aggregation — small enough that it sits near this
benchmark's own noise floor, which is why it is quoted as a range.

Shewchuk is *provably* exact rather than exact-on-this-data, and costs 4× plus a `Vec` per accumulator to close a gap
between one ulp and zero that no caller can observe — twelve orders of magnitude below the 1% this module already
reports on its percentiles. Arbitrary-precision floats are strictly worse than that floor. `f128` is not an option at
all: unstable in Rust, and soft-float on both x86-64 and aarch64, so every add would be a call into compiler-rt.

**The reference cannot be an `f64`, and that is not a detail.** Two versions of the test that proves this failed on
their own premise before the reason became clear: a total an `f64` can represent exactly is a total the naive loop
*computes* exactly. Those are the same condition, so there is no gap to aim at, and the reference has to live in `i128`.

## Why variance is not `Σx² − n·x̄²`

Because that formula is unusable on ordinary data. Take values near a billion that vary by one — timestamps, prices in
cents, sequence numbers, sensor readings. Both terms are around 10¹⁸, `f64` carries about sixteen significant digits,
and the answer lives *below the last one*. It returns nonsense, and often a **negative** variance, which makes the
standard deviation `NaN`.

So this keeps Welford's running form, and the *parallel* version of it for combining partials, which is not optional
when there is one accumulator per query thread. `the_naive_variance_formula_would_have_failed_here` computes both on
the same data and asserts the naive one is visibly broken — if that test ever starts passing for the wrong reason, it
says so in the failure message.

Writing that test also caught **me** rather than the code: the first version asserted the variance of 1000 values
cycling +0/+1/+2 was 2/3, which is the value for an even three-way split. `i % 3` over `0..1000` gives 334/333/333, so
the true answer is 666.999/999 — and Welford had it right to nine digits while the test was wrong.

## The percentile guarantee, precisely

Values are bucketed geometrically: `γ = (1+ε)/(1−ε)`, and `v > 0` lands in bucket `⌈log_γ v⌉`. Every value in a bucket
is then within ε of its representative, so the guarantee falls out of the *indexing* rather than out of an analysis:

> **Every percentile returned is within ε of the true value, relatively.**

Relative rather than absolute is the right guarantee for what percentiles get used for — latency, size and price all
span orders of magnitude, where an absolute tolerance is either uselessly loose at the top or absurdly tight at the
bottom. This is the scheme [DDSketch](https://arxiv.org/abs/1908.10693) describes. Negatives get a mirrored sketch,
zero is counted separately, and merging is adding counts per index, so it is associative and commutative.

It is **sparse**: ε = 1% covers a 10⁹ dynamic range in about 2,100 buckets and real data touches far fewer. There is
still a cap, and past it a new bucket folds into its neighbour and the event is counted in `percentile_collapsed` —
bounded means bounded, and the caller can see when the guarantee stopped strictly holding.

### An approximate percentile is approximate even when the answer is obvious

Over a bin that is `200` in 70% of records, the median comes back as **198.4**, not 200. That is the contract working:
the sketch returns a bucket representative, and 198.4 is within 0.82% of 200. A test of mine demanded exact equality
and failed, which looked like a bug and was the test contradicting the documented behaviour. **If you want the exact
answer for a low-cardinality bin, `mode` is the statistic that gives it.**

## The mode, and when to believe it

Space-Saving with capacity *m* over *n* observations:

> **Any value occurring more than n/m times is certain to be in the table**, and every reported count is an
> over-estimate by at most `mode_count_error`, so the truth lies in `[count − error, count]`.

Two things make that usable rather than merely true. If nothing was ever evicted, the table saw every distinct value
and **every count is exact** — which is the common case for the low-cardinality fields anybody actually asks the mode
of, and `mode_is_certain` reports it. And `distinct_values_tracked` says how many distinct values were *offered*, so a
caller can see that mode is the wrong question for their data.

Because it often is. **The mode of a million distinct measurements is a category error**, not a hard problem: every
value occurs once, so "most frequent" is whatever the tie-break picked. Mode belongs to categorical and integral data.
For continuous data the meaningful answers are the modal bucket and the top-k.

Ties break toward the smaller value, so the answer is a function of the data rather than of the order records arrived
in — the same reason the vector example breaks distance ties by digest.

## Distinct count, and why it is implemented here

Aerospike has an HLL particle type, but its operations are client-side `operate()` commands — **no UDF language can
reach them**. Counting distinct values *while* aggregating therefore needs the sketch inside the plugin, and it is a
hundred lines. That is itself an argument for the native ABI: the cost of adding a sketch is low enough to be
reasonable.

It is **not** interchangeable with an Aerospike HLL bin. The register layout is this module's own, so a sketch built
here cannot be unioned with one built by `operate()`.

## No dependencies

The plugin is **524 KB**, against 2.2 MB for the JSONPath example and 0.5 MB for the vector one. Everything here —
moments, quantile sketch, Space-Saving, HyperLogLog — is a few hundred lines of arithmetic, so a plugin computing
twelve statistics over a billion records has exactly one dependency: the SDK.

## Nothing is silently dropped

Integer and double bins both, and the result reports `read_int` and `read_double` — so a bin that is an integer in
most records and a double in a few is *visible*. The **type reaches the sum**: an integer bin is summed as an `i64` into
an `i128` and never widened. Records where the bin is absent, non-numeric or
not finite are counted in `skipped_absent`, `skipped_wrong_type` and `skipped_not_finite`.

Statistics the data could not support are **absent from the map rather than zero**. The standard deviation of one
record and a standard deviation of zero are different facts, and a result that cannot tell them apart makes the caller
guess.

## Also here

`top_values(bin, k)` — a record function rather than an aggregation, for a bin holding a list of numbers: the k most
frequent values *within one record*. One bulk host call reads the list, for the same reason the vector example reads a
packed blob.

## Running it

Configure the module, then:

```sh
cargo test                                          # the arithmetic, no server needed
AEROSPIKE_HOSTS=… cargo test --test against_a_server
```

The server tests assert against closed forms — the sum of 1..n, the variance of 1..n, the AM-GM-HM ordering — rather
than against a second implementation of the same formula, which would only prove the two agree.

**A connection failure with `AEROSPIKE_HOSTS` set is a test failure, not a skip.** Elsewhere in this project a suite
reported "11 passed" in 0.07 seconds because every test quietly returned early when the client could not connect,
which hid a real problem for a while.
