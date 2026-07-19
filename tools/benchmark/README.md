# Benchmark Tool

The benchmark tool is intended to generate artificial yet customizable load on your
database cluster to help you tweak your connection properties. Its feature set and
option names mirror the Java client's `benchmarks` application; options tied to
the JVM (virtual threads, Netty event loops, sync/async toggles) have no
counterpart here — this client is async-native, so concurrency is controlled
with `--tasks` and `--cores`.

## Usage

To see available switches:

    cargo run -- --help

The benchmark should be run in release mode with optimizations enabled:

    cargo run --release -- <benchmark options>

## Workloads (`-w`)

| Workload | Behavior |
| --- | --- |
| `I` | Insert: linear write of `--keys` records split across tasks |
| `RU[,rd%[,rdAll%[,wrAll%]]]` | Read/Update mix over random keys (default 100% reads) |
| `RR[,...]` | Read/Replace: like RU, but writes use the Replace record-exists action |
| `RMU` | Read all bins, then update one bin |
| `RMI` / `RMD` | Read all bins, then increment / decrement a counter bin (generation-checked) |
| `TXN,r:<n>,w:<m>[,v:<var>]` | Business transaction: n reads + m writes per transaction, counts randomized ± `v` (absolute or `20%`) |
| `TXN,t:<pattern>` | Fixed op pattern, e.g. `rrRu2b10i`: `r`/`R` read one/all bins, `u`/`U` update one/all, `p`/`P` replace, `i` increment, `b<n>` batch read of n keys, `w`≡`u` |
| `-F <file>` | Read-from-file: random reads of the keys listed in the file (`-K S\|I` for string/integer keys) |

`--mrt-size <n>` wraps groups of `n` operations of the I/RU/RR workloads in
multi-record transactions (commit on success, abort on failure; requires a
strong-consistency namespace).

## Key options

* Connection: `-h/--hosts`, `-U/--user`, `-P/--password`, `--cluster-name`,
  `--min/max-conns-per-node`, `-Y/--conn-pools-per-node`, `--tend-interval`,
  `--max-error-rate`, `--error-rate-window`, `--rack-id`,
  `--use-services-alternate`, `--ip-map`
* Working set: `-n/--namespace`, `-s/--set`, `-k/--keys`, `-S/--startkey`,
  `-b/--bins`, `-p/--bin-prefix`, `-o/--object-spec` (`I | D | B:<size> |
  S:<size> | R:<bytes>:<randPct>`, comma-separated per bin)
* Values: fixed by default (generated once at startup, Java parity);
  `-R/--random` generates fresh values per write; `-e/--expiration`,
  `--send-key`, `--read-touch-ttl-percent`
* Policies: `-r/--replica` (`master|master-proles|sequence|prefer-rack|random`),
  `--read-mode-ap`, `--read-mode-sc`, `--commit-level`, `-T/--timeout`,
  `--socket-timeout`, `--read/write-socket-timeout`, `--total-timeout`,
  `--read/write-total-timeout`, `--timeout-delay`, `--max-retries`,
  `--sleep-between-retries`
* Load shape: `-t/--tasks`, `-c/--cores`, `-B/--batch-size` (RU/RR),
  `--batch-namespaces` (round-robin batch reads), `-g/--throughput`
  (aggregate target TPS), `--transactions` (stop after N ops),
  `-d/--duration` (seconds), `--partition-ids` (restrict single-record ops)
* UDF reads: `--udf-package`, `--udf-function`, `--udf-values` make RU/RR
  reads execute the UDF instead of a get
* Reporting: `--report-style pretty|asbench`, `-N/--report-not-found`,
  `-l/--latency` (`ycsb[,<warmup>]` for avg/min/max + p95/p99, or
  `[alt,]<columns>,<shift>[,us|ms]` for Aerospike-style bucket tables),
  `-D/--debug`

## Examples

Write 10,000,000 keys to the database:

    $ cargo run --release -- -k 10000000 -w I

50% reads / 50% updates for 30 seconds, throttled to 10k TPS, with an
Aerospike-style latency table (7 columns, power-of-two):

    $ cargo run --release -- -k 10000000 -w RU,50 -d 30 -g 10000 -l 7,1

Business transactions of ~10 reads and ~2 writes (±20%), stopping after
100,000 operations:

    $ cargo run --release -- -w 'TXN,r:10,w:2,v:20%' --transactions 100000

Multi-record transactions of 4 operations each (strong-consistency
namespace required):

    $ cargo run --release -- -n sc_ns -w RU,80 --mrt-size 4 -d 30
