# Benchmark Tool

The benchmark tool is intended to generate artificial yet customizable load on your
database cluster to help you tweak your connection properties.

## Usage

To see available switches:

    cargo run -- --help

The benchmark should be run in release mode with optimizations enabled:

    cargo run --release -- <benchmark options>

## How it works

By default, load is generated on keys with values in key range (`-k` switch).
Bin data consists of random 64 bit integer values.

## Examples

To write 10,000,000 keys to the database:

    $ cargo run --release -- -k 10000000 -w I

To generate a load consisting 50% reads and 50% updates:

    $ cargo run --release -- -k 10000000 -w RU,50
