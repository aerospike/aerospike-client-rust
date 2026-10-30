# Discovery test tiers

Pluggable seed discovery and address translation are tested at three levels
besides the unit tests in `aerospike-core`:

| Tier | What runs | Where |
| --- | --- | --- |
| Fake nodes | `tests/src/discovery.rs`: the client against fake nodes that speak the info protocol. No server. Part of the `lib` test target. | every CI leg |
| L1 conformance | `tests/conformance.rs`: the shared fixture set of [aerospike-client-conformance](https://github.com/aerospike/aerospike-client-conformance) (translation, merge policy, dns and aws-tags providers, certificate name matching). No server. | `discovery-conformance.yml` |
| L2 topology | `tests/discovery_l2.rs`: the client against a three node cluster reachable only through a Toxiproxy listener per node, in profiles. | `discovery-integration.yml` |
| TLS matching | `tests/tls_matching.rs`: one Enterprise container per certificate shape, driven by `tests/tls/run_tls_matching.sh`. | `discovery-integration.yml` |

Each tier skips itself without its environment, so `cargo test -p aerospike`
stays green on a plain single node.

## L1 conformance

```sh
git clone https://github.com/aerospike/aerospike-client-conformance ../aerospike-client-conformance
CONFORMANCE_DIR=../aerospike-client-conformance \
  cargo test --test conformance --no-default-features \
  --features async,serialization,rt-tokio,tls,discovery-aws -- --nocapture
```

The runner pins the fixture set version (`FIXTURE_SET_VERSION` in
`tests/conformance.rs`) and fails on another one. The dns and aws-tags rows
start the fixture server the set ships (`python3`). Every row's result is
compared with `tests/conformance/rust-declared.yaml`; a row that changes in
either direction needs a reviewed change there. The report lands in
`target/conformance-report.json`.

## L2 topology

The rows are the Java client's `SuiteDiscovery`, numbered by the testing
strategy (N-1 to N-19, F-1 to F-4). The topology arrives as environment
variables; `tests/l2/topology.rs` lists them.

Locally, with Docker or Podman:

```sh
tests/discovery/run_l2.sh                       # default profile, community image
L2_PROFILE=reachable tests/discovery/run_l2.sh  # one profile
CONTAINER_ENGINE=podman tests/discovery/run_l2.sh
DISCOVERY_SUBSET=ee FEATURES_CONF_PATH=/path/features.conf tests/discovery/run_l2.sh
```

The script builds the test binary in a Rust container, starts the cluster and
Toxiproxy the way the CI action does, applies the profile, serves node
start/stop requests for the profiles that scale the cluster out, runs the rows
in a container on the client network and removes everything on exit. The log
is `target/discovery-l2/suite.log`.

| Profile | Rows | Topology |
| --- | --- | --- |
| `default` | N-1, N-2, N-5, N-6 (ee), N-7, N-8, N-11, N-12, N-14, strict misses, F-4 | 3 nodes, node server |
| `reachable` | N-10, alternate list cases | last node advertises a reachable name |
| `pass-through-names` | N-15 | one alternate hostname per node |
| `pass-through-ports` | N-16, N-18 | one hostname, one port per node; node server |
| `port-map-range` | N-19 | fixed subnet, a listener per address of a range; node server |
| `cn-only` | N-17 (ee) | certificate with the tls-name only as CN |
| `low-mtu` | F-2 | client network MTU 1200 |
| `port-range` | F-3 | proxy source ports 40000 to 40009 |
| `idle-reset` | F-1 | default topology |

In CI the cluster comes from the `discovery-l2-topology` action of the
conformance repository; `.github/discovery/run-l2-suite.sh` is the command the
client container runs in both places.

## TLS matching

```sh
AEROSPIKE_IMAGE=aerospike/aerospike-server-enterprise:8.2 \
AEROSPIKE_FEATURES_FILE=/path/features.conf \
  tests/tls/run_tls_matching.sh            # every case
  tests/tls/run_tls_matching.sh dns_exact  # one case
```

The expectations are the Rust client's: rustls matches the tls name against
the certificate's subject alternative names only, so the certificates the
Java client still accepts through its deprecated CN fallback are rejected,
and revocation by serial, which is not a client feature, is reported as
unsupported.
