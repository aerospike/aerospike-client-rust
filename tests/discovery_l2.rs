// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! The L2 discovery rows: the client against a three node cluster it reaches
//! only through one Toxiproxy listener per node behind one endpoint hostname,
//! with no route to the node addresses. The topology is built by
//! `.github/workflows/discovery-l2.yml` (the `discovery-l2-topology` action of
//! aerospike-client-conformance) or locally by `tests/discovery/run_l2.sh`,
//! and arrives as the `DISCOVERY_*` environment variables `l2::topology`
//! documents. Without `DISCOVERY_ENDPOINT_HOSTNAME` every test skips itself.
//!
//! The rows share Toxiproxy and the cluster, so they run one at a time:
//!
//! ```sh
//! cargo test --test discovery_l2 --no-default-features \
//!   --features async,serialization,rt-tokio,tls -- --test-threads=1 --nocapture
//! ```
//!
//! Row numbers (N-1 ... N-19, F-1 ... F-4) are those of the pluggable
//! discovery testing strategy, as in the Java client's `SuiteDiscovery`.

mod l2;
