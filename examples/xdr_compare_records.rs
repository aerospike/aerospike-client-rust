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

//! Compares one set across two clusters that ship to each other over XDR.
//!
//! Scans the set on both clusters and reports every record that is on one
//! side only, and every record whose bins differ by value, with its digest
//! and, when the record stores it, its user key. It works for any set. With
//! an `xdr-merge` handler bound, it finds the records a failed merge or a
//! handler that breaks the contract left apart, so that they can be repaired
//! (see "When the clusters differ" in the event chapter of the UDF book).
//!
//! The comparison itself is in `xdr/compare.rs`, which
//! `xdr_crdt_two_clusters` uses too. A record written while the scans run
//! can show as different: compare when the set is quiet, or compare again.
//!
//! Settings, from the environment:
//!
//! - `AEROSPIKE_XDR_DC1`, `AEROSPIKE_XDR_DC2`: the two clusters, `host:port`
//!   each. Without them the example skips.
//! - `AEROSPIKE_XDR_NAMESPACE` (default `test`) and `AEROSPIKE_XDR_SET`
//!   (default `subscribers`): what to compare.
//! - `AEROSPIKE_USE_SERVICES_ALTERNATE`, as for every example.
//!
//! The exit status is 0 when the two clusters hold the same records, 1 when
//! they do not.
//!
//! ```bash
//! AEROSPIKE_XDR_DC1=127.0.0.1:3400 AEROSPIKE_XDR_DC2=127.0.0.1:3500 \
//!   AEROSPIKE_XDR_SET=subscribers cargo run --example xdr_compare_records
//! ```

use std::env;

use aerospike::{Error, Result};

#[path = "xdr/cluster.rs"]
mod cluster;
#[path = "xdr/compare.rs"]
mod compare;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`). An error says the
/// clusters differ, or that one could not be read.
pub async fn run() -> Result<()> {
    let Some((dc1, dc2)) = cluster::two_clusters() else {
        println!("skipped: set AEROSPIKE_XDR_DC1 and AEROSPIKE_XDR_DC2 to the two clusters (host:port each)");
        return Ok(());
    };
    let namespace = cluster::namespace();
    let set = env::var("AEROSPIKE_XDR_SET").unwrap_or_else(|_| String::from("subscribers"));

    let c1 = cluster::connect(&dc1).await?;
    let c2 = cluster::connect(&dc2).await?;
    let comparison = compare::compare_set(&c1, &c2, &namespace, &set).await;

    c1.close().await?;
    c2.close().await?;

    let comparison = comparison?;

    print!("{}", comparison.report());

    if comparison.matches() {
        Ok(())
    } else {
        Err(Error::client_error(format!(
            "{} records of {namespace}.{set} differ between {dc1} and {dc2}",
            comparison.mismatches.len()
        )))
    }
}
