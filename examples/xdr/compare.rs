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

//! One set compared across two clusters, record by record and by value.
//!
//! Not an example of its own: `xdr_compare_records` and `xdr_crdt_two_clusters`
//! include this file with `#[path = "xdr/compare.rs"]`.
//!
//! Records are matched by digest, so a record is found on both sides whether
//! or not it stores its user key. Bins are compared with `Value`'s equality:
//! a map by its content, whatever order each cluster keeps its keys in; a
//! list, and a map kept in key-value order, in order.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;

use aerospike::query::PartitionFilter;
use aerospike::{Bins, Client, QueryPolicy, Result, Statement, Value};
use futures::StreamExt;

/// A record as a scan found it.
pub struct Found {
    /// The user key, when the record stores it (written with `send_key`).
    pub user_key: Option<Value>,
    pub bins: HashMap<String, Value>,
}

/// How one record differs between the two clusters.
pub enum Difference {
    /// On dc2 only.
    MissingOnDc1,
    /// On dc1 only.
    MissingOnDc2,
    /// On both, with these bins different: the name, then dc1's value and dc2's (None where the record
    /// has no such bin).
    Bins(Vec<(String, Option<Value>, Option<Value>)>),
}

/// A record that is not the same on both clusters.
pub struct Mismatch {
    pub digest: [u8; 20],
    pub user_key: Option<Value>,
    pub difference: Difference,
}

/// The outcome of comparing one set.
pub struct Comparison {
    pub namespace: String,
    pub set: String,
    pub on_dc1: usize,
    pub on_dc2: usize,
    pub mismatches: Vec<Mismatch>,
}

impl Comparison {
    /// Both clusters hold the same records, by value.
    pub fn matches(&self) -> bool {
        self.mismatches.is_empty()
    }

    /// A line per record that differs, then a summary line.
    pub fn report(&self) -> String {
        let mut out = String::new();

        for m in &self.mismatches {
            let key = m.user_key.as_ref().map_or_else(String::new, |k| format!(" key {k}"));
            let digest: String = m.digest.iter().map(|b| format!("{b:02x}")).collect();

            match &m.difference {
                Difference::MissingOnDc1 => {
                    let _ = writeln!(out, "  digest {digest}{key}: only on dc2");
                }
                Difference::MissingOnDc2 => {
                    let _ = writeln!(out, "  digest {digest}{key}: only on dc1");
                }
                Difference::Bins(bins) => {
                    let _ = writeln!(out, "  digest {digest}{key}: bins differ");

                    for (name, dc1, dc2) in bins {
                        let shown = |v: &Option<Value>| v.as_ref().map_or_else(|| String::from("(no bin)"), |v| format!("{v}"));
                        let _ = writeln!(out, "    {name}: dc1 {}, dc2 {}", shown(dc1), shown(dc2));
                    }
                }
            }
        }

        let _ = writeln!(
            out,
            "{}.{}: {} records on dc1, {} on dc2, {} differ",
            self.namespace,
            self.set,
            self.on_dc1,
            self.on_dc2,
            self.mismatches.len()
        );
        out
    }
}

/// Every record of the set on one cluster, by digest.
pub async fn scan(client: &Client, namespace: &str, set: &str) -> Result<BTreeMap<[u8; 20], Found>> {
    // A scan is a query without a filter, over every partition.
    let statement = Statement::new(namespace, set, Bins::All);
    let records = client.query(&QueryPolicy::default(), PartitionFilter::all(), statement).await?;
    let mut stream = records.into_stream();
    let mut found = BTreeMap::new();

    while let Some(record) = stream.next().await {
        let record = record?;

        // A scan always gives the record's key: its digest, and the user key when one is stored.
        if let Some(key) = record.key {
            let bins = record.bins.into_iter().collect();

            found.insert(key.digest(), Found { user_key: key.user_key().cloned(), bins });
        }
    }

    Ok(found)
}

/// The bins that differ between two copies of a record, by name.
fn differing_bins(dc1: &HashMap<String, Value>, dc2: &HashMap<String, Value>) -> Vec<(String, Option<Value>, Option<Value>)> {
    let names: BTreeSet<&String> = dc1.keys().chain(dc2.keys()).collect();

    names
        .into_iter()
        .filter(|name| dc1.get(*name) != dc2.get(*name))
        .map(|name| (name.clone(), dc1.get(name).cloned(), dc2.get(name).cloned()))
        .collect()
}

/// Scans the set on both clusters and compares every record. A record written while the scans run can
/// show as different; compare a set when nothing writes to it, or compare it again.
pub async fn compare_set(dc1: &Client, dc2: &Client, namespace: &str, set: &str) -> Result<Comparison> {
    let (on_dc1, on_dc2) = futures::try_join!(scan(dc1, namespace, set), scan(dc2, namespace, set))?;
    let mut mismatches = Vec::new();

    for (digest, a) in &on_dc1 {
        let difference = match on_dc2.get(digest) {
            None => Difference::MissingOnDc2,
            Some(b) => {
                let bins = differing_bins(&a.bins, &b.bins);

                if bins.is_empty() {
                    continue;
                }
                Difference::Bins(bins)
            }
        };

        mismatches.push(Mismatch { digest: *digest, user_key: a.user_key.clone(), difference });
    }

    for (digest, b) in on_dc2.iter().filter(|(digest, _)| !on_dc1.contains_key(*digest)) {
        mismatches.push(Mismatch { digest: *digest, user_key: b.user_key.clone(), difference: Difference::MissingOnDc1 });
    }

    Ok(Comparison {
        namespace: namespace.to_string(),
        set: set.to_string(),
        on_dc1: on_dc1.len(),
        on_dc2: on_dc2.len(),
        mismatches,
    })
}
