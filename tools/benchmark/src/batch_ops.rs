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

use std::sync::Arc;

use rand::rngs::StdRng;

use aerospike::operations;
use aerospike::{BatchOperation, BatchReadPolicy, Bin, Bins, Key, Value};

use crate::args::Args;

/// Build batch-read operations for `keys`. When `namespaces` is non-empty
/// the key namespaces round-robin over it (`--batch-namespaces`, Java
/// parity).
pub(crate) fn build_batch_read_ops(
    keys: &[Key],
    brpolicy: &BatchReadPolicy,
    bins: Bins,
    namespaces: &[Arc<str>],
    out: &mut Vec<BatchOperation>,
) {
    out.clear();
    out.reserve(keys.len());
    for (i, k) in keys.iter().enumerate() {
        let key = if namespaces.is_empty() {
            k.clone()
        } else {
            let ns = &namespaces[i % namespaces.len()];
            rekey(k, ns)
        };
        out.push(BatchOperation::read(brpolicy, key, bins.clone()));
    }
}

/// Rebuild a key in another namespace (same set and user key).
fn rekey(key: &Key, namespace: &Arc<str>) -> Key {
    match key.user_key.as_ref() {
        Some(Value::Int(v)) => as_key!(namespace.as_ref(), key.set_name.as_str(), *v),
        Some(Value::String(s)) => as_key!(namespace.as_ref(), key.set_name.as_str(), s.as_str()),
        _ => key.clone(),
    }
}

pub(crate) fn build_batch_write_ops(
    keys: &[Key],
    args: &Args,
    rng: &mut StdRng,
    multi_bins_write: bool,
    out: &mut Vec<BatchOperation>,
    bins_buffer: &mut Vec<Bin>,
) {
    out.clear();
    out.reserve(keys.len());
    for k in keys {
        if multi_bins_write {
            args.build_bins(k, rng, None, bins_buffer);
        } else {
            args.build_bins(k, rng, Some(1), bins_buffer);
        }
        let wops: Vec<_> = if multi_bins_write {
            bins_buffer.iter().map(operations::put).collect()
        } else {
            bins_buffer.iter().take(1).map(operations::put).collect()
        };
        out.push(BatchOperation::write(
            &args.batch_write_policy,
            k.clone(),
            wops,
        ));
    }
}
