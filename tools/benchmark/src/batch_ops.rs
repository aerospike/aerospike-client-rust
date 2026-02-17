// Copyright 2015-2018 Aerospike, Inc.
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

use rand::rngs::StdRng;

use aerospike::operations;
use aerospike::{BatchOperation, BatchReadPolicy, Bin, Bins, Key};

use crate::args::Args;

pub(crate) fn build_batch_read_ops(
    keys: &[Key],
    brpolicy: &BatchReadPolicy,
    bins: Bins,
    out: &mut Vec<BatchOperation>,
) {
    out.clear();
    out.reserve(keys.len());
    for k in keys {
        out.push(BatchOperation::read(brpolicy, k.clone(), bins.clone()));
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
        out.push(BatchOperation::write(&args.batch_write_policy, k.clone(), wops));
    }
}
