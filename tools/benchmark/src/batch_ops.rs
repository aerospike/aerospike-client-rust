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
use aerospike::{BatchOperation, BatchReadPolicy, Bins, Key};

use crate::args::Args;

pub(crate) fn build_batch_read_ops(
    keys: &[Key],
    brpolicy: &BatchReadPolicy,
    bins: Bins,
) -> Vec<BatchOperation> {
    keys.iter()
        .map(|k| BatchOperation::read(brpolicy, k.clone(), bins.clone()))
        .collect()
}

pub(crate) fn build_batch_write_ops(
    keys: &[Key],
    args: &Args,
    rng: &mut StdRng,
    multi_bins_write: bool,
) -> Vec<BatchOperation> {
    keys.iter()
        .map(|k| {
            let wops: Vec<_> = if multi_bins_write {
                let bins = args.build_bins(k, rng, None);
                bins.iter().map(operations::put).collect()
            } else {
                let bins = args.build_bins(k, rng, Some(1));
                bins.iter().take(1).map(operations::put).collect()
            };
            BatchOperation::write(&args.batch_write_policy, k.clone(), wops)
        })
        .collect()
}
