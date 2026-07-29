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

//! Shared per-run configuration handed to every worker task: fully wired
//! policies, bin/value generation, partition filtering and UDF settings.

use std::collections::HashSet;
use std::sync::Arc;

use aerospike::{
    BatchPolicy, BatchReadPolicy, BatchWritePolicy, Bin, Expiration, Key, ReadPolicy,
    ReadTouchTTL, RecordExistsAction, Value, WritePolicy,
};
use rand::rngs::StdRng;

use crate::{cli::Options, db_object_spec::DBObjectSpec, workers::Workload};

/// UDF invocation replacing reads in the RU/RR workloads
/// (`--udf-package`/`--udf-function`/`--udf-values`).
#[derive(Debug, Clone)]
pub struct UdfSpec {
    pub package: String,
    pub function: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct Args {
    pub n_bins: usize,
    pub bin_name_base: String,
    /// Precomputed bin names ("base_1", "base_2", ...) to avoid format! in build_bins hot path.
    pub bin_names: Vec<String>,
    pub object_specs: Vec<DBObjectSpec>,
    pub batch_size: usize,
    pub batch_read_policy: BatchReadPolicy,
    pub batch_write_policy: BatchWritePolicy,
    pub batch_policy: BatchPolicy,
    pub write_policy: WritePolicy,
    pub read_policy: ReadPolicy,
    /// Bins generated once at startup and reused for every write (Java
    /// benchmark default); `-R/--random` disables this and generates fresh
    /// values per write.
    pub fixed_bins: Option<Arc<Vec<Bin>>>,
    pub udf: Option<UdfSpec>,
    /// Single-record ops only touch keys in these partitions; other keys
    /// are counted as successes without a server call (Java parity).
    pub partition_ids: Option<HashSet<u16>>,
    /// Batch reads round-robin key namespaces over this list.
    pub batch_namespaces: Vec<Arc<str>>,
    /// Records per multi-record transaction (`--mrt-size`): wraps groups
    /// of operations in server transactions for the I/RU/RR workloads.
    pub mrt_size: Option<usize>,
}

impl Args {
    /// Wire all policies and value-generation settings from the CLI options.
    #[allow(clippy::too_many_lines)]
    pub fn from_options(opts: &Options) -> Args {
        let n_bins = opts.bins;
        let batch_size = match opts.workload {
            Workload::Initialize | Workload::ReadModUpdate => 1,
            _ => opts.batch_size,
        };

        let mut read_policy = ReadPolicy::default();
        let mut write_policy = WritePolicy::default();
        let mut batch_policy = BatchPolicy::default();
        let batch_read_policy = BatchReadPolicy::default();
        let mut batch_write_policy = BatchWritePolicy::default();

        // ---- replica & read modes ----
        read_policy.replica = opts.replica;
        batch_policy.replica = opts.replica;
        read_policy.base_policy.read_mode_ap = opts.read_mode_ap;
        read_policy.base_policy.read_mode_sc = opts.read_mode_sc;
        batch_policy.base_policy.read_mode_ap = opts.read_mode_ap;
        batch_policy.base_policy.read_mode_sc = opts.read_mode_sc;

        // ---- write options ----
        write_policy.commit_level = opts.commit_level.clone();
        write_policy.send_key = opts.send_key;
        write_policy.expiration = match opts.expiration {
            e if e < 0 => Expiration::Never,
            0 => Expiration::NamespaceDefault,
            e => Expiration::Seconds(e as u32),
        };
        if matches!(opts.workload, Workload::ReadReplace { .. }) {
            write_policy.record_exists_action = RecordExistsAction::Replace;
            batch_write_policy.record_exists_action = RecordExistsAction::Replace;
        }

        // ---- read-touch TTL ----
        if let Some(pct) = opts.read_touch_ttl_percent {
            read_policy.base_policy.read_touch_ttl = ReadTouchTTL::Percent(pct);
            batch_policy.base_policy.read_touch_ttl = ReadTouchTTL::Percent(pct);
        }

        // ---- timeouts & retries ----
        // -T/--timeout sets both socket and total everywhere; the specific
        // options override afterwards (Java behavior).
        let bases: [&mut aerospike::policy::BasePolicy; 3] = [
            &mut read_policy.base_policy,
            &mut write_policy.base_policy,
            &mut batch_policy.base_policy,
        ];
        for base in bases {
            if let Some(t) = opts.timeout {
                base.socket_timeout = t;
                base.total_timeout = t;
            }
            if let Some(t) = opts.socket_timeout {
                base.socket_timeout = t;
            }
            if let Some(t) = opts.total_timeout {
                base.total_timeout = t;
            }
            if let Some(t) = opts.timeout_delay {
                base.timeout_delay = t;
            }
            if let Some(r) = opts.max_retries {
                base.max_retries = r;
            }
            if let Some(s) = opts.sleep_between_retries {
                base.sleep_between_retries = s;
            }
        }
        if let Some(t) = opts.read_socket_timeout {
            read_policy.base_policy.socket_timeout = t;
            batch_policy.base_policy.socket_timeout = t;
        }
        if let Some(t) = opts.read_total_timeout {
            read_policy.base_policy.total_timeout = t;
            batch_policy.base_policy.total_timeout = t;
        }
        if let Some(t) = opts.write_socket_timeout {
            write_policy.base_policy.socket_timeout = t;
        }
        if let Some(t) = opts.write_total_timeout {
            write_policy.base_policy.total_timeout = t;
        }

        let bin_names: Vec<String> = (0..n_bins)
            .map(|i| format!("{}_{}", opts.bin_name_base, i + 1))
            .collect();

        let udf = opts.udf.as_ref().map(|(package, function, values)| UdfSpec {
            package: package.clone(),
            function: function.clone(),
            values: values.iter().map(|v| Value::from(v.as_str())).collect(),
        });

        let mut args = Args {
            n_bins,
            bin_name_base: opts.bin_name_base.clone(),
            bin_names,
            object_specs: opts.object_specs.clone(),
            batch_size,
            batch_policy,
            batch_read_policy,
            batch_write_policy,
            write_policy,
            read_policy,
            fixed_bins: None,
            udf,
            partition_ids: opts.partition_ids.clone(),
            batch_namespaces: opts
                .batch_namespaces
                .iter()
                .map(|ns| Arc::from(ns.as_str()))
                .collect(),
            mrt_size: opts.mrt_size,
        };

        if !opts.random_values {
            // Generate the write payload once and reuse it for every write.
            use rand::SeedableRng;
            let mut rng = StdRng::from_rng(&mut rand::rng());
            let mut bins = Vec::new();
            args.generate_bins(&mut rng, None, args.n_bins, &mut bins);
            args.fixed_bins = Some(Arc::new(bins));
        }

        args
    }

    /// True when single-record operations on this key should be skipped
    /// because it falls outside `--partition-ids`.
    pub fn skip_key(&self, key: &Key) -> bool {
        match &self.partition_ids {
            None => false,
            Some(ids) => {
                let pid = u16::from_le_bytes([key.digest[0], key.digest[1]]) & 0x0FFF;
                !ids.contains(&pid)
            }
        }
    }

    pub fn build_bins(
        &self,
        key: &Key,
        rng: &mut StdRng,
        bin_opted: Option<usize>,
        out: &mut Vec<Bin>,
    ) {
        let num_bins = bin_opted.unwrap_or(self.n_bins);
        if let Some(fixed) = &self.fixed_bins {
            out.clear();
            out.extend(fixed.iter().take(num_bins).cloned());
            return;
        }
        let seed = match key.user_key.as_ref() {
            Some(Value::Int(k)) => Some(*k),
            _ => None,
        };
        self.generate_bins(rng, seed, num_bins, out);
    }

    fn generate_bins(
        &self,
        rng: &mut StdRng,
        seed: Option<i64>,
        num_bins: usize,
        out: &mut Vec<Bin>,
    ) {
        out.clear();
        out.reserve(num_bins);
        let n_specs = self.object_specs.len();
        for i in 0..num_bins {
            let spec = &self.object_specs[i % n_specs];
            let value = if i == 0 {
                spec.gen_value(rng, seed)
            } else {
                spec.gen_value(rng, None)
            };
            let name = self.bin_names[i].clone();
            out.push(Bin::new(name, value));
        }
    }
}
