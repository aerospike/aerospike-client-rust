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

#[macro_use]
extern crate aerospike;
extern crate env_logger;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate rand;

mod args;
mod batch_ops;
mod cli;
mod db_object_spec;
mod generator;
mod percent;
mod stats;
mod tasks;
mod throttle;
mod workers;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use aerospike::{AuthMode, Client, ClientPolicy, Key, Result as AerospikeResult};

use crate::args::Args;
use crate::cli::Options;
use crate::generator::{KeyPartitions, KeyRangeGen, RandomKeyRange};
use crate::stats::Collector;
use crate::throttle::RunControl;
use crate::workers::{Worker, WorkerConfig, Workload};

fn main() {
    let options = match cli::parse_options() {
        Ok(options) => options,
        Err(err) => {
            eprintln!("Invalid benchmark configuration: {err}");
            std::process::exit(2);
        }
    };
    let cores = options.cores as usize;

    if options.debug {
        // Debug mode: verbose logging unless the user configured RUST_LOG.
        if std::env::var("RUST_LOG").is_err() {
            std::env::set_var("RUST_LOG", "debug");
        }
    }

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cores.max(1))
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            benchmark(options).await;
        })
}

async fn benchmark(options: Options) {
    let _ = env_logger::try_init();
    print_banner(&options);
    match connect(&options).await {
        Ok(client) => run_workload(client, options).await,
        Err(err) => {
            eprintln!(
                "Failed to connect to Aerospike cluster (hosts: {}).",
                options.hosts
            );
            eprintln!("Error: {err}");
            eprintln!();
            eprintln!(
                "Hint: if the server advertises an internal/unroutable IP (common with Docker Desktop), \
                 try `--ip-map '<advertised_ip>=127.0.0.1'` or configure Aerospike `service-alternate` \
                 and run with `--use-services-alternate`."
            );
            std::process::exit(1);
        }
    }
}

fn print_banner(o: &Options) {
    println!("Benchmark configuration:");
    println!("  hosts:      {}", o.hosts);
    println!(
        "  namespace:  {}   set: {}   workload: {:?}",
        o.namespace, o.set, o.workload
    );
    println!(
        "  keys: {} (start {}),  bins: {},  tasks: {},  cores: {}",
        o.keys, o.start_key, o.bins, o.tasks, o.cores
    );
    let mut extras = Vec::new();
    if o.batch_size > 1 {
        extras.push(format!("batch={}", o.batch_size));
    }
    if let Some(mrt) = o.mrt_size {
        extras.push(format!("mrt-size={mrt}"));
    }
    if o.throughput > 0 {
        extras.push(format!("target-tps={}", o.throughput));
    }
    if o.transactions > 0 {
        extras.push(format!("transactions={}", o.transactions));
    }
    if let Some(d) = o.duration_secs {
        extras.push(format!("duration={d}s"));
    }
    if o.udf.is_some() {
        extras.push("udf-reads".to_string());
    }
    if let Some(pids) = &o.partition_ids {
        extras.push(format!("partitions={}", pids.len()));
    }
    if !o.random_values {
        extras.push("fixed-values".to_string());
    }
    if !extras.is_empty() {
        println!("  {}", extras.join(",  "));
    }
    println!(
        "  replica: {:?},  readModeAP: {:?},  readModeSC: {:?},  commitLevel: {:?}",
        o.replica, o.read_mode_ap, o.read_mode_sc, o.commit_level
    );
    println!();
}

async fn connect(options: &Options) -> AerospikeResult<Client> {
    let mut policy = ClientPolicy::default();
    if let Ok(min_conns) = std::env::var("AEROSPIKE_BENCH_MIN_CONNS_PER_NODE") {
        policy.min_conns_per_node = min_conns
            .parse()
            .expect("AEROSPIKE_BENCH_MIN_CONNS_PER_NODE must be a non-negative integer");
    }
    if let Some(user) = &options.user {
        policy
            .set_auth_mode(AuthMode::Internal(
                user.clone(),
                options.password.clone().unwrap_or_default(),
            ))
            .expect("failed to configure authentication");
    }
    policy.cluster_name = options.cluster_name.clone();
    policy.conn_pools_per_node = options.conn_pools_per_node;
    if options.min_conns_per_node > 0 {
        policy.min_conns_per_node = options.min_conns_per_node;
    }
    if let Some(max) = options.max_conns_per_node {
        policy.max_conns_per_node = max;
    }
    if let Some(rate) = options.max_error_rate {
        policy.max_error_rate = rate;
    }
    if let Some(window) = options.error_rate_window {
        policy.error_rate_window = window;
    }
    if let Some(interval) = options.tend_interval {
        policy.tend_interval = interval.max(aerospike::policy::TEND_INTERVAL_MIN_MS);
    }
    if let Some(rack) = options.rack_id {
        policy.rack_ids = Some(HashSet::from([rack]));
    }
    policy.use_services_alternate = options.use_services_alternate;
    policy.ip_map = options.ip_map.clone();
    Client::new(&policy, &options.hosts).await
}

/// Load keys for the read-from-file workload: one key per line, string or
/// integer per `--key-type`.
fn load_key_file(options: &Options) -> Result<Arc<Vec<Key>>, String> {
    let path = options
        .key_file
        .as_ref()
        .expect("read-from-file workload requires --key-file");
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read key file {}: {e}", path.display()))?;
    let mut keys = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let key = if options.string_keys {
            as_key!(options.namespace.as_str(), options.set.as_str(), line)
        } else {
            let v: i64 = line
                .parse()
                .map_err(|e| format!("invalid integer key `{line}` in key file: {e}"))?;
            as_key!(options.namespace.as_str(), options.set.as_str(), v)
        };
        keys.push(key);
    }
    if keys.is_empty() {
        return Err(format!("key file {} contains no keys", path.display()));
    }
    Ok(Arc::new(keys))
}

#[allow(clippy::too_many_lines)]
async fn run_workload(client: Client, opts: Options) {
    let client = Arc::new(client);
    let (send, recv) = mpsc::channel(opts.tasks as usize);
    let collector = Collector::new(
        recv,
        opts.report_style,
        opts.latency_mode,
        opts.report_not_found,
    );

    let collector_handle = tokio::spawn(async move {
        collector.collect().await;
    });
    let mut worker_handles = Vec::new();

    let args = Arc::new(Args::from_options(&opts));
    let control = RunControl::new(opts.throughput, opts.transactions);

    let namespace_ref: Arc<str> = Arc::from(opts.namespace.as_str());
    let set_ref: Arc<str> = Arc::from(opts.set.as_str());

    let duration_limit = opts.duration_secs.map(Duration::from_secs);
    let tasks = opts.tasks.max(1);
    // Split the YCSB warmup across workers.
    let warmup_per_worker = opts.latency_mode.warmup() / tasks as u64;

    let file_keys = if opts.workload == Workload::ReadFromFile {
        match load_key_file(&opts) {
            Ok(keys) => Some(keys),
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(2);
            }
        }
    } else {
        None
    };

    // Group size: batch for RU/RR reads+writes, MRT transaction size for
    // MRT-wrapped workloads, otherwise 1.
    let group_size = opts.mrt_size.unwrap_or(args.batch_size).max(1);

    let config = WorkerConfig {
        workload: opts.workload.clone(),
        args: args.clone(),
        control: control.clone(),
        latency_mode: opts.latency_mode,
        warmup: warmup_per_worker,
        namespace: namespace_ref.clone(),
        set: set_ref.clone(),
        start_key: opts.start_key,
        key_count: opts.keys,
        file_keys,
        group_size,
    };

    if opts.workload == Workload::Initialize {
        for keys in KeyPartitions::new(
            namespace_ref.clone(),
            set_ref.clone(),
            opts.start_key,
            opts.keys,
            tasks,
        ) {
            let mut worker = Worker::for_workload(client.clone(), send.clone(), &config);
            let handle = tokio::spawn(async move {
                worker.run(keys, None).await;
            });
            worker_handles.push(handle);
        }
    } else {
        for _ in 0..tasks {
            let mut worker = Worker::for_workload(client.clone(), send.clone(), &config);
            let key_range = KeyRangeGen::Random(RandomKeyRange::new(
                Arc::clone(&namespace_ref),
                Arc::clone(&set_ref),
                opts.start_key,
                opts.keys,
                false,
            ));

            let handle = tokio::spawn(Box::pin(async move {
                worker.run(key_range, duration_limit).await;
            }));
            worker_handles.push(handle);
        }
    }

    drop(send);
    for handle in worker_handles {
        let _ = handle.await;
    }
    let _ = collector_handle.await;

    if control.stopped() && opts.transactions > 0 {
        println!("Transaction limit reached: {}. Exiting.", opts.transactions);
    }
}

#[cfg(test)]
mod tests {
    use crate::db_object_spec::{parse_object_spec_list, DBObjectSpec};

    #[test]
    fn parse_object_spec_list_single() {
        let specs = parse_object_spec_list("I").unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0], DBObjectSpec::default());
    }

    #[test]
    fn parse_object_spec_list_multiple() {
        let specs = parse_object_spec_list("I,S:20,B:30").unwrap();
        assert_eq!(specs.len(), 3);
    }

    #[test]
    fn parse_object_spec_list_empty_err() {
        let result = parse_object_spec_list("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Object spec is empty");
    }

    #[test]
    fn parse_object_spec_list_invalid_err() {
        let result = parse_object_spec_list("I:10");
        assert!(result.is_err());
    }
}
