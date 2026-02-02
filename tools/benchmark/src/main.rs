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

#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate num_cpus;
extern crate rand;

mod cli;
mod generator;
mod percent;
mod stats;
mod workers;

use std::sync::mpsc;
use std::sync::Arc;

use aerospike::{Client, ClientPolicy, Result as AerospikeResult};

use cli::Options;
use generator::KeyPartitions;
use stats::Collector;
use workers::Worker;

#[tokio::main]
async fn main() {
    let _ = env_logger::try_init();
    let options = match cli::parse_options() {
        Ok(options) => options,
        Err(err) => {
            eprintln!("Invalid benchmark configuration: {err}");
            std::process::exit(2);
        }
    };

    match connect(&options).await {
        Ok(client) => run_workload(client, options).await,
        Err(err) => {
            eprintln!("Failed to connect to Aerospike cluster (hosts: {}).", options.hosts);
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

async fn connect(options: &Options) -> AerospikeResult<Client> {
    let mut policy = ClientPolicy::default();
    policy.conn_pools_per_node = options.conn_pools_per_node;
    policy.use_services_alternate = options.use_services_alternate;
    policy.ip_map = options.ip_map.clone();
    Client::new(&policy, &options.hosts).await
}


async fn run_workload(client: Client, opts: Options) {
    let client = Arc::new(client);
    let (send, recv) =  mpsc::channel();
    let collector = Collector::new(recv);

    let collector_handle = tokio::task::spawn_blocking(move || {
        collector.collect(); 
    });
    let mut worker_handles = Vec::new();

    for keys in KeyPartitions::new(
        opts.namespace,
        opts.set,
        opts.start_key,
        opts.keys,
        opts.concurrency,
    ) {
        let mut worker = Worker::for_workload(&opts.workload, client.clone(), send.clone());
        
        let handle = tokio::spawn(async move { 
            worker.run(keys).await 
        });
        worker_handles.push(handle);
    }
    drop(send); 
    for handle in worker_handles {
        let _ = handle.await;
    }
    let _ = collector_handle.await;
    
}
