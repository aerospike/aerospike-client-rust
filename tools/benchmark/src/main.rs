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
use std::thread;

use aerospike::{Client, ClientPolicy};

use cli::Options;
use generator::KeyPartitions;
use stats::Collector;
use workers::Worker;

fn main() {
    let _ = env_logger::try_init();
    let options = cli::parse_options();
    info!("{:?}", options);
    let client = connect(&options);
    run_workload(client, options);
}

fn connect(options: &Options) -> Client {
    let mut policy = ClientPolicy::default();
    policy.conn_pools_per_node = options.conn_pools_per_node;
    Client::new(&policy, &options.hosts).unwrap()
}

fn run_workload(client: Client, opts: Options) {
    let client = Arc::new(client);
    let (send, recv) = mpsc::channel();
    let collector = Collector::new(recv);
    for keys in KeyPartitions::new(
        opts.namespace,
        opts.set,
        opts.start_key,
        opts.keys,
        opts.concurrency,
    ) {
        let mut worker = Worker::for_workload(&opts.workload, client.clone(), send.clone());
        thread::spawn(move || worker.run(keys));
    }
    drop(send);
    collector.collect();
}
