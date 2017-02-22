#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;

mod cli;
mod workers;
mod stats;
mod generator;

use std::sync::Arc;
use std::thread;

use aerospike::{Client, ClientPolicy};

use cli::Options;
use stats::Collector;
use workers::Worker;
use generator::KeyPartitions;

fn main() {
    env_logger::init().unwrap();
    let options = cli::parse_options();
    debug!("Command line options: {:?}", options);
    let client = connect(&options);
    run_workload(client, options);
}

fn connect(options: &Options) -> Client {
    let policy = ClientPolicy::default();
    Client::new(&policy, &options.hosts).unwrap()
}

fn run_workload(client: Client, opts: Options) {
    let client = Arc::new(client);
    let collector = Collector::new();
    for keys in KeyPartitions::new(opts.namespace,
                                   opts.set,
                                   opts.start_key,
                                   opts.keys,
                                   opts.concurrency) {
        let mut worker = Worker::for_workload(&opts.workload, client.clone(), &collector);
        thread::spawn(move || worker.run(keys));
    }
    let histogram = collector.collect();
    println!("{:?}", histogram);
}
