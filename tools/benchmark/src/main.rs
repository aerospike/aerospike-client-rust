#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;
#[macro_use]
extern crate lazy_static;
extern crate num_cpus;

mod cli;
mod workers;
mod stats;
mod generator;

use std::sync::Arc;
use std::thread;
use std::sync::mpsc;

use aerospike::{Client, ClientPolicy};

use cli::Options;
use stats::Collector;
use workers::Worker;
use generator::KeyPartitions;

fn main() {
    env_logger::init().unwrap();
    let options = cli::parse_options();
    info!("{:?}", options);
    let client = connect(&options);
    run_workload(client, options);
}

fn connect(options: &Options) -> Client {
    let policy = ClientPolicy::default();
    Client::new(&policy, &options.hosts).unwrap()
}

fn run_workload(client: Client, opts: Options) {
    let client = Arc::new(client);
    let (send, recv) = mpsc::channel();
    let collector = Collector::new(recv);
    for keys in KeyPartitions::new(opts.namespace,
                                   opts.set,
                                   opts.start_key,
                                   opts.keys,
                                   opts.concurrency) {
        let mut worker = Worker::for_workload(&opts.workload, client.clone(), send.clone());
        thread::spawn(move || worker.run(keys));
    }
    drop(send);
    collector.collect();
}
