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

use std::sync::Arc;
use std::thread;

use aerospike::{Client, ClientPolicy};

use cli::Options;
use stats::Collector;
use workers::Worker;

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

fn run_workload(client: Client, options: Options) {
    let workload = &options.workload;
    let client = Arc::new(client);
    let collector = Collector::new();
    let keys_per_task = options.keys / options.concurrency;
    let remainder = options.keys % options.concurrency;
    let mut start_key = options.start_key;
    let mut threads = vec![];
    for i in 0..options.concurrency {
        let mut keys = keys_per_task;
        if i < remainder {
            keys += 1
        }
        let mut worker = Worker::for_workload(workload, client.clone(), collector.sender(), &options);
        let key_range = start_key..(start_key + keys);
        let t = thread::spawn(move || worker.run(key_range));
        start_key += keys;
        threads.push(t);
    }

    let histogram = collector.collect();
    println!("{:?}", histogram);
}
