#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate env_logger;

mod cli;
mod workload;
mod tasks;

use std::sync::Arc;
use std::thread;

use aerospike::{Client, ClientPolicy};

use cli::Options;
use tasks::InsertTask;

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
    let client = Arc::new(client);
    let keys_per_task = options.keys / options.concurrency;
    let remainder = options.keys % options.concurrency;
    let mut start_key = options.start_key;
    let mut threads = vec![];
    for i in 0..options.concurrency {
        let mut keys = keys_per_task;
        if i < remainder {
            keys += 1
        }
        let key_range = start_key..(start_key + keys);
        start_key += keys;
        let task = InsertTask::new(client.clone(), key_range, &options);
        let t = thread::spawn(move || task.run());
        threads.push(t);
    }

    for t in threads {
        t.join().unwrap()
    }
}
