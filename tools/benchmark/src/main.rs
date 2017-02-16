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

use aerospike::{Client, ClientPolicy};

use cli::Options;
use workload::Workload;
use tasks::InsertTask;

fn main() {
    env_logger::init().unwrap();
    let options = cli::parse_options();
    debug!("Command line options: {:?}", options);
    let workload = &options.workload;
    let client = connect(&options);
    run_workload(workload, &client, &options);
}

fn connect(options: &Options) -> Client {
    let policy = ClientPolicy::default();
    Client::new(&policy, &options.hosts).unwrap()
}

fn run_workload(workload: &Workload, client: &Client, options: &Options) {
    println!("Running benchmark with workload {:?}", workload);
    let key_range = options.start_key..(options.start_key + options.keys);
    InsertTask::new(client, key_range, options).run();
}
