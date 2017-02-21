use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::boxed::Box;

use aerospike::{Key, Bin, Client, WritePolicy};

use cli::Options;
use counters::Counters;

#[derive(Debug)]
pub struct Percent(u8);

#[derive(Debug)]
pub enum Workload {
    // Initialize data with sequential key writes.
    Initialize,

    // Read/Update. Perform random key, random read all bins wor write all bins workload.
    ReadUpdate { read_pct: Percent },
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Workload, String> {
        match s {
            "RU" => Ok(Workload::ReadUpdate { read_pct: Percent(50) }),
            "I" => Ok(Workload::Initialize),
            _ => Err(String::from("Invalid workload definition")),
        }
    }
}

pub trait Worker: Send {
    fn run(&self, key_range: Range<i64>);
}

impl Worker {
    pub fn for_workload(workload: &Workload, client: Arc<Client>, counters: Arc<Counters>, options: &Options) -> Box<Worker> {
        let worker = match *workload {
            Workload::Initialize => InsertWorker::new(client, counters, options),
            Workload::ReadUpdate { .. } => panic!("not yet implemented"),
        };
        Box::new(worker)
    }
}


pub struct InsertWorker {
    client: Arc<Client>,
    policy: WritePolicy,
    namespace: String,
    set: String,
    counters: Arc<Counters>,
}

impl InsertWorker {
    pub fn new(client: Arc<Client>,
               counters: Arc<Counters>,
               options: &Options)
               -> Self {
        InsertWorker {
            client: client,
            policy: WritePolicy::default(),
            namespace: options.namespace.clone(),
            set: options.set.clone(),
            counters: counters,
        }
    }

    fn insert(&self, key: &Key, bins: &[&Bin]) {
        trace!("Inserting {}", key);
        self.counters.track_request(|| self.client.put(&self.policy, key, bins));
    }
}

impl Worker for InsertWorker {
    fn run(&self, key_range: Range<i64>) {
        for i in key_range {
            let key = as_key!(self.namespace.clone(), self.set.clone(), i);
            let bin = as_bin!("1", i);
            self.insert(&key, &[&bin]);
        }
    }
}
