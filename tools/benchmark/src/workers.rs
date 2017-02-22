use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::time::{Duration, Instant};

use aerospike::{Key, Bin, Client, WritePolicy, ErrorKind, ResultCode};
use aerospike::Result as asResult;
use aerospike::Error as asError;

use cli::Options;
use reporter::Histogram;

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
    fn run(&mut self, key_range: Range<i64>);
}

impl Worker {
    pub fn for_workload(workload: &Workload,
                        client: Arc<Client>,
                        reporter: Sender<Histogram>,
                        options: &Options)
                        -> Box<Worker> {
        let worker = match *workload {
            Workload::Initialize => InsertWorker::new(client, reporter, options),
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
    histogram: Histogram,
    reporter: Sender<Histogram>,
    last_report: Instant,
}

impl InsertWorker {
    pub fn new(client: Arc<Client>, reporter: Sender<Histogram>, options: &Options) -> Self {
        InsertWorker {
            client: client,
            policy: WritePolicy::default(),
            namespace: options.namespace.clone(),
            set: options.set.clone(),
            histogram: Histogram::new(4),
            reporter: reporter,
            last_report: Instant::now(),
        }
    }

    fn insert(&self, key: &Key, bins: &[&Bin]) -> asResult<()> {
        trace!("Inserting {}", key);
        self.client.put(&self.policy, key, bins)
    }
}

impl Worker for InsertWorker {
    fn run(&mut self, key_range: Range<i64>) {
        for i in key_range {
            let key = as_key!(self.namespace.clone(), self.set.clone(), i);
            let bin = as_bin!("1", i);
            let now = Instant::now();
            match self.insert(&key, &[&bin]) {
                Err(asError(ErrorKind::ServerError(ResultCode::Timeout), _)) => {
                    self.histogram.timeouts += 1;
                    ()
                }
                Err(_) => {
                    self.histogram.errors += 1;
                    ()
                }
                Ok(_) => {}
            }
            let elapsed = now.elapsed();
            let millis = elapsed.as_secs() * 1_000 + elapsed.subsec_nanos() as u64 / 1_000_000;
            self.histogram.add(millis);
            if self.last_report.elapsed() > Duration::new(0, 100_000_000) {
                self.reporter.send(self.histogram.clone()).unwrap();
                self.histogram.reset();
                self.last_report = Instant::now();
            }
        }
        self.reporter.send(self.histogram.clone()).unwrap();
    }
}
