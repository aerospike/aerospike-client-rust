use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::time::{Duration, Instant};

use aerospike::{Key, Client, WritePolicy, ErrorKind, ResultCode};
use aerospike::Result as asResult;
use aerospike::Error as asError;

use generator::KeyRange;
use stats::Histogram;

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

pub struct Worker {
    histogram: Histogram,
    reporter: Sender<Histogram>,
    last_report: Instant,
    task: Box<Task>,
}

impl Worker {
    pub fn for_workload(workload: &Workload,
                        client: Arc<Client>,
                        reporter: Sender<Histogram>)
                        -> Self {
        let task = match *workload {
            Workload::Initialize => Box::new(InsertTask::new(client)),
            Workload::ReadUpdate { .. } => panic!("not yet implemented"),
        };
        Worker {
            histogram: Histogram::new(4),
            reporter: reporter,
            last_report: Instant::now(),
            task: task,
        }
    }

    pub fn run(&mut self, key_range: KeyRange) {
        for key in key_range {
            let now = Instant::now();
            match self.task.execute(&key) {
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

trait Task: Send {
    fn execute(&self, key: &Key) -> asResult<()>;
}

pub struct InsertTask {
    client: Arc<Client>,
    policy: WritePolicy,
}

impl InsertTask {
    pub fn new(client: Arc<Client>) -> Self {
        InsertTask {
            client: client,
            policy: WritePolicy::default(),
        }
    }
}

impl Task for InsertTask {
    fn execute(&self, key: &Key) -> asResult<()> {
        let bin = as_bin!("1", 1);
        trace!("Inserting {}", key);
        self.client.put(&self.policy, key, &[&bin])
    }
}
