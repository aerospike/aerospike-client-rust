use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::time::{Duration, Instant};

use aerospike::{Key, Client, WritePolicy, ErrorKind, ResultCode};
use aerospike::Result as asResult;
use aerospike::Error as asError;

use generator::KeyRange;
use stats::{Collector, Histogram};
use util;

lazy_static! {
    // How frequently workers send stats to the collector (milliseconds)
    pub static ref COLLECT_MS: Duration = Duration::from_millis(100);
}

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
    collector: Sender<Histogram>,
    last_report: Instant,
    task: Box<Task>,
}

impl Worker {
    pub fn for_workload(workload: &Workload, client: Arc<Client>, collector: &Collector) -> Self {
        let task = match *workload {
            Workload::Initialize => Box::new(InsertTask::new(client)),
            Workload::ReadUpdate { .. } => panic!("not yet implemented"),
        };
        Worker {
            histogram: Histogram::new(),
            collector: collector.sender(),
            last_report: Instant::now(),
            task: task,
        }
    }

    pub fn run(&mut self, key_range: KeyRange) {
        for key in key_range {
            let now = Instant::now();
            if let Err(error) = self.task.execute(&key) {
                match error {
                    asError(ErrorKind::ServerError(ResultCode::Timeout), _) => {
                        self.histogram.timeouts += 1
                    }
                    _ => self.histogram.errors += 1,
                }
            }
            let millis = util::elapsed_millis(now);
            self.histogram.add(millis);
            self.collect(false);
        }
        self.collect(true);
    }

    fn collect(&mut self, force: bool) {
        if force || self.last_report.elapsed() > *COLLECT_MS {
            self.collector.send(self.histogram).unwrap();
            self.histogram.reset();
            self.last_report = Instant::now();
        }
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
