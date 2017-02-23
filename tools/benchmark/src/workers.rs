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
use util;

lazy_static! {
    // How frequently workers send stats to the collector
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
    task: Box<Task>,
}

impl Worker {
    pub fn for_workload(workload: &Workload, client: Arc<Client>, sender: Sender<Histogram>) -> Self {
        let task = match *workload {
            Workload::Initialize => Box::new(InsertTask::new(client)),
            Workload::ReadUpdate { .. } => panic!("not yet implemented"),
        };
        Worker {
            histogram: Histogram::new(),
            collector: sender,
            task: task,
        }
    }

    pub fn run(&mut self, key_range: KeyRange) {
        let mut last_collection = Instant::now();
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
            if last_collection.elapsed() > *COLLECT_MS {
                self.collect();
                last_collection = Instant::now();
            }
        }
        self.collect();
    }

    fn collect(&mut self) {
        self.collector.send(self.histogram).unwrap();
        self.histogram.reset();
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
