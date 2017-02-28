// Copyright 2015-2017 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::cmp::Ordering;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::boxed::Box;
use std::time::{Duration, Instant};
use std::rc::Rc;
use std::cell::RefCell;

use rand;
use rand::{Rand, Rng, XorShiftRng};

use aerospike::{Key, Client, ReadPolicy, WritePolicy, ErrorKind, ResultCode};
use aerospike::Result as asResult;
use aerospike::Error as asError;

use generator::KeyRange;
use stats::Histogram;

lazy_static! {
    // How frequently workers send stats to the collector
    pub static ref COLLECT_MS: Duration = Duration::from_millis(100);
}

thread_local!(static THREAD_WEAK_RNG_KEY: Rc<RefCell<XorShiftRng>> = {
    Rc::new(RefCell::new(rand::weak_rng()))
});

fn random<T: Rand>() -> T {
    THREAD_WEAK_RNG_KEY.with(|rng| rng.borrow_mut().gen())
}

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Debug)]
pub struct Percent(u8);

impl FromStr for Percent {
    type Err = String;

    fn from_str(s: &str) -> Result<Percent, String> {
        if let Ok(pct) = u8::from_str(s) {
            if pct <= 100 {
                return Ok(Percent(pct));
            }
        }
        Err("Invalid percent value".into())
    }
}

impl Ord for Percent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Rand for Percent {
    fn rand<R: Rng>(rng: &mut R) -> Self {
        let r: u32 = rng.gen();
        let pct = r % 101;
        Percent(pct as u8)
    }
}

#[derive(Debug, PartialEq)]
pub enum Workload {
    // Initialize data with sequential key writes.
    Initialize,

    // Read/Update. Perform random key, random read all bins wor write all bins workload.
    ReadUpdate { read_pct: Percent },
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Workload, String> {
        let mut parts = s.splitn(2, ',');
        match parts.next() {
            Some("RU") => {
                let read_pct = Percent::from_str(parts.next().unwrap_or("100"))?;
                Ok(Workload::ReadUpdate { read_pct: read_pct })
            }
            Some("I") => Ok(Workload::Initialize),
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
    pub fn for_workload(workload: &Workload,
                        client: Arc<Client>,
                        sender: Sender<Histogram>)
                        -> Self {
        let task: Box<Task> = match *workload {
            Workload::Initialize => Box::new(InsertTask::new(client)),
            Workload::ReadUpdate { read_pct } => Box::new(ReadUpdateTask::new(client, read_pct)),
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
            let status = self.task.execute(&key);
            self.histogram.add(now.elapsed(), status);
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

pub enum Status {
    Success,
    Error,
    Timeout,
}

trait Task: Send {
    fn execute(&self, key: &Key) -> Status;

    fn status(&self, result: asResult<()>) -> Status {
        match result {
            Err(asError(ErrorKind::ServerError(ResultCode::Timeout), _)) => Status::Timeout,
            Err(_) => Status::Error,
            _ => Status::Success,
        }
    }
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
    fn execute(&self, key: &Key) -> Status {
        let bin = as_bin!("int", random::<i64>());
        trace!("Inserting {}", key);
        self.status(self.client.put(&self.policy, key, &[&bin]))
    }
}

pub struct ReadUpdateTask {
    client: Arc<Client>,
    rpolicy: ReadPolicy,
    wpolicy: WritePolicy,
    reads: Percent,
}

impl ReadUpdateTask {
    pub fn new(client: Arc<Client>, reads: Percent) -> Self {
        ReadUpdateTask {
            client: client,
            rpolicy: ReadPolicy::default(),
            wpolicy: WritePolicy::default(),
            reads: reads,
        }
    }
}

impl Task for ReadUpdateTask {
    fn execute(&self, key: &Key) -> Status {
        if self.reads >= random() {
            trace!("Reading {}", key);
            self.status(self.client.get(&self.rpolicy, key, Some(&["int"])).map(|_| ()))
        } else {
            trace!("Writing {}", key);
            let bin = as_bin!("int", random::<i64>());
            self.status(self.client.put(&self.wpolicy, key, &[&bin]))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_percent_from_str() {
        assert_eq!(Percent::from_str("42"), Ok(Percent(42)));
        assert!(Percent::from_str("0.5").is_err());
        assert!(Percent::from_str("120").is_err());
        assert!(Percent::from_str("abc").is_err());
    }

    #[test]
    fn test_workload_from_str() {
        assert_eq!(Workload::from_str("I"), Ok(Workload::Initialize));
        assert_eq!(Workload::from_str("RU"),
                   Ok(Workload::ReadUpdate { read_pct: Percent(100) }));
        assert_eq!(Workload::from_str("RU,50"),
                   Ok(Workload::ReadUpdate { read_pct: Percent(50) }));
    }
}
