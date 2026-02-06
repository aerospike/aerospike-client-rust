// Copyright 2015-2018 Aerospike, Inc.
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

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::UnboundedSender;

use rand::rngs::StdRng;
use rand::SeedableRng;

use aerospike::{Client, Key};

use crate::args::Args;
use crate::generator::KeyRangeGen;
use crate::percent::Percent;
use crate::stats::Histogram;
use crate::tasks::{InsertTask, ReadUpdateTask, TaskType};

pub use crate::tasks::Status;

lazy_static! {
    // How frequently workers send stats to the collector
    pub static ref COLLECT_MS: Duration = Duration::from_millis(100);
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Workload {
    // Initialize data with sequential key writes.
    Initialize,

    // Read/Update. Perform random key, random read all bins or write all bins workload.
    ReadUpdate { read_pct: Percent, r_all_bin_pct: Percent, w_all_bin_pct: Percent },
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Workload, String> {
        let mut parts = s.split(',');
        match parts.next() {
            Some("RU") => {
                let read_pct   = Percent::from_str(parts.next().unwrap_or("100"))?;
                let r_all_bin_pct = Percent::from_str(parts.next().unwrap_or("0"))?; // default to single bin read
                let w_all_bin_pct  = Percent::from_str(parts.next().unwrap_or("0"))?; // default to single bin write

                Ok(Workload::ReadUpdate {
                    read_pct,
                    r_all_bin_pct,
                    w_all_bin_pct,
                })
            }
            Some("I") => Ok(Workload::Initialize),
            _ => Err(String::from("Invalid workload definition")),
        }
        
    }
}

pub struct Worker {
    histogram: Histogram,
    collector: UnboundedSender<Histogram>,
    task: TaskType,
    rng: StdRng,
    batch_size: usize
}

impl Worker {
    pub fn for_workload(
        workload: Workload,
        client: Arc<Client>,
        sender: UnboundedSender<Histogram>,
        args: Arc<Args>
    ) -> Self {
        let batch_size = args.batch_size;
        let task = match workload {
            Workload::Initialize => TaskType::Insert(InsertTask::new(client, args)),
            Workload::ReadUpdate { read_pct, r_all_bin_pct, w_all_bin_pct } => {
                TaskType::ReadUpdate(ReadUpdateTask::new(client, read_pct, r_all_bin_pct, w_all_bin_pct, args))
            }
        };
        Worker {
            histogram: Histogram::new(),
            collector: sender,
            task,
            rng: StdRng::from_entropy(),
            batch_size,
        }
    }

    pub async fn run(&mut self, key_range: KeyRangeGen) {
        let mut last_collection = Instant::now();
        let mut key_range = key_range;
        loop {
            let batch: Vec<Key> = key_range.by_ref().take(self.batch_size).collect();
            if batch.is_empty() {
                break;
            }
            let now = Instant::now();
            let status = self.task.execute(&batch, &mut self.rng).await;
            self.histogram.add(now.elapsed(), status);
            if last_collection.elapsed() > *COLLECT_MS {
                self.collect();
                last_collection = Instant::now();
            }
            self.collect();
        }
    }

    fn collect(&mut self) {
        let _ = self.collector.send(self.histogram);
        self.histogram.reset();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_workload_from_str() {
        assert_eq!(Workload::from_str("I"), Ok(Workload::Initialize));
        assert_eq!(
            Workload::from_str("RU"),
            Ok(Workload::ReadUpdate {
                read_pct: Percent::new(100),
                r_all_bin_pct: Percent:: new(0),
                w_all_bin_pct: Percent:: new(0)
            })
        );
        assert_eq!(
            Workload::from_str("RU,50"),
            Ok(Workload::ReadUpdate {
                read_pct: Percent::new(50),
                r_all_bin_pct: Percent:: new(0),
                w_all_bin_pct: Percent:: new(0)
            })
        );
    }
}



 