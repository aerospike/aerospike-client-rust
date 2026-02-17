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

use aerospike::{BatchOperation, Bin, Client, Key};

use crate::args::Args;
use crate::generator::KeyRangeGen;
use crate::percent::Percent;
use crate::stats::Histogram;
use crate::tasks::{
    InsertTask, OpType, ReadIncrementTask, ReadModUpdateTask, ReadUpdateTask, TaskType,
};

pub use crate::tasks::Status;

lazy_static! {
    // How frequently workers send stats to the collector
    pub static ref COLLECT_MS: Duration = Duration::from_millis(100);
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Workload {
    Initialize,
    ReadUpdate {
        read_pct: Percent,
        r_all_bin_pct: Percent,
        w_all_bin_pct: Percent,
    },
    ReadReplace {
        read_pct: Percent,
        r_all_bin_pct: Percent,
        w_all_bin_pct: Percent,
    },
    ReadModUpdate,
    ReadAndIncrement,
    ReadAndDecrement,
}

impl Workload {
    pub fn extract_read_workload_param(self) -> Option<(Percent, Percent, Percent)> {
        match self {
            Workload::ReadUpdate {
                read_pct,
                r_all_bin_pct,
                w_all_bin_pct,
            }
            | Workload::ReadReplace {
                read_pct,
                r_all_bin_pct,
                w_all_bin_pct,
            } => Some((read_pct, r_all_bin_pct, w_all_bin_pct)),
            _ => None,
        }
    }
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(s: &str) -> Result<Workload, String> {
        let mut parts = s.split(',');
        match parts.next() {
            Some("RU") => {
                let read_pct = Percent::from_str(parts.next().unwrap_or("100"))?;
                let r_all_bin_pct = Percent::from_str(parts.next().unwrap_or("0"))?;
                let w_all_bin_pct = Percent::from_str(parts.next().unwrap_or("0"))?;
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for RU"));
                }
                Ok(Workload::ReadUpdate {
                    read_pct,
                    r_all_bin_pct,
                    w_all_bin_pct,
                })
            }
            Some("RR") => {
                let read_pct = Percent::from_str(parts.next().unwrap_or("100"))?;
                let r_all_bin_pct = Percent::from_str(parts.next().unwrap_or("0"))?;
                let w_all_bin_pct = Percent::from_str(parts.next().unwrap_or("0"))?;
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for RR"));
                }
                Ok(Workload::ReadReplace {
                    read_pct,
                    r_all_bin_pct,
                    w_all_bin_pct,
                })
            }
            Some("I") => {
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for I"));
                }
                Ok(Workload::Initialize)
            }
            Some("RMU") => {
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for RMU"));
                }
                Ok(Workload::ReadModUpdate)
            }
            Some("RMI") => {
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for RMI"));
                }
                Ok(Workload::ReadAndIncrement)
            }
            Some("RMD") => {
                if parts.next().is_some() {
                    return Err(String::from("Extra parameter(s) not allowed for RMD"));
                }
                Ok(Workload::ReadAndDecrement)
            }
            _ => Err(String::from("Invalid workload definition")),
        }
    }
}

pub struct Worker {
    read_histogram: Histogram,
    write_histogram: Histogram,
    collector: UnboundedSender<(Histogram, Histogram)>,
    task: TaskType,
    rng: StdRng,
    batch_size: usize, 
    batch: Vec<Key>,                          // Reused each loop to avoid allocating a new Vec<Key> per batch.
    results: Vec<(Status, Duration, OpType)>, // Reused each batch so task execute() fills this instead of allocating a new Vec.
    batch_ops: Vec<BatchOperation>, // Reused for batch read/write ops to avoid allocating Vec<BatchOperation> per batch.
    bins_buffer: Vec<Bin>,          // Reused for build_bins to avoid allocating Vec<Bin> per call.
}

impl Worker {
    pub fn for_workload(
        workload: Workload,
        client: Arc<Client>,
        sender: UnboundedSender<(Histogram, Histogram)>,
        args: Arc<Args>,
    ) -> Self {
        let batch_size = args.batch_size;
        let task = match workload {
            Workload::Initialize => TaskType::Insert(InsertTask::new(client, args)),
            Workload::ReadModUpdate => {
                TaskType::ReadModifyUpdate(ReadModUpdateTask::new(client, args))
            }
            Workload::ReadAndIncrement => {
                TaskType::ReadIncrement(ReadIncrementTask::new(client, args, 1))
            }
            Workload::ReadAndDecrement => {
                TaskType::ReadIncrement(ReadIncrementTask::new(client, args, -1))
            }
            _ => {
                let (read_pct, r_all_bin_pct, w_all_bin_pct) = workload
                    .extract_read_workload_param()
                    .expect("RU or RR workload params");
                TaskType::ReadUpdate(ReadUpdateTask::new(
                    client,
                    read_pct,
                    r_all_bin_pct,
                    w_all_bin_pct,
                    args,
                ))
            }
        };
        Worker {
            read_histogram: Histogram::new(),
            write_histogram: Histogram::new(),
            collector: sender,
            task,
            rng: StdRng::from_entropy(),
            batch_size,
            batch: Vec::with_capacity(batch_size),
            results: Vec::with_capacity(2), // max 2 per batch (e.g. RMU, RMI)
            batch_ops: Vec::new(),
            bins_buffer: Vec::new(),
        }
    }

    pub async fn run(&mut self, key_range: KeyRangeGen) {
        let mut last_collection = Instant::now();
        let mut key_range = key_range;
        loop {
            self.batch.clear();
            for _ in 0..self.batch_size {
                match key_range.next() {
                    Some(k) => self.batch.push(k),
                    None => break,
                }
            }
            if self.batch.is_empty() {
                break;
            }
            self.task
                .execute(
                    &self.batch,
                    &mut self.rng,
                    &mut self.results,
                    &mut self.batch_ops,
                    &mut self.bins_buffer,
                )
                .await;
            for (status, duration, op_type) in &self.results {
                match op_type {
                    OpType::Read => self.read_histogram.add(*duration, *status),
                    OpType::Write => self.write_histogram.add(*duration, *status),
                }
            }
            if last_collection.elapsed() > *COLLECT_MS {
                self.collect();
                last_collection = Instant::now();
            }
        }
        self.collect();

    }

    fn collect(&mut self) {
        let _ = self.collector.send((self.read_histogram, self.write_histogram));
        self.read_histogram.reset();
        self.write_histogram.reset();
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
                r_all_bin_pct: Percent::new(0),
                w_all_bin_pct: Percent::new(0)
            })
        );
        assert_eq!(
            Workload::from_str("RU,50"),
            Ok(Workload::ReadUpdate {
                read_pct: Percent::new(50),
                r_all_bin_pct: Percent::new(0),
                w_all_bin_pct: Percent::new(0)
            })
        );
        assert_eq!(
            Workload::from_str("RR"),
            Ok(Workload::ReadReplace {
                read_pct: Percent::new(100),
                r_all_bin_pct: Percent::new(0),
                w_all_bin_pct: Percent::new(0)
            })
        );
    }
}
