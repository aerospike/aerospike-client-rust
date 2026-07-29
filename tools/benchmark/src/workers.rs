// Copyright 2015-2026 Aerospike, Inc.
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

use tokio::sync::mpsc::Sender;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use aerospike::{BatchOperation, Bin, Client, Key};

use crate::args::Args;
use crate::generator::KeyRangeGen;
use crate::percent::Percent;
use crate::stats::{Histogram, LatencyMode, StatsPacket};
use crate::tasks::{
    InsertTask, MrtInsertTask, MrtReadUpdateTask, OpType, ReadFromFileTask, ReadIncrementTask,
    ReadModUpdateTask, ReadUpdateTask, TaskType, TransactionTask,
};
use crate::throttle::RunControl;

pub use crate::tasks::Status;

lazy_static! {
    // How frequently workers send stats to the collector
    pub static ref COLLECT_MS: Duration = Duration::from_millis(100);
}

/// Randomization of the per-transaction read/write counts in the TXN
/// workload (`v:` parameter): absolute (`v:5`) or percentage (`v:20%`).
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Variance {
    None,
    Absolute(u32),
    Percent(f64),
}

impl Variance {
    /// Randomize `base` within ± the variance.
    fn apply(self, base: u32, rng: &mut StdRng) -> u32 {
        let delta = match self {
            Variance::None => return base,
            Variance::Absolute(d) => d,
            Variance::Percent(pct) => ((f64::from(base) * pct) / 100.0).round() as u32,
        };
        if delta == 0 {
            return base;
        }
        let min = base.saturating_sub(delta);
        let max = base + delta;
        rng.random_range(min..=max)
    }
}

/// One operation inside a TXN-workload business transaction.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TxnItem {
    ReadOne,
    ReadAll,
    BatchRead(usize),
    UpdateOne,
    UpdateAll,
    ReplaceOne,
    ReplaceAll,
    Increment,
}

/// TXN workload definition: either read/write counts with optional
/// variance (`TXN,r:10,w:2,v:20%`) or a fixed operation pattern
/// (`TXN,t:rrwub10`).
#[derive(Debug, PartialEq, Clone)]
pub struct TxnSpec {
    pub reads: u32,
    pub writes: u32,
    pub variance: Variance,
    pub pattern: Option<Vec<TxnItem>>,
}

impl TxnSpec {
    /// The operations for one transaction instance.
    pub fn items(&self, rng: &mut StdRng) -> Vec<TxnItem> {
        if let Some(pattern) = &self.pattern {
            return pattern.clone();
        }
        let reads = self.variance.apply(self.reads, rng);
        let writes = self.variance.apply(self.writes, rng);
        // Interleave reads and writes randomly, like the Java benchmark's
        // WorkloadIterator.
        let mut items = Vec::with_capacity((reads + writes) as usize);
        items.extend(std::iter::repeat_n(TxnItem::ReadAll, reads as usize));
        items.extend(std::iter::repeat_n(TxnItem::UpdateAll, writes as usize));
        for i in (1..items.len()).rev() {
            items.swap(i, rng.random_range(0..=i));
        }
        items
    }
}

fn parse_txn_pattern(pattern: &str) -> Result<Vec<TxnItem>, String> {
    let mut items = Vec::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        // Digits following a code repeat it; for batch reads they set the
        // batch size instead.
        let mut count = 0usize;
        while let Some(d) = chars.peek().and_then(|c| c.to_digit(10)) {
            count = count * 10 + d as usize;
            chars.next();
        }
        let repeat = |item: TxnItem, items: &mut Vec<TxnItem>| {
            for _ in 0..count.max(1) {
                items.push(item);
            }
        };
        match c {
            'r' => repeat(TxnItem::ReadOne, &mut items),
            'R' => repeat(TxnItem::ReadAll, &mut items),
            'b' | 'B' => items.push(TxnItem::BatchRead(count.max(1))),
            'u' | 'w' => repeat(TxnItem::UpdateOne, &mut items),
            'U' | 'W' => repeat(TxnItem::UpdateAll, &mut items),
            'p' => repeat(TxnItem::ReplaceOne, &mut items),
            'P' => repeat(TxnItem::ReplaceAll, &mut items),
            'i' => repeat(TxnItem::Increment, &mut items),
            other => return Err(format!("Invalid TXN pattern code `{other}`")),
        }
    }
    if items.is_empty() {
        return Err("Empty TXN pattern".to_string());
    }
    Ok(items)
}

fn parse_txn_spec(parts: &mut std::str::Split<char>) -> Result<TxnSpec, String> {
    let mut reads = 0u32;
    let mut writes = 0u32;
    let mut variance = Variance::None;
    let mut pattern = None;
    for part in parts {
        let part = part.trim();
        let (code, value) = part
            .split_once(':')
            .ok_or_else(|| format!("Invalid TXN parameter `{part}` (expected code:value)"))?;
        match code {
            "r" => {
                reads = value
                    .parse()
                    .map_err(|e| format!("Invalid TXN read count `{value}`: {e}"))?;
            }
            "w" => {
                writes = value
                    .parse()
                    .map_err(|e| format!("Invalid TXN write count `{value}`: {e}"))?;
            }
            "v" => {
                variance = if let Some(pct) = value.strip_suffix('%') {
                    Variance::Percent(
                        pct.parse()
                            .map_err(|e| format!("Invalid TXN variance `{value}`: {e}"))?,
                    )
                } else {
                    Variance::Absolute(
                        value
                            .parse()
                            .map_err(|e| format!("Invalid TXN variance `{value}`: {e}"))?,
                    )
                };
            }
            "t" => pattern = Some(parse_txn_pattern(value)?),
            other => return Err(format!("Invalid TXN parameter code `{other}`")),
        }
    }
    if pattern.is_none() && reads == 0 && writes == 0 {
        return Err("TXN workload needs r:/w: counts or a t: pattern".to_string());
    }
    Ok(TxnSpec {
        reads,
        writes,
        variance,
        pattern,
    })
}

#[derive(Debug, PartialEq, Clone)]
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
    Transactional(TxnSpec),
    ReadFromFile,
}

impl Workload {
    pub fn extract_read_workload_param(&self) -> Option<(Percent, Percent, Percent)> {
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
            } => Some((*read_pct, *r_all_bin_pct, *w_all_bin_pct)),
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
            Some("TXN") => Ok(Workload::Transactional(parse_txn_spec(&mut parts)?)),
            _ => Err(String::from("Invalid workload definition")),
        }
    }
}

/// Everything a worker needs beyond the client: task selection inputs and
/// run-wide controls.
pub struct WorkerConfig {
    pub workload: Workload,
    pub args: Arc<Args>,
    pub control: Arc<RunControl>,
    pub latency_mode: LatencyMode,
    /// YCSB warmup samples this worker skips (global warmup split over
    /// workers).
    pub warmup: u64,
    pub namespace: Arc<str>,
    pub set: Arc<str>,
    pub start_key: i64,
    pub key_count: i64,
    /// Preloaded keys for the read-from-file workload.
    pub file_keys: Option<Arc<Vec<Key>>>,
    /// Operations per task invocation (batch size, or MRT transaction size).
    pub group_size: usize,
}

pub struct Worker {
    read_histogram: Histogram,
    write_histogram: Histogram,
    txn_histogram: Histogram,
    collector: Sender<StatsPacket>,
    task: TaskType,
    rng: StdRng,
    control: Arc<RunControl>,
    latency_mode: LatencyMode,
    warmup_remaining: u64,
    group_size: usize,
    batch: Vec<Key>, // Reused each loop to avoid allocating a new Vec<Key> per batch.
    results: Vec<(Status, Duration, OpType)>, // Reused each batch so task execute() fills this instead of allocating a new Vec.
    batch_ops: Vec<BatchOperation>, // Reused for batch read/write ops to avoid allocating Vec<BatchOperation> per batch.
    bins_buffer: Vec<Bin>,          // Reused for build_bins to avoid allocating Vec<Bin> per call.
}

impl Worker {
    pub fn for_workload(
        client: Arc<Client>,
        sender: Sender<StatsPacket>,
        config: &WorkerConfig,
    ) -> Self {
        let args = config.args.clone();
        let mrt = args.mrt_size;
        let task = match &config.workload {
            Workload::Initialize => {
                if mrt.is_some() {
                    TaskType::MrtInsert(MrtInsertTask::new(client, args))
                } else {
                    TaskType::Insert(InsertTask::new(client, args))
                }
            }
            Workload::ReadModUpdate => {
                TaskType::ReadModifyUpdate(ReadModUpdateTask::new(client, args))
            }
            Workload::ReadAndIncrement => {
                TaskType::ReadIncrement(ReadIncrementTask::new(client, args, 1))
            }
            Workload::ReadAndDecrement => {
                TaskType::ReadIncrement(ReadIncrementTask::new(client, args, -1))
            }
            Workload::Transactional(spec) => TaskType::Transaction(TransactionTask::new(
                client,
                args,
                spec.clone(),
                config.namespace.clone(),
                config.set.clone(),
                config.start_key,
                config.key_count,
            )),
            Workload::ReadFromFile => TaskType::ReadFromFile(ReadFromFileTask::new(
                client,
                args,
                config
                    .file_keys
                    .clone()
                    .expect("read-from-file workload requires keys"),
            )),
            workload => {
                let (read_pct, r_all_bin_pct, w_all_bin_pct) = workload
                    .extract_read_workload_param()
                    .expect("RU or RR workload params");
                if mrt.is_some() {
                    TaskType::MrtReadUpdate(MrtReadUpdateTask::new(
                        client,
                        read_pct,
                        r_all_bin_pct,
                        w_all_bin_pct,
                        args,
                    ))
                } else {
                    TaskType::ReadUpdate(ReadUpdateTask::new(
                        client,
                        read_pct,
                        r_all_bin_pct,
                        w_all_bin_pct,
                        args,
                    ))
                }
            }
        };
        Worker {
            read_histogram: Histogram::new(config.latency_mode),
            write_histogram: Histogram::new(config.latency_mode),
            txn_histogram: Histogram::new(config.latency_mode),
            collector: sender,
            task,
            rng: StdRng::from_rng(&mut rand::rng()),
            control: config.control.clone(),
            latency_mode: config.latency_mode,
            warmup_remaining: config.warmup,
            group_size: config.group_size,
            batch: Vec::with_capacity(config.group_size),
            results: Vec::with_capacity(2),
            batch_ops: Vec::new(),
            bins_buffer: Vec::new(),
        }
    }

    pub async fn run(&mut self, mut key_range: KeyRangeGen, duration_limit: Option<Duration>) {
        let mut last_collection = Instant::now();
        let run_start = Instant::now();
        loop {
            if self.control.stopped() {
                break;
            }
            if let Some(limit) = duration_limit {
                if run_start.elapsed() >= limit {
                    break;
                }
            }
            self.batch.clear();
            for _ in 0..self.group_size {
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
            let mut client_ops = 0u64;
            for (status, duration, op_type) in &self.results {
                let record_latency = if self.warmup_remaining > 0 {
                    self.warmup_remaining -= 1;
                    false
                } else {
                    true
                };
                match op_type {
                    OpType::Read => {
                        client_ops += 1;
                        self.read_histogram
                            .add_sample(*duration, *status, record_latency);
                    }
                    OpType::Write => {
                        client_ops += 1;
                        self.write_histogram
                            .add_sample(*duration, *status, record_latency);
                    }
                    OpType::Txn => {
                        self.txn_histogram
                            .add_sample(*duration, *status, record_latency);
                    }
                }
            }
            self.control.pace(client_ops).await;
            if last_collection.elapsed() > *COLLECT_MS {
                self.collect().await;
                last_collection = Instant::now();
            }
        }
        self.collect().await;
    }

    async fn collect(&mut self) {
        let packet = StatsPacket {
            read: std::mem::replace(&mut self.read_histogram, Histogram::new(self.latency_mode)),
            write: std::mem::replace(&mut self.write_histogram, Histogram::new(self.latency_mode)),
            txn: std::mem::replace(&mut self.txn_histogram, Histogram::new(self.latency_mode)),
        };
        let _ = self.collector.send(packet).await;
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

    #[test]
    fn test_txn_workload_from_str() {
        let w = Workload::from_str("TXN,r:10,w:2,v:20%").unwrap();
        let Workload::Transactional(spec) = w else {
            panic!("expected TXN workload");
        };
        assert_eq!(spec.reads, 10);
        assert_eq!(spec.writes, 2);
        assert_eq!(spec.variance, Variance::Percent(20.0));
        assert!(spec.pattern.is_none());

        let w = Workload::from_str("TXN,t:rrRu2b20ip").unwrap();
        let Workload::Transactional(spec) = w else {
            panic!("expected TXN workload");
        };
        let p = spec.pattern.unwrap();
        assert_eq!(
            p,
            vec![
                TxnItem::ReadOne,
                TxnItem::ReadOne,
                TxnItem::ReadAll,
                TxnItem::UpdateOne,
                TxnItem::UpdateOne,
                TxnItem::BatchRead(20),
                TxnItem::Increment,
                TxnItem::ReplaceOne,
            ]
        );

        assert!(Workload::from_str("TXN").is_err());
        assert!(Workload::from_str("TXN,x:1").is_err());
        assert!(Workload::from_str("TXN,t:q").is_err());
    }

    #[test]
    fn txn_spec_items_respect_variance() {
        let spec = TxnSpec {
            reads: 10,
            writes: 5,
            variance: Variance::None,
            pattern: None,
        };
        let mut rng = StdRng::seed_from_u64(7);
        let items = spec.items(&mut rng);
        assert_eq!(items.len(), 15);
        assert_eq!(
            items.iter().filter(|i| **i == TxnItem::ReadAll).count(),
            10
        );

        // Variance applies to both counts: reads in [5,15], writes in [0,5].
        let spec = TxnSpec {
            reads: 10,
            writes: 0,
            variance: Variance::Absolute(5),
            pattern: None,
        };
        for _ in 0..50 {
            let n = spec.items(&mut rng).len();
            assert!((5..=20).contains(&n), "unexpected item count {n}");
        }
    }
}
