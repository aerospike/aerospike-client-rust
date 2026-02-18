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

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::Rng;

use aerospike::Result as asResult;
use aerospike::{BatchOperation, Bin, Bins, Client, Error, Key, ResultCode, WritePolicy};

use crate::args::Args;
use crate::batch_ops::{build_batch_read_ops, build_batch_write_ops};
use crate::percent::Percent;

#[derive(Clone, Copy)]
pub enum Status {
    Success,
    Error,
    Timeout,
}

#[derive(Clone, Copy)]
pub enum OpType {
    Read,
    Write,
}

pub enum TaskType {
    Insert(InsertTask),
    ReadUpdate(ReadUpdateTask),
    ReadModifyUpdate(ReadModUpdateTask),
    ReadIncrement(ReadIncrementTask),
}

impl TaskType {
    pub async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        match self {
            TaskType::Insert(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::ReadUpdate(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::ReadModifyUpdate(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::ReadIncrement(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
        }
    }
}

trait Task: Send {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    );

    fn status<T>(&self, result: asResult<T>) -> Status {
        match result {
            Err(Error::ServerError(ResultCode::Timeout, _, _) | Error::Timeout(_)) => {
                Status::Timeout
            }
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => Status::Success,
            Err(_) => Status::Error,
            _ => Status::Success,
        }
    }

    async fn timed_execution<F, T>(&self, fut: F) -> (Status, Duration)
    where
        F: Future<Output = asResult<T>> + Send,
    {
        let start = Instant::now();
        let status = self.status(fut.await);
        (status, start.elapsed())
    }
}

// ------ Insert Task ---------

pub struct InsertTask {
    client: Arc<Client>,
    policy: WritePolicy,
    args: Arc<Args>,
}

impl InsertTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>) -> Self {
        InsertTask {
            client,
            policy: WritePolicy::default(),
            args,
        }
    }
}

impl Task for InsertTask {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let key = &keys[0];
        self.args.build_bins(key, rng, None, bins_buffer);
        trace!("Inserting {}", key);
        let (status, duration) = self
            .timed_execution(self.client.put(&self.policy, key, bins_buffer))
            .await;
        results.push((status, duration, OpType::Write));
    }
}

// ------ ReadModUpdateTask ---------

pub struct ReadModUpdateTask {
    client: Arc<Client>,
    args: Arc<Args>,
}

impl ReadModUpdateTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>) -> Self {
        ReadModUpdateTask { client, args }
    }
}

impl Task for ReadModUpdateTask {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let key = &keys[0];
        // Read all bins
        let client = self.client.clone();
        let policy = self.args.read_policy.clone();
        let key_clone = key.clone();
        let (status, duration) = self
            .timed_execution(client.get(&policy, &key_clone, Bins::All))
            .await;
        results.push((status, duration, OpType::Read));

        // write single bins
        self.args.build_bins(key, rng, Some(1), bins_buffer);
        trace!("Writing first bin {}", key);
        let (status, duration) = self
            .timed_execution(
                self.client
                    .put(&self.args.write_policy, key, &bins_buffer[..1]),
            )
            .await;
        results.push((status, duration, OpType::Write));
    }
}

// ------ ReadUpdateTask ---------

pub struct ReadUpdateTask {
    client: Arc<Client>,
    reads: Percent,
    read_bins_pct: Percent,
    write_bins_pct: Percent,
    args: Arc<Args>,
    first_bin_name: String,
}

impl ReadUpdateTask {
    pub fn new(
        client: Arc<Client>,
        reads: Percent,
        read_bins_pct: Percent,
        write_bins_pct: Percent,
        args: Arc<Args>,
    ) -> Self {
        let first_bin_name = format!("{}_{}", args.bin_name_base, 1);
        ReadUpdateTask {
            client,
            reads,
            read_bins_pct,
            write_bins_pct,
            args,
            first_bin_name,
        }
    }
}

impl Task for ReadUpdateTask {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        if rng.gen_range(0..100u8) < self.reads.as_u8() {
            self.execute_read(keys, rng, results, batch_ops, bins_buffer)
                .await
        } else {
            self.execute_write(keys, rng, results, batch_ops, bins_buffer)
                .await
        }
    }
}

impl ReadUpdateTask {
    async fn execute_read(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        _bins_buffer: &mut Vec<Bin>,
    ) {
        if keys.is_empty() {
            return;
        }
        let multi_bins_read = rng.gen_range(0..100u8) < self.read_bins_pct.as_u8();
        match keys.len() {
            1 => {
                let key = &keys[0];
                if multi_bins_read {
                    trace!("Reading all bins {}", key);
                    let client = self.client.clone();
                    let (status, duration) = self
                        .timed_execution(client.get(&self.args.read_policy, &key, Bins::All))
                        .await;
                    results.push((status, duration, OpType::Read));
                } else {
                    trace!("Reading single bin {} {}", self.first_bin_name, key);
                    let client = self.client.clone();
                    let (status, duration) = self
                        .timed_execution(client.get(
                            &self.args.read_policy,
                            &key,
                            Bins::from([self.first_bin_name.as_str()]),
                        ))
                        .await;
                    results.push((status, duration, OpType::Read));
                }
            }
            _ => {
                // batch read
                trace!("Batch Reads ");
                build_batch_read_ops(keys, &self.args.batch_read_policy, Bins::All, batch_ops);
                let ops = batch_ops.as_slice();
                let client = self.client.clone();
                let (status, duration) = self
                    .timed_execution(client.batch(&self.args.batch_policy, ops))
                    .await;
                results.push((status, duration, OpType::Read));
            }
        }
    }

    async fn execute_write(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        if keys.is_empty() {
            return;
        }
        let multi_bins_write = rng.gen_range(0..100u8) < self.write_bins_pct.as_u8();
        match keys.len() {
            1 => {
                let key = &keys[0];
                if multi_bins_write {
                    self.args.build_bins(key, rng, None, bins_buffer);
                    trace!("Writing all bins {}", key);
                    let (status, duration) = self
                        .timed_execution(self.client.put(&self.args.write_policy, key, bins_buffer))
                        .await;
                    results.push((status, duration, OpType::Write));
                } else {
                    self.args.build_bins(key, rng, Some(1), bins_buffer);
                    trace!("Writing first bin {}", key);
                    let (status, duration) = self
                        .timed_execution(self.client.put(
                            &self.args.write_policy,
                            key,
                            &bins_buffer[..1],
                        ))
                        .await;
                    results.push((status, duration, OpType::Write));
                }
            }
            _ => {
                // batch write
                build_batch_write_ops(
                    keys,
                    &self.args,
                    rng,
                    multi_bins_write,
                    batch_ops,
                    bins_buffer,
                );
                let ops = batch_ops.as_slice();
                let client = self.client.clone();
                let policy = self.args.batch_policy.clone();
                let (status, duration) = self.timed_execution(client.batch(&policy, ops)).await;
                results.push((status, duration, OpType::Write));
            }
        }
    }
}

// ------ ReadIncrementTask ---------

pub struct ReadIncrementTask {
    client: Arc<Client>,
    args: Arc<Args>,
    write_policy: WritePolicy,
    delta: i64,
    /// Precomputed to avoid format! allocation every execute().
    counter_bin_name: String,
}

impl ReadIncrementTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>, delta: i64) -> Self {
        let mut write_policy = args.write_policy.clone();
        write_policy.generation_policy = aerospike::GenerationPolicy::ExpectGenEqual;
        write_policy.generation = 0;
        let counter_bin_name = format!("{}_counter", args.bin_name_base);
        Self {
            client,
            args,
            write_policy,
            delta,
            counter_bin_name,
        }
    }
}

impl Task for ReadIncrementTask {
    async fn execute(
        &self,
        keys: &[Key],
        _rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        _bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let key = &keys[0];
        // Read all bins
        let client = self.client.clone();
        let policy = self.args.read_policy.clone();
        let key_clone = key.clone();
        let (status, duration) = self
            .timed_execution(client.get(&policy, &key_clone, Bins::All))
            .await;
        results.push((status, duration, OpType::Read));
        let bins = [as_bin!(self.counter_bin_name.as_str(), self.delta)];
        let (status, duration) = self
            .timed_execution(self.client.add(&self.write_policy, key, &bins))
            .await;
        results.push((status, duration, OpType::Write));
    }
}
