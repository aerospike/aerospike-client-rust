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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use rand::rngs::StdRng;
use rand::RngExt;

use aerospike::Result as asResult;
use aerospike::{BatchOperation, Bin, Bins, Client, Key, ResultCode, Txn, WritePolicy};

use crate::args::Args;
use crate::batch_ops::{build_batch_read_ops, build_batch_write_ops};
use crate::percent::Percent;
use crate::workers::{TxnItem, TxnSpec};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Success,
    /// Read of a non-existent record: a successful op, reported separately
    /// with -N/--report-not-found.
    NotFound,
    Error,
    Timeout,
}

#[derive(Clone, Copy)]
pub enum OpType {
    Read,
    Write,
    /// Whole-transaction sample (TXN and MRT workloads).
    Txn,
}

pub enum TaskType {
    Insert(InsertTask),
    ReadUpdate(ReadUpdateTask),
    ReadModifyUpdate(ReadModUpdateTask),
    ReadIncrement(ReadIncrementTask),
    Transaction(TransactionTask),
    ReadFromFile(ReadFromFileTask),
    MrtInsert(MrtInsertTask),
    MrtReadUpdate(MrtReadUpdateTask),
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
            TaskType::Transaction(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::ReadFromFile(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::MrtInsert(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
            TaskType::MrtReadUpdate(task) => {
                task.execute(keys, rng, results, batch_ops, bins_buffer)
                    .await
            }
        }
    }
}

fn status_of<T>(result: &asResult<T>) -> Status {
    match result {
        Err(e)
            if e.server_result_code() == Some(ResultCode::Timeout)
                || matches!(e.kind(), aerospike::ErrorKind::Timeout) =>
        {
            Status::Timeout
        }
        Err(e) if e.server_result_code() == Some(ResultCode::KeyNotFoundError) => Status::NotFound,
        Err(_) => Status::Error,
        _ => Status::Success,
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
        status_of(&result)
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
    args: Arc<Args>,
}

impl InsertTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>) -> Self {
        InsertTask { client, args }
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
        if self.args.skip_key(key) {
            results.push((Status::Success, Duration::ZERO, OpType::Write));
            return;
        }
        self.args.build_bins(key, rng, None, bins_buffer);
        trace!("Inserting {}", key);
        let (status, duration) = self
            .timed_execution(self.client.put(&self.args.write_policy, key, bins_buffer))
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
        if self.args.skip_key(key) {
            results.push((Status::Success, Duration::ZERO, OpType::Read));
            return;
        }
        // Read all bins
        let (status, duration) = self
            .timed_execution(self.client.get(&self.args.read_policy, key, Bins::All))
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
        if rng.random_range(0..100u8) < self.reads.as_u8() {
            self.execute_read(keys, rng, results, batch_ops, bins_buffer)
                .await
        } else {
            self.execute_write(keys, rng, results, batch_ops, bins_buffer)
                .await
        }
    }
}

impl ReadUpdateTask {
    /// Single-key read: UDF execute when configured, all-bins or first-bin
    /// get otherwise.
    async fn read_one(
        &self,
        key: &Key,
        multi_bins: bool,
        results: &mut Vec<(Status, Duration, OpType)>,
    ) {
        if self.args.skip_key(key) {
            results.push((Status::Success, Duration::ZERO, OpType::Read));
            return;
        }
        if let Some(udf) = &self.args.udf {
            trace!("UDF read {} {}", udf.function, key);
            let (status, duration) = self
                .timed_execution(self.client.execute_udf(
                    &self.args.write_policy,
                    key,
                    &udf.package,
                    &udf.function,
                    Some(&udf.values),
                ))
                .await;
            results.push((status, duration, OpType::Read));
        } else if multi_bins {
            trace!("Reading all bins {}", key);
            let (status, duration) = self
                .timed_execution(self.client.get(&self.args.read_policy, key, Bins::All))
                .await;
            results.push((status, duration, OpType::Read));
        } else {
            trace!("Reading single bin {} {}", self.first_bin_name, key);
            let (status, duration) = self
                .timed_execution(self.client.get(
                    &self.args.read_policy,
                    key,
                    Bins::from([self.first_bin_name.as_str()]),
                ))
                .await;
            results.push((status, duration, OpType::Read));
        }
    }

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
        let multi_bins_read = rng.random_range(0..100u8) < self.read_bins_pct.as_u8();
        match keys.len() {
            1 => self.read_one(&keys[0], multi_bins_read, results).await,
            _ => {
                // batch read
                trace!("Batch Reads ");
                build_batch_read_ops(
                    keys,
                    &self.args.batch_read_policy,
                    Bins::All,
                    &self.args.batch_namespaces,
                    batch_ops,
                );
                let ops = batch_ops.as_slice();
                let (status, duration) = self
                    .timed_execution(self.client.batch(&self.args.batch_policy, ops))
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
        let multi_bins_write = rng.random_range(0..100u8) < self.write_bins_pct.as_u8();
        match keys.len() {
            1 => {
                let key = &keys[0];
                if self.args.skip_key(key) {
                    results.push((Status::Success, Duration::ZERO, OpType::Write));
                    return;
                }
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
                let policy = self.args.batch_policy.clone();
                let (status, duration) = self
                    .timed_execution(self.client.batch(&policy, ops))
                    .await;
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
        if self.args.skip_key(key) {
            results.push((Status::Success, Duration::ZERO, OpType::Read));
            return;
        }
        // Read all bins
        let (status, duration) = self
            .timed_execution(self.client.get(&self.args.read_policy, key, Bins::All))
            .await;
        results.push((status, duration, OpType::Read));
        let bins = [as_bin!(self.counter_bin_name.as_str(), self.delta)];
        let (status, duration) = self
            .timed_execution(self.client.add(&self.write_policy, key, &bins))
            .await;
        results.push((status, duration, OpType::Write));
    }
}

// ------ TransactionTask (TXN workload) ---------

/// Runs one "business transaction" per execute(): a group of single-record
/// reads/writes (per the TXN spec), timed as a whole in addition to the
/// individual operations. Keys are drawn randomly from the working set.
pub struct TransactionTask {
    client: Arc<Client>,
    args: Arc<Args>,
    spec: TxnSpec,
    namespace: Arc<str>,
    set: Arc<str>,
    start_key: i64,
    key_count: i64,
    first_bin_name: String,
    replace_policy: WritePolicy,
    increment_policy: WritePolicy,
    counter_bin_name: String,
}

impl TransactionTask {
    pub fn new(
        client: Arc<Client>,
        args: Arc<Args>,
        spec: TxnSpec,
        namespace: Arc<str>,
        set: Arc<str>,
        start_key: i64,
        key_count: i64,
    ) -> Self {
        let first_bin_name = format!("{}_{}", args.bin_name_base, 1);
        let mut replace_policy = args.write_policy.clone();
        replace_policy.record_exists_action = aerospike::RecordExistsAction::Replace;
        let mut increment_policy = args.write_policy.clone();
        increment_policy.generation_policy = aerospike::GenerationPolicy::ExpectGenEqual;
        increment_policy.generation = 0;
        let counter_bin_name = format!("{}_counter", args.bin_name_base);
        TransactionTask {
            client,
            args,
            spec,
            namespace,
            set,
            start_key,
            key_count,
            first_bin_name,
            replace_policy,
            increment_policy,
            counter_bin_name,
        }
    }

    fn random_key(&self, rng: &mut StdRng) -> Key {
        let k = rng.random_range(self.start_key..self.start_key + self.key_count);
        as_key!(self.namespace.as_ref(), self.set.as_ref(), k)
    }

    async fn run_item(
        &self,
        item: TxnItem,
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        match item {
            TxnItem::ReadOne | TxnItem::ReadAll => {
                let key = self.random_key(rng);
                if self.args.skip_key(&key) {
                    results.push((Status::Success, Duration::ZERO, OpType::Read));
                    return;
                }
                let bins = if item == TxnItem::ReadAll {
                    Bins::All
                } else {
                    Bins::from([self.first_bin_name.as_str()])
                };
                let (status, duration) = self
                    .timed_execution(self.client.get(&self.args.read_policy, &key, bins))
                    .await;
                results.push((status, duration, OpType::Read));
            }
            TxnItem::BatchRead(n) => {
                let keys: Vec<Key> = (0..n).map(|_| self.random_key(rng)).collect();
                build_batch_read_ops(
                    &keys,
                    &self.args.batch_read_policy,
                    Bins::All,
                    &self.args.batch_namespaces,
                    batch_ops,
                );
                let (status, duration) = self
                    .timed_execution(self.client.batch(&self.args.batch_policy, batch_ops.as_slice()))
                    .await;
                results.push((status, duration, OpType::Read));
            }
            TxnItem::UpdateOne | TxnItem::UpdateAll | TxnItem::ReplaceOne | TxnItem::ReplaceAll => {
                let key = self.random_key(rng);
                if self.args.skip_key(&key) {
                    results.push((Status::Success, Duration::ZERO, OpType::Write));
                    return;
                }
                let all = matches!(item, TxnItem::UpdateAll | TxnItem::ReplaceAll);
                let policy = if matches!(item, TxnItem::ReplaceOne | TxnItem::ReplaceAll) {
                    &self.replace_policy
                } else {
                    &self.args.write_policy
                };
                let bin_count = if all { None } else { Some(1) };
                self.args.build_bins(&key, rng, bin_count, bins_buffer);
                let (status, duration) = self
                    .timed_execution(self.client.put(policy, &key, bins_buffer))
                    .await;
                results.push((status, duration, OpType::Write));
            }
            TxnItem::Increment => {
                let key = self.random_key(rng);
                if self.args.skip_key(&key) {
                    results.push((Status::Success, Duration::ZERO, OpType::Write));
                    return;
                }
                let bins = [as_bin!(self.counter_bin_name.as_str(), 1)];
                let (status, duration) = self
                    .timed_execution(self.client.add(&self.increment_policy, &key, &bins))
                    .await;
                results.push((status, duration, OpType::Write));
            }
        }
    }
}

impl Task for TransactionTask {
    async fn execute(
        &self,
        _keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let items = self.spec.items(rng);
        let txn_start = Instant::now();
        for item in items {
            self.run_item(item, rng, results, batch_ops, bins_buffer)
                .await;
        }
        // The whole business transaction is one additional sample; it fails
        // if any of its operations failed.
        let status = results
            .iter()
            .map(|(s, _, _)| *s)
            .fold(Status::Success, |acc, s| match (acc, s) {
                (Status::Error, _) | (_, Status::Error) => Status::Error,
                (Status::Timeout, _) | (_, Status::Timeout) => Status::Timeout,
                _ => Status::Success,
            });
        results.push((status, txn_start.elapsed(), OpType::Txn));
    }
}

// ------ ReadFromFileTask ---------

/// Reads random keys from a preloaded key list (`-F/--key-file`).
pub struct ReadFromFileTask {
    client: Arc<Client>,
    args: Arc<Args>,
    keys: Arc<Vec<Key>>,
}

impl ReadFromFileTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>, keys: Arc<Vec<Key>>) -> Self {
        ReadFromFileTask { client, args, keys }
    }
}

impl Task for ReadFromFileTask {
    async fn execute(
        &self,
        _keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        _bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let key = &self.keys[rng.random_range(0..self.keys.len())];
        if self.args.skip_key(key) {
            results.push((Status::Success, Duration::ZERO, OpType::Read));
            return;
        }
        let (status, duration) = self
            .timed_execution(self.client.get(&self.args.read_policy, key, Bins::All))
            .await;
        results.push((status, duration, OpType::Read));
    }
}

// ------ MRT tasks (multi-record transactions) ---------

/// Attach a fresh server transaction to the given policy.
fn with_txn(policy: &WritePolicy, txn: &Arc<Txn>) -> WritePolicy {
    let mut p = policy.clone();
    p.base_policy.txn = Some(txn.clone());
    p
}

async fn finish_txn(
    client: &Client,
    txn: &Arc<Txn>,
    ok: bool,
    txn_start: Instant,
    results: &mut Vec<(Status, Duration, OpType)>,
) {
    if ok {
        let commit: asResult<_> = client.commit(txn).await;
        results.push((status_of(&commit), txn_start.elapsed(), OpType::Txn));
    } else {
        let _ = client.abort(txn).await;
        results.push((Status::Error, txn_start.elapsed(), OpType::Txn));
    }
}

/// Inserts `--mrt-size` sequential records per server transaction.
pub struct MrtInsertTask {
    client: Arc<Client>,
    args: Arc<Args>,
}

impl MrtInsertTask {
    pub fn new(client: Arc<Client>, args: Arc<Args>) -> Self {
        MrtInsertTask { client, args }
    }
}

impl Task for MrtInsertTask {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let txn = Arc::new(Txn::new());
        let policy = with_txn(&self.args.write_policy, &txn);
        let txn_start = Instant::now();
        let mut ok = true;
        for key in keys {
            self.args.build_bins(key, rng, None, bins_buffer);
            let (status, duration) = self
                .timed_execution(self.client.put(&policy, key, bins_buffer))
                .await;
            if !matches!(status, Status::Success | Status::NotFound) {
                ok = false;
            }
            results.push((status, duration, OpType::Write));
            if !ok {
                break;
            }
        }
        finish_txn(&self.client, &txn, ok, txn_start, results).await;
    }
}

/// Runs `--mrt-size` read/update operations per server transaction
/// (RU and RR workloads).
pub struct MrtReadUpdateTask {
    client: Arc<Client>,
    args: Arc<Args>,
    reads: Percent,
    read_bins_pct: Percent,
    write_bins_pct: Percent,
    first_bin_name: String,
}

impl MrtReadUpdateTask {
    pub fn new(
        client: Arc<Client>,
        reads: Percent,
        read_bins_pct: Percent,
        write_bins_pct: Percent,
        args: Arc<Args>,
    ) -> Self {
        let first_bin_name = format!("{}_{}", args.bin_name_base, 1);
        MrtReadUpdateTask {
            client,
            args,
            reads,
            read_bins_pct,
            write_bins_pct,
            first_bin_name,
        }
    }
}

impl Task for MrtReadUpdateTask {
    async fn execute(
        &self,
        keys: &[Key],
        rng: &mut StdRng,
        results: &mut Vec<(Status, Duration, OpType)>,
        _batch_ops: &mut Vec<BatchOperation>,
        bins_buffer: &mut Vec<Bin>,
    ) {
        results.clear();
        let txn = Arc::new(Txn::new());
        let write_policy = with_txn(&self.args.write_policy, &txn);
        let mut read_policy = self.args.read_policy.clone();
        read_policy.base_policy.txn = Some(txn.clone());

        let txn_start = Instant::now();
        let mut ok = true;
        for key in keys {
            if self.args.skip_key(key) {
                results.push((Status::Success, Duration::ZERO, OpType::Read));
                continue;
            }
            if rng.random_range(0..100u8) < self.reads.as_u8() {
                let multi = rng.random_range(0..100u8) < self.read_bins_pct.as_u8();
                let bins = if multi {
                    Bins::All
                } else {
                    Bins::from([self.first_bin_name.as_str()])
                };
                let (status, duration) = self
                    .timed_execution(self.client.get(&read_policy, key, bins))
                    .await;
                if matches!(status, Status::Error | Status::Timeout) {
                    ok = false;
                }
                results.push((status, duration, OpType::Read));
            } else {
                let multi = rng.random_range(0..100u8) < self.write_bins_pct.as_u8();
                let bin_count = if multi { None } else { Some(1) };
                self.args.build_bins(key, rng, bin_count, bins_buffer);
                let (status, duration) = self
                    .timed_execution(self.client.put(&write_policy, key, bins_buffer))
                    .await;
                if matches!(status, Status::Error | Status::Timeout) {
                    ok = false;
                }
                results.push((status, duration, OpType::Write));
            }
            if !ok {
                break;
            }
        }
        finish_txn(&self.client, &txn, ok, txn_start, results).await;
    }
}
