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

use std::sync::Arc;

use rand::rngs::StdRng;
use rand::Rng;

use aerospike::Result as asResult;
use aerospike::{Bins, Client, Error, Key, ReadPolicy, ResultCode, WritePolicy};

use crate::args::Args;
use crate::batch_ops::{build_batch_read_ops, build_batch_write_ops};
use crate::percent::Percent;

pub enum Status {
    Success,
    Error,
    Timeout,
}

pub enum TaskType {
    Insert(InsertTask),
    ReadUpdate(ReadUpdateTask),
}

impl TaskType {
    pub async fn execute(&self, keys: &[Key], rng: &mut StdRng) -> Status {
        match self {
            TaskType::Insert(task) => task.execute(keys, rng).await,
            TaskType::ReadUpdate(task) => task.execute(keys, rng).await,
        }
    }
}

trait Task: Send {
    async fn execute(&self, keys: &[Key], rng: &mut StdRng) -> Status;

    fn status(&self, result: asResult<()>) -> Status {
        match result {
            Err(Error::ServerError(ResultCode::Timeout, _, _) | Error::Timeout(_)) => {
                Status::Timeout
            }
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => Status::Success,
            Err(_) => Status::Error,
            _ => Status::Success,
        }
    }
}

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
    async fn execute(&self, keys: &[Key], rng: &mut StdRng) -> Status {
        let key = &keys[0];
        let bins = self.args.build_bins(key, rng, None);
        trace!("Inserting {}", key);
        self.status(self.client.put(&self.policy, key, &bins).await)
    }
}

pub struct ReadUpdateTask {
    client: Arc<Client>,
    rpolicy: ReadPolicy,
    wpolicy: WritePolicy,
    reads: Percent,
    read_bins_pct: Percent,
    write_bins_pct: Percent,
    args: Arc<Args>,
}

impl ReadUpdateTask {
    pub fn new(
        client: Arc<Client>,
        reads: Percent,
        read_bins_pct: Percent,
        write_bins_pct: Percent,
        args: Arc<Args>,
    ) -> Self {
        ReadUpdateTask {
            client,
            rpolicy: ReadPolicy::default(),
            wpolicy: WritePolicy::default(),
            reads,
            read_bins_pct,
            write_bins_pct,
            args,
        }
    }
}

impl Task for ReadUpdateTask {
    async fn execute(&self, keys: &[Key], rng: &mut StdRng) -> Status {
        if rng.gen_range(0..100u8) < self.reads.as_u8() {
            self.execute_read(keys, rng).await
        } else {
            self.execute_write(keys, rng).await
        }
    }
}

impl ReadUpdateTask {
    async fn execute_read(&self, keys: &[Key], rng: &mut StdRng) -> Status {
        let multi_bins_read = rng.gen_range(0..100u8) < self.read_bins_pct.as_u8();

        if keys.is_empty() {
            return Status::Success;
        }
        match keys.len() {
            1 => {
                let key = &keys[0];
                if multi_bins_read {
                    trace!("Reading all bins {}", key);
                    self.status(
                        self.client
                            .get(&self.rpolicy, key, Bins::All)
                            .await
                            .map(|_| ()),
                    )
                } else {
                    let single_bin = format!("{}_{}", self.args.bin_name_base, 1);
                    trace!("Reading single bin {} {}", single_bin, key);
                    self.status(
                        self.client
                            .get(&self.rpolicy, &keys[0], Bins::from([single_bin.as_str()]))
                            .await
                            .map(|_| ()),
                    )
                }
            }
            _ => {
                // batch read
                trace!("Batch Reads ");
                let ops = build_batch_read_ops(keys, &self.args.batch_read_policy, Bins::All);
                self.status(
                    self.client
                        .batch(&self.args.batch_policy, &ops)
                        .await
                        .map(|_| ()),
                )
            }
        }
    }

    async fn execute_write(&self, keys: &[Key], rng: &mut StdRng) -> Status {
        let multi_bins_write = rng.gen_range(0..100u8) < self.write_bins_pct.as_u8();

        if keys.is_empty() {
            return Status::Success;
        }

        match keys.len() {
            1 => {
                let key = &keys[0];
                if multi_bins_write {
                    let multi_bins = self.args.build_bins(key, rng, None);
                    trace!("Writing all bins {}", key);
                    self.status(self.client.put(&self.wpolicy, key, &multi_bins).await)
                } else {
                    let first_bin = self.args.build_bins(key, rng, Some(1));
                    trace!("Writing first bin {}", key);
                    self.status(self.client.put(&self.wpolicy, key, &first_bin[..1]).await)
                }
            }
            _ => {
                // batch write
                let ops = build_batch_write_ops(keys, &self.args, rng, multi_bins_write);
                self.status(
                    self.client
                        .batch(&self.args.batch_policy, &ops)
                        .await
                        .map(|_| ()),
                )
            }
        }
    }
}
