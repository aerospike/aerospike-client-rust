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

use std::ops::{Deref, DerefMut, Drop};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::errors::{Error, ErrorKind, Result};
use crate::net::{Connection, Host};
use crate::policy::ClientPolicy;
use std::sync::Mutex;
use std::collections::VecDeque;
use std::time::Duration;
use aerospike_rt::{Semaphore, OwnedSemaphorePermit};

#[derive(Debug)]
struct IdleConnection(Connection);

#[derive(Debug)]
struct SharedQueue {
    // SYNCHRONOUS LOCK! Do not hold across an await point or it _will_ deadlock.
    connections: Mutex<VecDeque<IdleConnection>>,
    host: Host,
    policy: ClientPolicy,
}

#[derive(Debug, Clone)]
struct Queue(Arc<SharedQueue>, Arc<Semaphore>);

impl Queue {
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let shared = SharedQueue {
            connections: Mutex::new(VecDeque::with_capacity(capacity)),
            host,
            policy,
        };
        Queue(Arc::new(shared), Arc::new(Semaphore::new(capacity)))
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        let Ok(permit) = self.1.clone().try_acquire_owned() else {
            bail!(ErrorKind::Connection(
                "Too many connections".to_string()
            ));
        };
        let mut connections_to_free = Vec::new();
        let mut conn: Option<Connection> = None;
        {
            let mut connections = self.0.connections.lock().unwrap();
            while let Some(IdleConnection(this_conn)) = connections.pop_front() {
                if this_conn.is_idle() {
                    connections_to_free.push(this_conn);
                } else {
                    conn = Some(this_conn);
                    break;
                }
            }
        }

        for mut connection in connections_to_free {
            connection.close().await;
        }

        if conn.is_none() {
            let new_conn = aerospike_rt::timeout(
                Duration::from_secs(5),
                Connection::new(&self.0.host.address(), &self.0.policy),
            )
            .await;

            let Ok(Ok(new_conn)) = new_conn else {
                bail!(ErrorKind::Connection(
                    "Could not open network connection".to_string()
                ));
            };

            conn = Some(new_conn);
        }

        Ok(PooledConnection {
            queue: self.clone(),
            conn,
            _permit: permit,
        })
    }

    pub fn put_back(&self, conn: Connection) {
        let mut connections = self.0.connections.lock().unwrap();
        connections.push_back(IdleConnection(conn));
    }

    pub fn drop_conn(&self, mut conn: Connection) {
        aerospike_rt::spawn(async move { conn.close().await });
    }

    pub async fn clear(&mut self) {
        let connections = {
            let mut connections = self.0.connections.lock().unwrap();
            std::mem::take(connections.deref_mut())
        };
        for mut conn in connections {
            conn.0.close().await;
        }
    }
}


#[derive(Debug)]
pub struct ConnectionPool {
    num_queues: usize,
    queues: Vec<Queue>,
    queue_counter: AtomicUsize,
}

impl ConnectionPool {
    pub fn new(host: Host, policy: ClientPolicy) -> Self {
        let num_conns = policy.max_conns_per_node;
        let num_queues = policy.conn_pools_per_node;
        let queues = ConnectionPool::initialize_queues(num_conns, num_queues, host, policy);
        ConnectionPool {
            num_queues,
            queues,
            queue_counter: AtomicUsize::default(),
        }
    }

    fn initialize_queues(
        num_conns: usize,
        num_queues: usize,
        host: Host,
        policy: ClientPolicy,
    ) -> Vec<Queue> {
        let max = num_conns / num_queues;
        let mut rem = num_conns % num_queues;
        let mut queues = Vec::with_capacity(num_queues);
        for _ in 0..num_queues {
            let mut capacity = max;
            if rem > 0 {
                capacity += 1;
                rem -= 1;
            }
            queues.push(Queue::with_capacity(capacity, host.clone(), policy.clone()));
        }
        queues
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get().await
        } else {
            let mut attempts = self.num_queues;
            loop {
                let i = self.queue_counter.fetch_add(1, Ordering::Relaxed);
                let connection = self.queues[i % self.num_queues].get().await;
                if let Err(Error(ErrorKind::NoMoreConnections, _)) = connection {
                    attempts -= 1;
                    if attempts > 0 {
                        continue;
                    }
                }
                return connection;
            }
        }
    }

    pub async fn close(&mut self) {
        for mut queue in self.queues.drain(..) {
            queue.clear().await;
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    queue: Queue,
    pub conn: Option<Connection>,
    _permit: OwnedSemaphorePermit,
}

impl PooledConnection {
    pub fn invalidate(mut self) {
        let conn = self.conn.take().unwrap();
        self.queue.drop_conn(conn);
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.queue.put_back(conn);
        }
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().unwrap()
    }
}
