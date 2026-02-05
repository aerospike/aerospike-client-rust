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

use crate::errors::{Error, Result};
use crate::net::{Connection, ConnectionState, Host};
use crate::policy::ClientPolicy;
use aerospike_rt::Mutex;
use std::collections::VecDeque;

#[derive(Debug)]
struct IdleConnection(Connection);

#[derive(Debug)]
struct QueueInternals {
    connections: VecDeque<IdleConnection>,
    num_conns: usize,
}

#[derive(Debug)]
struct SharedQueue {
    internals: Mutex<QueueInternals>,
    capacity: usize,
    host: Host,
    policy: ClientPolicy,
    hashed_pass: Option<String>,
}

#[derive(Debug)]
struct Queue(Arc<SharedQueue>);

impl Queue {
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let internals = QueueInternals {
            connections: VecDeque::with_capacity(capacity),
            num_conns: 0,
        };

        let hashed_pass = policy.hashed_pass();

        let shared = SharedQueue {
            internals: Mutex::new(internals),
            capacity,
            host,
            policy,
            hashed_pass,
        };
        Queue(Arc::new(shared))
    }

    pub async fn make_conn(&self) -> Result<Connection> {
        let conn = aerospike_rt::timeout(
            self.0.policy.timeout(),
            Connection::new(&self.0.host, &self.0.policy, self.0.hashed_pass.as_ref()),
        )
        .await;

        if let Ok(Ok(conn)) = conn {
            return Ok(conn);
        }
        Err(Error::Connection(
            "Could not open network connection".to_string(),
        ))
    }

    pub async fn get(&self, total_conns: usize) -> Result<PooledConnection> {
        let mut internals = self.0.internals.lock().await;
        let connection;
        loop {
            if let Some(IdleConnection(mut conn)) = internals.connections.pop_front() {
                if conn.is_idle() {
                    if total_conns > internals.num_conns {
                        internals.num_conns -= 1;
                        conn.close();
                    }
                    continue;
                }
                connection = conn;
                break;
            }

            if internals.num_conns >= self.0.capacity
                || (self.0.policy.max_conns_per_node > 0
                    && total_conns >= self.0.policy.max_conns_per_node)
            {
                return Err(Error::NoMoreConnections);
            }

            // Free the lock to prevent deadlocking
            drop(internals);

            let conn = self.make_conn().await?;

            let mut internals = self.0.internals.lock().await;
            internals.num_conns += 1;
            drop(internals);

            connection = conn;
            break;
        }

        Ok(PooledConnection {
            queue: self.clone(),
            conn: Some(connection),
        })
    }

    pub async fn put_back(&self, mut conn: Connection) {
        let mut internals = self.0.internals.lock().await;
        if conn.state == ConnectionState::Ready && internals.num_conns < self.0.capacity {
            internals.connections.push_back(IdleConnection(conn));
        } else {
            conn.close();
            internals.num_conns -= 1;
        }
    }

    pub async fn drop_conn(&self, mut conn: Connection) {
        {
            let mut internals = self.0.internals.lock().await;
            internals.num_conns -= 1;
        }
        conn.close();
    }

    pub async fn clear(&self) {
        let mut internals = self.0.internals.lock().await;
        for mut conn in internals.connections.drain(..) {
            conn.0.close();
        }
        internals.num_conns = 0;
    }

    pub async fn num_conns(&self) -> usize {
        self.0.internals.lock().await.num_conns
    }
}

impl Clone for Queue {
    fn clone(&self) -> Self {
        Queue(self.0.clone())
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

    pub async fn get(&self, total_conns: usize) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get(total_conns).await
        } else {
            let mut attempts = self.num_queues;
            loop {
                let i: usize = self.queue_counter.fetch_add(1, Ordering::Relaxed);
                let connection = self.queues[i % self.num_queues].get(total_conns).await;
                if matches!(connection, Err(Error::NoMoreConnections)) {
                    attempts -= 1;
                    if attempts > 0 {
                        continue;
                    }
                }
                return connection;
            }
        }
    }

    pub async fn make_conn(&self) -> Result<()> {
        let mut attempts = self.num_queues;
        loop {
            let i = self.queue_counter.fetch_add(1, Ordering::Relaxed);
            let queue = &self.queues[i % self.num_queues];
            if queue.0.internals.lock().await.num_conns >= queue.0.capacity {
                attempts -= 1;
                if attempts <= 0 {
                    break;
                }
                continue;
            }

            let conn = queue.make_conn().await?;
            queue.put_back(conn).await;
            queue.0.internals.lock().await.num_conns += 1;
            return Ok(());
        }

        Err(Error::ClientError(
            "Could not make a connection for the connection pool".into(),
        ))
    }

    pub async fn close(&mut self) {
        for queue in self.queues.drain(..) {
            queue.clear().await;
        }
    }

    pub async fn num_conns(&self) -> usize {
        let mut sum = 0;
        for q in &self.queues {
            sum += q.num_conns().await;
        }
        sum
    }

    async fn recover_connection(queue: Queue, mut conn: Connection) {
        conn.recover_connection().await;
        if conn.state == ConnectionState::Ready {
            queue.put_back(conn).await;
        } else {
            queue.drop_conn(conn).await;
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    queue: Queue,
    pub conn: Option<Connection>,
}

impl PooledConnection {
    pub async fn invalidate(mut self) {
        let conn = self.conn.take().unwrap();
        self.queue.drop_conn(conn).await;
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // need to spawn a new green thread to avoid blocking the current one
        if let Some(conn) = self.conn.take() {
            aerospike_rt::spawn(ConnectionPool::recover_connection(
                Queue(self.queue.0.clone()),
                conn,
            ));
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
