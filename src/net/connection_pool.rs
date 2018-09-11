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

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut, Drop};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use errors::*;
use net::{Connection, Host};
use policy::ClientPolicy;

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
}

#[derive(Debug)]
struct Queue(Arc<SharedQueue>);

impl Queue {
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let internals = QueueInternals {
            connections: VecDeque::with_capacity(capacity),
            num_conns: 0,
        };
        let shared = SharedQueue {
            internals: Mutex::new(internals),
            capacity: capacity,
            host: host,
            policy: policy,
        };
        Queue(Arc::new(shared))
    }

    pub fn get(&self, timeout: Option<Duration>) -> Result<PooledConnection> {
        let mut internals = self.0.internals.lock();
        let connection;
        loop {
            match internals.connections.pop_front() {
                Some(IdleConnection(mut conn)) => {
                    if conn.is_idle() {
                        internals.num_conns -= 1;
                        conn.close();
                        continue;
                    }
                    connection = conn;
                    break;
                }
                None => {
                    if internals.num_conns >= self.0.capacity {
                        bail!(ErrorKind::NoMoreConnections);
                    }
                    let conn = Connection::new(&self.0.host, &self.0.policy)?;
                    internals.num_conns += 1;
                    connection = conn;
                    break;
                }
            }
        }

        connection.set_timeout(timeout).or_else(|err| {
            internals.num_conns -= 1;
            Err(err)
        })?;

        Ok(PooledConnection {
            queue: self.clone(),
            conn: Some(connection),
        })
    }

    pub fn put_back(&self, mut conn: Connection) {
        let mut internals = self.0.internals.lock();
        if internals.num_conns < self.0.capacity {
            internals.connections.push_back(IdleConnection(conn));
        } else {
            conn.close();
            internals.num_conns -= 1;
        }
    }

    pub fn drop_conn(&self, mut conn: Connection) {
        {
            let mut internals = self.0.internals.lock();
            internals.num_conns -= 1;
        }
        conn.close();
    }

    pub fn clear(&mut self) {
        let mut internals = self.0.internals.lock();
        for mut conn in internals.connections.drain(..) {
            conn.0.close();
        }
        internals.num_conns = 0;
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
            num_queues: num_queues,
            queues: queues,
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

    pub fn get(&self, timeout: Option<Duration>) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get(timeout)
        } else {
            let mut attempts = self.num_queues;
            loop {
                let i = self.queue_counter.fetch_add(1, Ordering::Relaxed);
                let connection = self.queues[i % self.num_queues].get(timeout);
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

    pub fn close(&mut self) {
        for mut queue in self.queues.drain(..) {
            queue.clear();
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    queue: Queue,
    pub conn: Option<Connection>,
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
