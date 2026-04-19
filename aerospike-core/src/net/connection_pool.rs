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
use std::sync::Arc;

use crate::errors::{Error, Result};
use crate::net::{Connection, ConnectionState, Host};
use crate::policy::ClientPolicy;
use std::collections::VecDeque;
use std::sync::Mutex;

#[derive(Debug)]
struct SharedQueue {
    connections: Mutex<VecDeque<Connection>>,
    // Total number of connections associated with the queue.
    // These connections may be in flight and not in the queue.
    reserved: Mutex<usize>,
    capacity: usize,
    host: Host,
    policy: ClientPolicy,
    hashed_pass: Option<String>,
}

#[derive(Debug)]
pub struct Queue(Arc<SharedQueue>);

impl Queue {
    /// Creates a connection pool with a fixed capacity.
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let hashed_pass = policy.hashed_pass();
        let shared = SharedQueue {
            connections: Mutex::new(VecDeque::with_capacity(capacity)),
            reserved: Mutex::new(0),
            capacity,
            host,
            policy,
            hashed_pass,
        };
        Queue(Arc::new(shared))
    }

    /// Checks if the queue has capacity for another connection.
    /// If so, it will increase the reserved value by one and return true.
    /// Otherwise, return false.
    pub fn reserve_capacity(&self) -> bool {
        let mut reserved = self
            .0
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *reserved < self.0.capacity {
            *reserved += 1;
            drop(reserved);
            return true;
        }
        false
    }

    /// Decreases the reserved value by one, opening up capacity for more connections.
    #[cfg(test)]
    fn reserved(&self) -> usize {
        let reserved = self.0.reserved.lock().unwrap_or_else(|e| e.into_inner());
        *reserved
    }

    /// Decreases the reserved value by one, opening up capacity for more connections.
    pub fn reduce_capacity(&self) {
        let mut reserved = self
            .0
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *reserved > 0 {
            *reserved -= 1;
        }
    }

    /// Creates a new connection based on the queue's `ClientPolicy`.
    /// It does not check for the capacity of the queue.
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

    /// Takes a connection out of the queue.
    pub fn get(&self) -> Result<PooledConnection> {
        let connection;
        loop {
            let mut connections = self
                .0
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(conn) = connections.pop_front() {
                drop(connections);
                if conn.is_idle() {
                    // let the connection drop and close
                    continue;
                }
                connection = conn;
                break;
            }
            return Err(Error::NoMoreConnections);
        }
        Ok(PooledConnection {
            queue: self.clone(),
            conn: Some(connection),
        })
    }

    /// Puts the connection back into the queue.
    /// Putting back a connection in the queue does not reserve capacity.
    /// You should reserve capacity before putting back the connection in the queue.
    pub fn put_back(&self, conn: Connection) {
        let mut connections = self
            .0
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if conn.state == ConnectionState::Ready && connections.len() < self.0.capacity {
            connections.push_back(conn);
        }
        // otherwise let it drop
    }

    /// Removes all the connections from the queue.
    pub fn clear(&self) {
        let mut connections = self
            .0
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for mut conn in connections.drain(..) {
            conn.close();
        }
    }

    /// Returns the current number of connection in the queue.
    pub fn num_conns(&self) -> usize {
        self.0
            .connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Reserved count: total connections owned by this queue, including
    /// ones currently out on loan to callers. Reaped conns must have
    /// [`reduce_capacity`] called separately to release their slot.
    pub fn reserved_count(&self) -> usize {
        *self
            .0
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Pull every currently-idle connection out of the queue without
    /// blocking — uses `try_lock` so a contended pool returns `None`
    /// and the caller can skip this iteration.
    ///
    /// Ownership of the returned connections transfers to the caller: they
    /// must either put survivors back with [`put_back`] or drop + call
    /// [`reduce_capacity`] for each one that goes away. Non-idle connections
    /// stay in the queue.
    pub fn try_extract_idle(&self) -> Option<Vec<Connection>> {
        let mut connections = self.0.connections.try_lock().ok()?;
        if connections.is_empty() {
            return Some(Vec::new());
        }
        let mut idle = Vec::new();
        let mut kept = VecDeque::with_capacity(connections.len());
        for conn in connections.drain(..) {
            if conn.is_idle() {
                idle.push(conn);
            } else {
                kept.push_back(conn);
            }
        }
        *connections = kept;
        Some(idle)
    }
}

impl Clone for Queue {
    fn clone(&self) -> Self {
        Queue(self.0.clone())
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    num_queues: u8,
    queues: Vec<Queue>,
}

impl ConnectionPool {
    pub fn new(host: Host, policy: ClientPolicy) -> Self {
        let num_conns = policy.max_conns_per_node;
        let num_queues = policy.conn_pools_per_node;
        let queues = ConnectionPool::initialize_queues(num_conns, num_queues, host, policy);
        ConnectionPool { num_queues, queues }
    }

    fn initialize_queues(
        num_conns: usize,
        num_queues: u8,
        host: Host,
        policy: ClientPolicy,
    ) -> Vec<Queue> {
        let num_queues = usize::from(num_queues);
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

    /// Get a connection from one of the internal pools.
    pub fn get(&self, hint: u8) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get()
        } else {
            let mut attempts = self.num_queues;
            let mut i = usize::from(hint % self.num_queues);
            loop {
                let connection = self.queues[i].get();
                i += 1;
                if i >= self.queues.len() {
                    i = 0;
                }
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

    /// If there is a pool with capacity to hold more connections, create a connection
    /// for that queue and return it to the user.
    pub async fn make_conn(&self, hint: usize) -> Result<PooledConnection> {
        let num_queues = usize::from(self.num_queues);
        let mut attempts = self.num_queues;
        let mut i = hint % num_queues;
        loop {
            let queue = &self.queues[i % num_queues];
            i += 1;
            if i >= self.queues.len() {
                i = 0;
            }
            if queue.reserve_capacity() {
                match queue.make_conn().await {
                    Ok(conn) => {
                        return Ok(PooledConnection {
                            queue: queue.clone(),
                            conn: Some(conn),
                        });
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            attempts -= 1;
            if attempts == 0 {
                break;
            }
        }

        Err(Error::ClientError(
            "Could not make a connection for the connection pool".into(),
        ))
    }

    /// Closes the connection pool and clears all the internal queues from connection,
    /// closing then in the process.
    pub fn close(&mut self) {
        for queue in self.queues.drain(..) {
            queue.clear();
        }
    }

    /// Internal queues — exposed so the Node layer can implement
    /// reap-and-refresh semantics that are aware of `min_conns_per_node`.
    pub fn queues(&self) -> &[Queue] {
        &self.queues
    }

    /// Returns sum total of connections inside all the internal queues.
    pub fn num_conns(&self) -> usize {
        let mut sum = 0;
        for q in &self.queues {
            sum += q.num_conns();
        }
        sum
    }

    /// If a connection was dropped in a state that was not [`ConnectionState::Ready`],
    /// this method will try to recover the connection by parsing the rest of the data
    /// and returning the connection to a valid state.
    async fn recover_connection(queue: Queue, mut conn: Connection) {
        let mut r = crate::net::connection::ConnectionRecovery::new(&mut conn);
        r.recover().await;
        if conn.state == ConnectionState::Ready {
            queue.put_back(conn);
            return;
        }

        queue.reduce_capacity();
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    pub queue: Queue,
    pub conn: Option<Connection>,
}

impl PooledConnection {
    pub fn invalidate(&mut self) {
        if let Some(conn) = self.conn.as_mut() {
            conn.close();
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            match conn.state {
                ConnectionState::Closed => self.queue.reduce_capacity(),
                ConnectionState::Ready => self.queue.put_back(conn),
                _ if conn.should_attempt_recovery() => {
                    // need to spawn a new green thread to avoid blocking the current one
                    aerospike_rt::spawn(ConnectionPool::recover_connection(
                        Queue(self.queue.0.clone()),
                        conn,
                    ));
                }
                _ => self.queue.reduce_capacity(),
            }
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

#[cfg(test)]
mod tests {
    use crate::net::Connection;

    use super::{ClientPolicy, ConnectionPool, Host, Queue};

    macro_rules! put_back_with_reserve {
        ($queue:ident, $conn:ident) => {{
            if $queue.reserve_capacity() {
                $queue.put_back($conn);
            }
        }};
    }

    macro_rules! get_and_invalidate {
        ($queue:ident) => {{
            match $queue.get() {
                Ok(mut c) => {
                    c.invalidate();
                    Ok(c)
                }
                Err(e) => Err(e),
            }
        }};
    }

    macro_rules! pool_get_and_invalidate {
        ($pool:ident, $hint:tt) => {{
            match $pool.get($hint) {
                Ok(mut c) => {
                    c.invalidate();
                    Ok(c)
                }
                Err(e) => Err(e),
            }
        }};
    }

    macro_rules! get_or_make {
        ($pool:ident, $hint:tt) => {{
            match $pool.get($hint) {
                Ok(c) => Ok(c),
                Err(_) => $pool.make_conn($hint).await,
            }
        }};
    }

    #[aerospike_macro::test]
    async fn queue() {
        let host = Host::new("some-url", 30000);
        let policy = ClientPolicy::default();

        let q = Queue::with_capacity(3, host.clone(), policy.clone());
        assert_eq!(q.num_conns(), 0);
        assert_eq!(q.reserved(), 0);
        assert_eq!(q.get().is_err(), true);

        let c = Connection::new(&host, &policy, None)
            .await
            .expect("creating dummy connection failed");
        put_back_with_reserve!(q, c);
        assert_eq!(q.reserved(), 1);
        assert_eq!(q.num_conns(), 1);

        let c = Connection::new(&host, &policy, None)
            .await
            .expect("creating dummy connection failed");
        put_back_with_reserve!(q, c);
        assert_eq!(q.reserved(), 2);
        assert_eq!(q.num_conns(), 2);

        let c = Connection::new(&host, &policy, None)
            .await
            .expect("creating dummy connection failed");
        put_back_with_reserve!(q, c);
        assert_eq!(q.reserved(), 3);
        assert_eq!(q.num_conns(), 3);

        let c = Connection::new(&host, &policy, None)
            .await
            .expect("creating dummy connection failed");
        put_back_with_reserve!(q, c);
        assert_eq!(q.num_conns(), 3);
        assert_eq!(q.reserved(), 3);
        assert_eq!(q.reserve_capacity(), false);

        // drain the queue =====================================

        // remove and drop; the connection goes back into
        // the queue automatically, because it is in ready state.
        assert_eq!(q.get().is_err(), false);
        assert_eq!(q.reserved(), 3);
        assert_eq!(q.num_conns(), 3);

        assert_eq!(get_and_invalidate!(q).is_err(), false);
        assert_eq!(q.reserved(), 2);
        assert_eq!(q.num_conns(), 2);

        assert_eq!(get_and_invalidate!(q).is_err(), false);
        assert_eq!(q.num_conns(), 1);
        assert_eq!(q.reserved(), 1);

        assert_eq!(get_and_invalidate!(q).is_err(), false);
        assert_eq!(q.num_conns(), 0);
        assert_eq!(q.reserved(), 0);

        assert_eq!(get_and_invalidate!(q).is_err(), true);
        assert_eq!(q.num_conns(), 0);
        assert_eq!(q.reserved(), 0);
    }

    #[aerospike_macro::test]
    async fn single_queue_connection_pool() {
        let host = Host::new("some-url", 30000);
        let policy = ClientPolicy::default();

        let p = ConnectionPool::new(host.clone(), policy.clone());
        assert_eq!(p.num_conns(), 0);
        assert_eq!(p.get(0).is_err(), true);

        assert_eq!(get_or_make!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 1);

        // connection was returned to the pool after the above.
        // so the number of connections in the pool is still 1.
        assert_eq!(get_or_make!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 1);

        assert_eq!(p.make_conn(0).await.is_err(), false);
        assert_eq!(p.num_conns(), 2);

        assert_eq!(p.get(0).is_err(), false);
        assert_eq!(p.num_conns(), 2);

        assert_eq!(pool_get_and_invalidate!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 1);
        assert_eq!(p.queues[0].reserved(), 1);

        assert_eq!(pool_get_and_invalidate!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 0);
        assert_eq!(p.queues[0].reserved(), 0);
    }

    #[aerospike_macro::test]
    async fn multi_queue_connection_pool() {
        let host = Host::new("some-url", 30000);
        let policy = ClientPolicy {
            conn_pools_per_node: 2,
            max_conns_per_node: 3,
            ..ClientPolicy::default()
        };

        let p = ConnectionPool::new(host.clone(), policy.clone());
        assert_eq!(p.num_conns(), 0);
        assert_eq!(p.get(0).is_err(), true);

        assert_eq!(get_or_make!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 1);
        assert_eq!(p.queues[0].reserved(), 1);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 0);
        assert_eq!(p.queues[1].num_conns(), 0);

        // connection was returned to the pool after the above.
        // so the number of connections in the pool is still 1.
        assert_eq!(get_or_make!(p, 1).is_err(), false);
        assert_eq!(p.num_conns(), 1);
        assert_eq!(p.queues[0].reserved(), 1);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 0);
        assert_eq!(p.queues[1].num_conns(), 0);

        assert_eq!(p.make_conn(1).await.is_err(), false);
        assert_eq!(p.num_conns(), 2);
        assert_eq!(p.queues[0].reserved(), 1);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        assert_eq!(p.get(0).is_err(), false);
        assert_eq!(p.num_conns(), 2);
        assert_eq!(p.queues[0].reserved(), 1);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        assert_eq!(pool_get_and_invalidate!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 1);
        assert_eq!(p.queues[0].reserved(), 0);
        assert_eq!(p.queues[0].num_conns(), 0);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        assert_eq!(pool_get_and_invalidate!(p, 0).is_err(), false);
        assert_eq!(p.num_conns(), 0);
        assert_eq!(p.queues[0].reserved(), 0);
        assert_eq!(p.queues[0].num_conns(), 0);
        assert_eq!(p.queues[1].reserved(), 0);
        assert_eq!(p.queues[1].num_conns(), 0);

        assert_eq!(pool_get_and_invalidate!(p, 0).is_err(), true);
        assert_eq!(p.num_conns(), 0);
        assert_eq!(p.queues[0].reserved(), 0);
        assert_eq!(p.queues[0].num_conns(), 0);
        assert_eq!(p.queues[1].reserved(), 0);
        assert_eq!(p.queues[1].num_conns(), 0);

        // test for capacity planning

        assert_eq!(p.make_conn(1).await.is_err(), false);
        assert_eq!(p.num_conns(), 1);
        assert_eq!(p.queues[0].reserved(), 0);
        assert_eq!(p.queues[0].num_conns(), 0);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        assert_eq!(p.make_conn(1).await.is_err(), false);
        assert_eq!(p.num_conns(), 2);
        assert_eq!(p.queues[0].reserved(), 1);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        assert_eq!(p.make_conn(1).await.is_err(), false);
        assert_eq!(p.num_conns(), 3);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 2);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        // can't make more, all queues are full
        assert_eq!(p.make_conn(1).await.is_err(), true);
        assert_eq!(p.num_conns(), 3);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 2);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        // can't make more, all queues are full
        let mut c = p.get(0).unwrap();
        // we are at capacity, no more connections can be created
        assert_eq!(p.make_conn(1).await.is_err(), true);
        // but there is one connection in flight
        assert_eq!(p.num_conns(), 2);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 1);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);
        c.reset_state();
        // c should go back to the pool
        drop(c);
        assert_eq!(p.num_conns(), 3);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 2);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);

        // SAME as above, but with a different hint
        // can't make more, all queues are full
        let mut c = p.get(1).unwrap();
        // we are at capacity, no more connections can be created
        assert_eq!(p.make_conn(1).await.is_err(), true);
        // but there is one connection in flight
        assert_eq!(p.num_conns(), 2);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 2);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 0);
        c.reset_state();
        // c should go back to the pool
        drop(c);
        assert_eq!(p.num_conns(), 3);
        assert_eq!(p.queues[0].reserved(), 2);
        assert_eq!(p.queues[0].num_conns(), 2);
        assert_eq!(p.queues[1].reserved(), 1);
        assert_eq!(p.queues[1].num_conns(), 1);
    }
}
