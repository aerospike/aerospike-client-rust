// Copyright 2015-2017 Aerospike, Inc.
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
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use errors::*;
use net::{Connection, Host};
use policy::ClientPolicy;

#[derive(Debug)]
struct IdleConnection {
    conn: Connection,
}

#[derive(Debug)]
struct SharedPool {
    host: Host,
    connections: RwLock<VecDeque<IdleConnection>>,
    connection_count: AtomicUsize,
    client_policy: ClientPolicy,
}

#[derive(Debug)]
pub struct ConnectionPool(Arc<SharedPool>);

impl ConnectionPool {
    pub fn new(host: Host, client_policy: ClientPolicy) -> Self {
        let pool_size = client_policy.connection_pool_size_per_node;
        let connections = RwLock::new(VecDeque::with_capacity(pool_size));
        let shared = SharedPool {
            host: host,
            connections: connections,
            connection_count: AtomicUsize::new(0),
            client_policy: client_policy,
        };
        let shared = Arc::new(shared);
        ConnectionPool(shared)
    }

    pub fn get(&self, timeout: Option<Duration>) -> Result<PooledConnection> {
        let mut connections = self.0.connections.write().unwrap();
        let connection;
        loop {
            match connections.pop_front() {
                Some(IdleConnection { mut conn }) => {
                    {
                        if conn.is_idle() {
                            conn.close();
                            self.dec_connections();
                            continue;
                        }
                        try!(conn.set_timeout(timeout));
                    }
                    connection = conn;
                    break;
                }
                None => {
                    if self.inc_connections() > self.0.client_policy.connection_pool_size_per_node {
                        // too many connections, undo
                        self.dec_connections();
                        bail!("Exceeded max. connection pool size of {}",
                              self.0.client_policy.connection_pool_size_per_node);
                    }

                    let conn = match Connection::new(&self.0.host, &self.0.client_policy) {
                        Ok(c) => c,
                        Err(e) => {
                            self.dec_connections();
                            return Err(e);
                        }
                    };

                    if let Err(e) = conn.set_timeout(timeout) {
                        self.dec_connections();
                        return Err(e);
                    }

                    connection = conn;
                    break;
                }
            }
        }

        Ok(PooledConnection {
            pool: self.clone(),
            conn: Some(connection),
        })
    }

    pub fn put(&self, mut conn: Connection) {
        let mut connections = self.0.connections.write().unwrap();
        if connections.len() < self.0.client_policy.connection_pool_size_per_node {
            connections.push_back(IdleConnection { conn: conn });
        } else {
            conn.close();
            self.dec_connections();
        }
    }

    pub fn close(&self) {
        let mut connections = self.0.connections.write().unwrap();
        connections.clear();
    }

    pub fn dec_connections(&self) -> usize {
        self.0.connection_count.fetch_sub(1, Ordering::Relaxed)
    }

    fn inc_connections(&self) -> usize {
        self.0.connection_count.fetch_add(1, Ordering::Relaxed)
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        ConnectionPool(self.0.clone())
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    pool: ConnectionPool,
    pub conn: Option<Connection>,
}

impl PooledConnection {
    pub fn invalidate(mut self) {
        self.conn.take().unwrap().close()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.put(conn);
        } else {
            self.pool.dec_connections();
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
