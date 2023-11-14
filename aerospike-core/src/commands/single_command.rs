// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Add;
use std::sync::{Arc, Weak};

use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{self};
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::Connection;
use crate::policy::Policy;
use crate::Key;
use aerospike_rt::sleep;
use aerospike_rt::time::Instant;

pub struct SingleCommand<'a> {
    cluster: Arc<Cluster>,
    pub key: &'a Key,
    partition: Partition<'a>,
    last_tried: Weak<Node>,
    replica: crate::policy::Replica,
}

pub async fn try_with_timeout<O, F: futures::Future<Output = Result<O>>> (deadline: Option<Instant>, future: F) -> Result<O> {
    if let Some(deadline) = deadline {
        aerospike_rt::timeout_at(deadline, future).await.unwrap_or_else(
            move |_|{
                Err(ErrorKind::Connection("Timeout".to_string()).into())
        })
    } else {
        future.await
    }
}

impl<'a> SingleCommand<'a> {
    pub fn new(cluster: Arc<Cluster>, key: &'a Key, replica: crate::policy::Replica,) -> Self {
        let partition = Partition::new_by_key(key);
        SingleCommand {
            cluster,
            key,
            partition,
            last_tried: Weak::new(),
            replica,
        }
    }

    pub fn get_node(&mut self) -> Result<Arc<Node>> {
        let this_time = self.cluster.get_node(&self.partition, self.replica, self.last_tried.clone())?;
        self.last_tried = Arc::downgrade(&this_time);
        Ok(this_time)
    }

    pub async fn empty_socket(conn: &mut Connection) -> Result<()> {
        // There should not be any more bytes.
        // Empty the socket to be safe.
        let sz = conn.buffer.read_i64(None);
        let header_length = i64::from(conn.buffer.read_u8(None));
        let receive_size = ((sz & 0xFFFF_FFFF_FFFF) - header_length) as usize;

        // Read remaining message bytes.
        if receive_size > 0 {
            conn.buffer.resize_buffer(receive_size)?;
            conn.read_buffer(receive_size).await?;
        }

        Ok(())
    }

    // EXECUTE
    //

    pub async fn execute(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        let mut iterations = 0;

        // set timeout outside the loop
        let deadline = policy.deadline();

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                if let Some(sleep_between_retries) = policy.sleep_between_retries() {
                    // check for command timeout
                    if let Some(deadline) = deadline {
                        if Instant::now().add(sleep_between_retries) > deadline {
                            break;
                        }
                    }
        
                    sleep(sleep_between_retries).await;
                } else {
                    // check for command timeout
                    if let Some(deadline) = deadline {
                        if Instant::now() > deadline {
                            break;
                        }
                    }
        
                    // yield to free space for the runtime to execute other futures between runs because the loop would block the thread
                    aerospike_rt::task::yield_now().await;
                }
            }

            // set command node, so when you return a record it has the node
            let node = match cmd.get_node() {
                Ok(node) => node,
                Err(_) => continue, // Node is currently inactive. Retry.
            };

            let mut conn = match try_with_timeout(deadline, node.get_connection()).await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Node {}: {}", node, err);
                    continue;
                }
            };

            cmd.prepare_buffer(&mut conn)
                .chain_err(|| "Failed to prepare send buffer")?;

            try_with_timeout(deadline, cmd.write_timeout(&mut conn, policy.timeout()))
                .await
                .chain_err(|| "Failed to set timeout for send buffer")?;

            // Send command.
            if let Err(err) = try_with_timeout(deadline, cmd.write_buffer(&mut conn)).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {}: {}", node, err);
                continue;
            }

            // Parse results.
            if let Err(err) = try_with_timeout(deadline, cmd.parse_result(&mut conn)).await {
                // close the connection
                // cancelling/closing the batch/multi commands will return an error, which will
                // close the connection to throw away its data and signal the server about the
                // situation. We will not put back the connection in the buffer.
                if !commands::keep_connection(&err) {
                    conn.invalidate();
                }
                return Err(err);
            } else {
                return Ok(());
            }
        }

        bail!(ErrorKind::Connection("Timeout".to_string()))
    }
}
