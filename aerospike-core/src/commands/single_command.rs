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

use std::sync::Arc;

use crate::cluster::partition::Partition;
use crate::cluster::{Cluster, Node};
use crate::commands::{self};
use crate::errors::{Error, Result};
use crate::net::Connection;
use crate::policy::Policy;
use crate::Key;
use aerospike_rt::sleep;
use aerospike_rt::time::{Duration, Instant};

pub struct SingleCommand<'a> {
    cluster: Arc<Cluster>,
    pub key: &'a Key,
    partition: Partition<'a>,
    last_tried: Option<Arc<Node>>,
    replica: crate::policy::Replica,
}

impl<'a> SingleCommand<'a> {
    pub fn new(cluster: Arc<Cluster>, key: &'a Key, replica: crate::policy::Replica) -> Self {
        let partition = Partition::new_by_key(key);
        SingleCommand {
            cluster,
            key,
            partition,
            last_tried: None,
            replica,
        }
    }

    pub const fn hint(&self) -> u8 {
        self.key.digest[0]
    }

    pub fn get_node(&mut self) -> Result<Arc<Node>> {
        let node = self
            .cluster
            .get_node(&self.partition, self.replica, self.last_tried.clone())?;

        self.last_tried = Some(node.clone());
        Ok(node)
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
            conn.read_body(receive_size).await?;
        }

        Ok(())
    }

    // EXECUTE
    //

    #[allow(clippy::option_if_let_else)]
    pub async fn execute(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        if policy.total_timeout() > 0 {
            match aerospike_rt::timeout(
                Duration::from_millis(u64::from(policy.total_timeout())),
                Self::execute_command(policy, cmd),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => Err(Error::Timeout("Timeout".to_string())),
            }
        } else {
            Self::execute_command(policy, cmd).await
        }
    }

    pub async fn execute_command(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        let mut iterations = 0;
        // Remember the most recent failure so that when retries are exhausted
        // the caller gets a meaningful error — in particular the circuit
        // breaker's `MaxErrorRate` — instead of a bare timeout. This is what
        // lets the breaker "return an error informing the user it tripped".
        let mut last_err: Option<Error> = None;
        // set timeout outside the loop
        let deadline = policy.deadline();
        let effective_attempt = policy.max_retries() + 1;

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // check for max retries
            if iterations > effective_attempt {
                // first attempt isn't a retry
                return Err(last_err.unwrap_or_else(|| {
                    Error::Timeout(format!("Timeout after {iterations} tries"))
                }));
            }

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                // DO NOT retry for streaming commands here. They retry in their own execution logic.
                // DO NOT retry for any error other than network errors.
                if !cmd.can_retry() {
                    return Err(last_err.unwrap_or_else(|| Error::Timeout("Timeout".to_string())));
                }

                if let Some(sleep_between_retries) = policy.sleep_between_retries() {
                    sleep(sleep_between_retries).await;
                }
            }

            // check for command timeout
            if let Some(deadline) = deadline {
                if Instant::now() > deadline {
                    break;
                }
            }

            // set command node, so when you return a record it has the node
            let node_future = cmd.get_node();
            let node = match node_future.await {
                Ok(node) => node,
                e @ Err(Error::InvalidArgument(_)) => e?,
                Err(e) => {
                    warn!("Error selecting node from the partition table: {e}");
                    continue;
                } // Node is currently inactive. Retry.
            };

            // Per-node circuit breaker: if this node has tripped its
            // error-rate window, refuse the command outright (no socket
            // open, no retry on this node) and let the caller back off.
            // Mirrors Java `SyncCommand.executeCommand` calling
            // `node.validateErrorCount()` before `getConnection`.
            if let Err(err) = node.validate_error_count() {
                last_err = Some(err);
                continue;
            }

            let mut conn = match node.get_connection(cmd.hint()).await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Node {node}: {err}");
                    node.incr_error_rate();
                    last_err = Some(err);
                    continue;
                }
            };

            conn.set_socket_timeout(deadline, policy.socket_timeout());
            conn.set_timeout_delay(cmd.can_recover_connection(), policy.timeout_delay());

            cmd.prepare_buffer(&mut conn)
                .await
                .map_err(|e| e.chain_error("Failed to prepare send buffer"))?;
            cmd.write_timeout(&mut conn)
                .await
                .map_err(|e| e.chain_error("Failed to set timeout for send buffer"))?;

            // Send command.
            if let Err(err) = cmd.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {node}: {err}");
                node.incr_error_rate();
                last_err = Some(err);
                continue;
            }

            // Parse results.
            if let Err(err) = cmd.parse_result(&mut conn).await {
                // close the connection
                // cancelling/closing the batch/multi commands will return an error, which will
                // close the connection to throw away its data and signal the server about the
                // situation. We will not put back the connection in the buffer.
                if !commands::keep_connection(&err) {
                    conn.invalidate();
                }
                if commands::should_retry(&err) {
                    // Bump the per-node breaker for the retriable error subset
                    // Java counts: TIMEOUT, DEVICE_OVERLOAD, KEY_BUSY, plus
                    // client-side network failures.
                    if commands::is_network_error(&err) || commands::is_retriable_server_error(&err)
                    {
                        node.incr_error_rate();
                    }
                    last_err = Some(err);
                    continue;
                }
                return Err(err);
            }

            // allow the connection to be put back in the connection pool
            conn.reset_state();

            // command has completed successfully. Exit method.
            return Ok(());
        }

        Err(last_err.unwrap_or_else(|| {
            Error::Timeout(format!("Command timed out after {iterations} tries"))
        }))
    }
}
