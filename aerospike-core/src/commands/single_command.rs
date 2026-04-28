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
    pub partition: Partition<'a>,
}

impl<'a> SingleCommand<'a> {
    pub const fn new(cluster: Arc<Cluster>, key: &'a Key, partition: Partition<'a>) -> Self {
        SingleCommand {
            cluster,
            key,
            partition,
        }
    }

    pub const fn hint(&self) -> u8 {
        self.key.digest[0]
    }

    pub fn get_node(&mut self) -> Result<Arc<Node>> {
        self.partition.get_node(&self.cluster)
    }

    pub const fn prepare_retry(&mut self, is_client_timeout: bool) {
        self.partition.prepare_retry(is_client_timeout);
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
        let mut iterations: usize = 0;
        // Number of times the command was actually sent on the wire (matches
        // Java's `commandSentCounter`). Used to compute `in_doubt` on failure.
        let mut commands_sent: u32 = 0;
        let iterations_as_u32 =
            |n: usize| if n > u32::MAX as usize { u32::MAX } else { n as u32 };
        let mut last_err: Option<Error> = None;
        let mut sub_errors: Vec<Error> = Vec::new();
        let mut last_node_addr: Option<String> = None;
        let is_write = cmd.is_write();

        // set timeout outside the loop
        let deadline = policy.deadline();

        // Finalizes an error before returning to the caller: applies `in_doubt`
        // per Java's rule and attaches retry context (iteration count, last
        // node, sub-error history).
        let finalize = |err: Error,
                        iterations: u32,
                        commands_sent: u32,
                        last_node_addr: Option<String>,
                        sub_errors: Vec<Error>|
         -> Error {
            err.set_in_doubt(is_write, commands_sent).with_retry_context(
                iterations,
                last_node_addr.as_deref(),
                sub_errors,
            )
        };

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // check for max retries
            if policy.max_retries() > 0 && iterations > policy.max_retries() {
                // first attempt isn't a retry
                let err = Error::Timeout(format!("Timeout after {iterations} tries"));
                let tail = match last_err.take() {
                    Some(e) => e.wrap(err),
                    None => err,
                };
                return Err(finalize(
                    tail,
                    iterations_as_u32(iterations),
                    commands_sent,
                    last_node_addr,
                    sub_errors,
                ));
            }

            // Sleep before trying again, after the first iteration
            if iterations > 1 {
                // DO NOT retry for streaming commands here. They retry in their own execution logic.
                // DO NOT retry for any error other than network errors.
                if !cmd.can_retry() {
                    let err = Error::Timeout("Timeout".to_string());
                    let tail = match last_err.take() {
                        Some(e) => e.wrap(err),
                        None => err,
                    };
                    return Err(finalize(
                        tail,
                        iterations_as_u32(iterations),
                        commands_sent,
                        last_node_addr,
                        sub_errors,
                    ));
                }

                // Advance the partition sequence for the retry. Only treat a
                // client-side timeout as a timeout for partition sequencing —
                // a server-reported TIMEOUT should still advance the sequence.
                let is_client_timeout = matches!(&last_err, Some(Error::Timeout(_)));
                cmd.prepare_retry(is_client_timeout);

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

            // Record the previous iteration's error as a sub-error once we're
            // committing to another attempt.
            if let Some(prev) = last_err.take() {
                sub_errors.push(prev);
            }

            // set command node, so when you return a record it has the node
            let node = match cmd.get_node() {
                Ok(node) => node,
                e @ Err(Error::InvalidArgument(_)) => e?,
                Err(e) => {
                    warn!("Error selecting node from the partition table: {e}");
                    last_err = Some(e);
                    continue;
                } // Node is currently inactive. Retry.
            };
            last_node_addr = Some(node.to_string());

            let mut conn = match node.get_connection(cmd.hint()).await {
                Ok(conn) => conn,
                Err(err) => {
                    warn!("Node {node}: {err}");
                    last_err = Some(err);
                    continue;
                }
            };

            conn.set_socket_timeout(deadline, policy.socket_timeout());
            conn.set_timeout_delay(cmd.can_recover_connection(), policy.timeout_delay());

            conn.buffer.set_compress(policy.use_compression());
            cmd.prepare_buffer(&mut conn)
                .await
                .map_err(|e| e.chain_error("Failed to prepare send buffer"))?;
            cmd.write_timeout(&mut conn)
                .await
                .map_err(|e| e.chain_error("Failed to set timeout for send buffer"))?;

            // Compress the buffer after timeout is written but before sending.
            if policy.use_compression() {
                conn.buffer
                    .compress()
                    .map_err(|e| e.chain_error("Failed to compress send buffer"))?;
            }

            // Send command.
            if let Err(err) = cmd.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {node}: {err}");
                last_err = Some(err);
                continue;
            }
            commands_sent += 1;

            // Parse results.
            if let Err(err) = cmd.parse_result(&mut conn).await {
                // close the connection if the error is not safe to pool
                if !commands::keep_connection(&err) {
                    conn.invalidate();
                }

                // Retry on network errors (client side) and on explicit
                // server-side retriables (TIMEOUT / DEVICE_OVERLOAD / KEY_BUSY
                // / PARTITION_UNAVAILABLE) — matching Java's SyncCommand.
                if commands::should_retry(&err) {
                    last_err = Some(err);
                    continue;
                }

                return Err(finalize(
                    err,
                    iterations_as_u32(iterations),
                    commands_sent,
                    last_node_addr,
                    sub_errors,
                ));
            }

            // allow the connection to be put back in the connection pool
            conn.reset_state();

            // command has completed successfully. Exit method.
            return Ok(());
        }

        let err = Error::Timeout(format!("Command timed out after {iterations} tries"));
        let tail = match last_err.take() {
            Some(e) => e.wrap(err),
            None => err,
        };
        Err(finalize(
            tail,
            iterations_as_u32(iterations),
            commands_sent,
            last_node_addr,
            sub_errors,
        ))
    }
}
