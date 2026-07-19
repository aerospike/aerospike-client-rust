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
use crate::policy::{next_retry_interval, Policy};
use crate::{Key, ResultCode};
use aerospike_rt::sleep;
use aerospike_rt::time::Instant;

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

    /// The cluster this command runs against. Exposed so concrete commands can
    /// surface it through [`Command::cluster`](crate::commands::Command::cluster)
    /// for cluster-wide metrics recording.
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
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
        // `total_timeout` is enforced per-IO via `Connection::deadline()` —
        // an outer wrapper here would just duplicate it under the global
        // Tokio time-driver mutex.
        Self::execute_command(policy, cmd).await
    }

    pub async fn execute_command(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        let mut iterations: usize = 0;
        // Number of times the command was actually sent on the wire (matches
        // Java's `commandSentCounter`). Used to compute `in_doubt` on failure.
        let mut commands_sent: u32 = 0;
        let iterations_as_u32 = |n: usize| {
            if n > u32::MAX as usize {
                u32::MAX
            } else {
                n as u32
            }
        };
        let mut last_err: Option<Error> = None;
        let mut sub_errors: Vec<Error> = Vec::new();
        let mut last_node_addr: Option<String> = None;
        let is_write = cmd.is_write();

        // Metrics: captured once before the retry loop. `cmd_type`/`cmd_namespace`
        // attribute per-command-type and detailed per-namespace metrics;
        // `trans_start` measures total command latency; `last_node` lets the
        // terminal error paths attribute the failure to the node that served it.
        let cmd_type = cmd.command_type();
        let cmd_namespace: Option<String> = cmd.namespace().map(str::to_owned);
        let trans_start = Instant::now();
        let mut last_node: Option<Arc<Node>> = None;
        // Latency metrics are recorded in milliseconds (matching the Java
        // client's histogram units); sub-millisecond phases record 0 and land
        // in the first bucket.
        let millis_since = |start: Instant| start.elapsed().as_millis() as u64;

        // set timeout outside the loop
        let deadline = policy.deadline();
        let effective_attempt = policy.max_retries() + 1;
        // Retry backoff: the sleep interval starts at `sleep_between_retries`
        // and is multiplied by `sleep_multiplier` after each retry sleep
        // (matching the Go client). A multiplier <= 1.0 keeps it constant.
        let sleep_multiplier = policy.sleep_multiplier();
        let mut sleep_interval = policy.sleep_between_retries();
        // Consecutive waits spent on an empty connection pool while a
        // background task opens a connection (not part of the retry budget).
        let mut pool_empty_waits: usize = 0;

        // Finalizes an error before returning to the caller: applies `in_doubt`
        // per Java's rule and attaches retry context (iteration count, last
        // node, sub-error history).
        let finalize = |err: Error,
                        iterations: u32,
                        commands_sent: u32,
                        last_node_addr: Option<String>,
                        sub_errors: Vec<Error>|
         -> Error {
            err.set_in_doubt(is_write, commands_sent)
                .with_retry_context(iterations, last_node_addr.as_deref(), sub_errors)
        };

        // Execute command until successful, timed out or maximum iterations have been reached.
        loop {
            iterations += 1;

            // check for max retries
            if iterations > effective_attempt {
                // first attempt isn't a retry
                if let Some(n) = &last_node {
                    n.metrics().incr_transaction_error();
                }
                if let Some(cluster) = cmd.cluster() {
                    cluster.incr_max_retries_exceeded();
                }
                let err = Error::timeout(format!("Timeout after {iterations} tries"));
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
                    if let Some(n) = &last_node {
                        n.metrics().incr_transaction_error();
                    }
                    let err = Error::timeout("Timeout".to_string());
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
                let is_client_timeout = matches!(&last_err, Some(Error::Timeout { .. }));
                cmd.prepare_retry(is_client_timeout);

                if let Some(interval) = sleep_interval {
                    if let Some(deadline) = deadline {
                        if Instant::now() + interval > deadline {
                            // We will timeout anyway after sleep. break immediately.
                            break;
                        }
                    }
                    sleep(interval).await;
                    sleep_interval = Some(next_retry_interval(interval, sleep_multiplier));
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
            last_node = Some(node.clone());

            // Per-node circuit breaker: if this node has tripped its
            // error-rate window, refuse the command outright (no socket
            // open, no retry on this node) and let the caller back off.
            // Mirrors Java `SyncCommand.executeCommand` calling
            // `node.validateErrorCount()` before `getConnection`.
            if let Err(err) = node.validate_error_count() {
                node.metrics().incr_circuit_breaker_hits();
                last_err = Some(err);
                continue;
            }

            let aq_start = Instant::now();
            let mut conn = match node.get_connection(cmd.hint()).await {
                Ok(conn) => conn,
                Err(Error::ConnectionPoolEmpty)
                    if pool_empty_waits < commands::POOL_EMPTY_MAX_WAITS =>
                {
                    // A background task is opening a connection. This is a
                    // pacing wait, not a failure: it consumes neither the
                    // retry budget (`iterations` is rolled back, so writes
                    // with `max_retries == 0` still succeed on a cold pool)
                    // nor the node's error-rate breaker. Bounded by the
                    // command deadline (checked at the loop top) and by
                    // `POOL_EMPTY_MAX_WAITS` for deadline-less commands.
                    // (Deliberately not recorded into `last_err`: the loop
                    // top drains `last_err` into `sub_errors` every pass, and
                    // thousands of waits must not produce thousands of
                    // sub-error entries.)
                    iterations -= 1;
                    pool_empty_waits += 1;
                    sleep(commands::POOL_EMPTY_WAIT).await;
                    continue;
                }
                Err(err) => {
                    warn!("Node {node}: {err}");
                    node.incr_error_rate();
                    last_err = Some(err);
                    continue;
                }
            };
            // Decide once per attempt whether to record metrics for this
            // command: collection enabled AND the policy's sampler selects it.
            let metrics_on = node.metrics().should_sample(conn.rng());
            if metrics_on {
                if let Some(ns) = cmd_namespace.as_deref() {
                    node.metrics()
                        .record_connection_aq(ns, cmd_type, millis_since(aq_start));
                }
            }

            conn.set_socket_timeout(deadline, policy.socket_timeout());
            conn.set_timeout_delay(cmd.can_recover_connection(), policy.timeout_delay());

            conn.buffer
                .set_compress(policy.use_compression(), policy.compression_threshold());
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
            let bytes_sent = conn.buffer.data_buffer.len() as u64;
            let write_start = Instant::now();
            if let Err(err) = cmd.write_buffer(&mut conn).await {
                // IO errors are considered temporary anomalies. Retry.
                // Close socket to flush out possible garbage. Do not put back in pool.
                conn.invalidate();
                warn!("Node {node}: {err}");
                node.incr_error_rate();
                if metrics_on {
                    node.metrics().incr_transaction_retry();
                }
                last_err = Some(err);
                continue;
            }
            commands_sent += 1;
            if metrics_on {
                if let Some(ns) = cmd_namespace.as_deref() {
                    node.metrics().record_write(
                        ns,
                        cmd_type,
                        bytes_sent,
                        millis_since(write_start),
                    );
                }
            }

            // Parse results.
            let parse_start = Instant::now();
            if let Err(err) = cmd.parse_result(&mut conn).await {
                // close the connection if the error is not safe to pool
                if !commands::keep_connection(&err) {
                    conn.invalidate();
                }

                // Record the server result code (if any) for this attempt.
                if metrics_on {
                    if let (Some(ns), Some(rc)) =
                        (cmd_namespace.as_deref(), err.server_result_code())
                    {
                        node.metrics().record_result_code(ns, cmd_type, rc);
                    }
                }

                // Retry on network errors (client side) and on explicit
                // server-side retriables (TIMEOUT / DEVICE_OVERLOAD / KEY_BUSY
                // / PARTITION_UNAVAILABLE) — matching Java's SyncCommand.
                if commands::should_retry(&err) {
                    // Bump the per-node breaker for the retriable
                    // error subset Java counts: TIMEOUT, DEVICE_OVERLOAD,
                    // KEY_BUSY, plus client-side network failures.
                    if commands::is_network_error(&err) || commands::is_retriable_server_error(&err)
                    {
                        node.incr_error_rate();
                    }
                    if metrics_on {
                        node.metrics().incr_transaction_retry();
                    }
                    last_err = Some(err);
                    continue;
                }

                if metrics_on {
                    node.metrics().incr_transaction_error();
                }
                return Err(finalize(
                    err,
                    iterations_as_u32(iterations),
                    commands_sent,
                    last_node_addr,
                    sub_errors,
                ));
            }

            // Command completed successfully. Record the OK result code, the
            // parse cost / bytes received, and the overall command latency.
            if metrics_on {
                if let Some(ns) = cmd_namespace.as_deref() {
                    node.metrics()
                        .record_result_code(ns, cmd_type, ResultCode::Ok);
                    node.metrics().record_parse(
                        ns,
                        cmd_type,
                        millis_since(parse_start),
                        conn.bytes_read() as u64,
                    );
                }
                node.metrics()
                    .record_command(cmd_type, millis_since(trans_start));
            }

            // allow the connection to be put back in the connection pool
            conn.reset_state();

            // command has completed successfully. Exit method.
            return Ok(());
        }

        if let Some(n) = &last_node {
            n.metrics().incr_transaction_error();
        }
        if let Some(cluster) = cmd.cluster() {
            cluster.incr_total_timeout_exceeded();
        }
        let err = Error::timeout(format!("Command timed out after {iterations} tries"));
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
