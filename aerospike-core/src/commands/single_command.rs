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
use aerospike_rt::time::Instant;

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

    pub async fn execute(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        // `total_timeout` is enforced inside the retry loop itself: per-IO
        // via `Connection::deadline()`, on the inline connect and the
        // pool-empty wait via the deadline passed to `Node::get_connection`,
        // and on the retry sleep by the pre-sleep deadline check. An outer
        // wrapper here would just duplicate that under the global runtime
        // time-driver mutex, arming a timer-wheel entry for every command.
        Self::execute_command(policy, cmd).await
    }

    pub async fn execute_command(
        policy: &(dyn Policy + Send + Sync),
        cmd: &'a mut (dyn commands::Command + Send),
    ) -> Result<()> {
        let mut iterations = 0;
        let mut pool_empty_waits = 0;
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

            let mut conn = match node.get_connection(cmd.hint(), deadline).await {
                Ok(conn) => conn,
                Err(err)
                    if err.is_pool_empty()
                        && pool_empty_waits < commands::POOL_EMPTY_MAX_WAITS =>
                {
                    // Every connection this node is allowed exists and is in
                    // flight; one of the tasks holding them will return one.
                    // This is a pacing wait, not a failure: it consumes
                    // neither the retry budget (`iterations` is rolled back,
                    // so writes with `max_retries == 0` still succeed on a
                    // busy pool) nor the node's error-rate breaker (a busy
                    // pool is not a broken node, and tripping the breaker
                    // here would turn the shortage into instant
                    // `MaxErrorRate` refusals). Bounded by the command
                    // deadline (checked at the loop top) and by
                    // `POOL_EMPTY_MAX_WAITS` for deadline-less commands.
                    // Deliberately not recorded into `last_err`: thousands
                    // of waits must not bury the terminal error.
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

            conn.set_socket_timeout(deadline, policy.socket_timeout());
            conn.set_timeout_delay(cmd.can_recover_connection(), policy.timeout_delay());

            cmd.prepare_buffer(&mut conn).await.map_err(|e| {
                // An argument the client refuses to encode is the caller's
                // mistake, not a buffer problem: surface it as-is so the caller
                // sees `InvalidArgument` and its message, the way Java throws
                // PARAMETER_ERROR straight out of the command. Anything else
                // (I/O, sizing) gets the buffer context, which is where it is
                // useful.
                if matches!(e, Error::InvalidArgument(_)) {
                    e
                } else {
                    e.chain_error("Failed to prepare send buffer")
                }
            })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use aerospike_rt::time::Duration;

    use crate::cluster::node_validator::NodeValidator;
    use crate::net::Host;
    use crate::policy::{BasePolicy, ClientPolicy};
    use crate::Version;

    fn test_node(policy: ClientPolicy) -> Arc<Node> {
        let nv = Arc::new(NodeValidator {
            name: "test-node".to_string(),
            aliases: vec![Host::new("127.0.0.1", 3000)],
            services: vec![],
            address: "127.0.0.1:3000".to_string(),
            client_policy: policy.clone(),
            use_new_info: true,
            version: Version::default(),
        });
        Arc::new(Node::new(policy, nv))
    }

    /// A command stub that always routes to one node and succeeds at every
    /// step past acquisition, so the tests exercise exactly the acquisition
    /// arm of the retry loop. `node_calls` counts loop passes.
    struct PoolWaitProbe {
        node: Arc<Node>,
        node_calls: usize,
    }

    #[async_trait::async_trait]
    impl commands::Command for PoolWaitProbe {
        fn hint(&self) -> u8 {
            0
        }
        async fn write_timeout(&mut self, _conn: &mut Connection) -> Result<()> {
            Ok(())
        }
        async fn prepare_buffer(&mut self, _conn: &mut Connection) -> Result<()> {
            Ok(())
        }
        async fn get_node(&mut self) -> Result<Arc<Node>> {
            self.node_calls += 1;
            Ok(self.node.clone())
        }
        async fn parse_result(&mut self, _conn: &mut Connection) -> Result<()> {
            Ok(())
        }
        async fn write_buffer(&mut self, _conn: &mut Connection) -> Result<()> {
            Ok(())
        }
        fn can_retry(&mut self) -> bool {
            true
        }
        fn can_recover_connection(&mut self) -> bool {
            false
        }
    }

    /// Saturate a single-connection node: the returned borrow keeps the pool
    /// at capacity until it is dropped.
    async fn saturated_node(max_error_rate: usize) -> (Arc<Node>, crate::net::PooledConnection) {
        let policy = ClientPolicy {
            max_conns_per_node: 1,
            conn_pools_per_node: 1,
            max_error_rate,
            ..ClientPolicy::default()
        };
        let node = test_node(policy);
        let held = node
            .get_connection(0, None)
            .await
            .expect("first borrow saturates the pool");
        (node, held)
    }

    /// A command that cannot get a connection waits for its deadline instead
    /// of failing instantly — and the wait is paced, feeds neither the
    /// error-rate breaker nor the retry budget, and ends in a clean timeout.
    #[aerospike_macro::test]
    async fn pool_empty_waits_until_the_deadline_without_spinning() {
        // max_error_rate 1: a single stray incr_error_rate would trip the
        // breaker and change the terminal error, failing this test.
        let (node, _held) = saturated_node(1).await;
        let mut cmd = PoolWaitProbe {
            node: node.clone(),
            node_calls: 0,
        };
        let policy = BasePolicy {
            total_timeout: 200,
            max_retries: 0,
            ..BasePolicy::default()
        };

        let start = Instant::now();
        let err = SingleCommand::execute_command(&policy, &mut cmd)
            .await
            .expect_err("nothing ever returns a connection");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, Error::Timeout(_)),
            "the wait must end in a clean timeout, got: {:?}", err
        );
        assert!(
            elapsed >= Duration::from_millis(150),
            "must wait for the deadline, not fail instantly: {:?}", elapsed
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "must not overshoot the deadline: {:?}", elapsed
        );
        // Paced at POOL_EMPTY_WAIT: ~200 passes in 200ms, not an unbounded
        // spin. The bound is deliberately loose to stay CI-safe.
        assert!(
            cmd.node_calls <= 500,
            "acquisition must be paced, saw {} loop passes",
            cmd.node_calls
        );
        assert_eq!(
            node.error_rate_count(),
            0,
            "a pool wait must not count toward the node error rate"
        );
        assert!(node.validate_error_count().is_ok());
    }

    /// The wait ends as soon as another task returns its connection, and a
    /// write with `max_retries == 0` still succeeds — the wait consumed no
    /// retry budget.
    #[aerospike_macro::test]
    async fn pool_empty_wait_ends_when_a_connection_is_returned() {
        let (node, held) = saturated_node(1).await;
        let mut cmd = PoolWaitProbe {
            node: node.clone(),
            node_calls: 0,
        };
        let policy = BasePolicy {
            total_timeout: 5_000,
            max_retries: 0,
            ..BasePolicy::default()
        };

        aerospike_rt::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            // Ready state at drop -> put_back: the common release path.
            drop(held);
        });

        let start = Instant::now();
        SingleCommand::execute_command(&policy, &mut cmd)
            .await
            .expect("must succeed once a connection is returned");
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(40),
            "should have waited for the release: {:?}", elapsed
        );
        assert_eq!(
            node.error_rate_count(),
            0,
            "a pool wait is not a node error"
        );
    }

    /// With the outer timeout wrapper gone, `execute` (the public entry) must
    /// still fail at roughly `total_timeout` when the command stalls inside
    /// acquisition — proving the retry loop's own bounds enforce the deadline
    /// end-to-end.
    #[aerospike_macro::test]
    async fn execute_without_wrapper_still_enforces_total_timeout() {
        let (node, _held) = saturated_node(1).await;
        let mut cmd = PoolWaitProbe {
            node: node.clone(),
            node_calls: 0,
        };
        let policy = BasePolicy {
            total_timeout: 200,
            max_retries: 2,
            ..BasePolicy::default()
        };

        let start = Instant::now();
        let err = SingleCommand::execute(&policy, &mut cmd)
            .await
            .expect_err("nothing ever returns a connection");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, Error::Timeout(_)),
            "must fail with a timeout, got: {:?}",
            err
        );
        assert!(
            elapsed >= Duration::from_millis(150) && elapsed < Duration::from_secs(2),
            "must fail at roughly the deadline: {:?}",
            elapsed
        );
    }

    /// Without a deadline (`total_timeout == 0`), the wait is bounded by
    /// POOL_EMPTY_MAX_WAITS instead of running forever, and the terminal
    /// error surfaces the pool state.
    #[aerospike_macro::test]
    async fn pool_empty_wait_is_bounded_without_a_deadline() {
        let (node, _held) = saturated_node(100).await;
        let mut cmd = PoolWaitProbe {
            node: node.clone(),
            node_calls: 0,
        };
        let policy = BasePolicy {
            total_timeout: 0,
            max_retries: 0,
            ..BasePolicy::default()
        };

        let start = Instant::now();
        let err = SingleCommand::execute_command(&policy, &mut cmd)
            .await
            .expect_err("nothing ever returns a connection");
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_secs(3),
            "the wait cap must be reached, not fail fast: {:?}", elapsed
        );
        assert!(
            elapsed < Duration::from_secs(30),
            "the wait cap must terminate the wait: {:?}", elapsed
        );
        assert!(
            err.is_pool_empty(),
            "after the cap the pool state is the terminal error, got: {:?}", err
        );
    }
}
