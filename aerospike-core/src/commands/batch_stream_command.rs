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

use futures::channel::mpsc::Sender;
use futures::SinkExt;

use crate::batch::{BatchOperation, BatchRecord, BatchRecordIndex};
use crate::cluster::{Cluster, Node};
use crate::commands::batch_operate_command::parse_batch_record;
use crate::commands::{self, BatchAttr};
use crate::errors::{Error, Result};
use crate::net::{BufferedConn, Connection};
use crate::policy::{BatchPolicy, Policy};

/// Per-node command for `batch_stream`. Encodes the request, sends it, then streams each
/// parsed record through `sender` as it arrives from the server.
///
/// Unlike `BatchOperateCommand` there is no retry loop — the stream is single-attempt per node.
/// Partial failures (network errors mid-parse) simply close this node's contribution to the
/// stream; records from other nodes are unaffected.
pub struct BatchStreamCommand {
    policy: BatchPolicy,
    node: Arc<Node>,
    /// Original indices (into the shared `all_ops`) this node is responsible for.
    owned_indices: Vec<usize>,
    /// Read-only shared reference to the full operations vec (used only for encoding).
    all_ops: Arc<Vec<BatchOperation>>,
    sender: Sender<(usize, BatchRecord)>,
}

impl BatchStreamCommand {
    pub fn new(
        policy: BatchPolicy,
        node: Arc<Node>,
        owned_indices: Vec<usize>,
        all_ops: Arc<Vec<BatchOperation>>,
        sender: Sender<(usize, BatchRecord)>,
    ) -> Self {
        BatchStreamCommand {
            policy,
            node,
            owned_indices,
            all_ops,
            sender,
        }
    }

    /// Execute the command, streaming results through the sender.
    /// Errors are swallowed — the caller discovers them only by seeing fewer stream items than
    /// expected. The sender is dropped on completion (or error), which signals the channel.
    pub async fn execute(self, cluster: Arc<Cluster>) {
        let _ = self.execute_inner(cluster).await;
    }

    async fn execute_inner(self, _cluster: Arc<Cluster>) -> Result<()> {
        // Destructure so that `sender` can be borrowed mutably while other fields are
        // borrowed immutably at the same time (Sender::send takes &mut self).
        let Self {
            policy,
            node,
            owned_indices,
            all_ops,
            mut sender,
        } = self;

        let hint = owned_indices
            .first()
            .map(|&i| all_ops[i].key().digest[0])
            .unwrap_or(0);

        let mut conn = node.get_connection(hint).await.map_err(|err| {
            warn!("Node {}: {}", node, err);
            err
        })?;

        // Build a borrowed view for encoding — no cloning of BatchOperation.
        let _attr: BatchAttr = conn
            .buffer
            .set_batch_operate(&policy, &all_ops.as_ref(), &owned_indices)
            .map_err(|_| Error::ClientError("Failed to prepare send buffer".into()))?;

        conn.buffer.write_timeout(policy.server_timeout());
        conn.set_socket_timeout(policy.deadline(), policy.socket_timeout());
        conn.set_timeout_delay(true, policy.timeout_delay());

        if let Err(err) = conn.flush().await {
            conn.invalidate();
            return Err(err);
        }

        if let Err(err) = Self::parse_result(&owned_indices, &all_ops, &mut conn, &mut sender).await
        {
            if !crate::commands::keep_connection(&err) {
                conn.invalidate();
            }
            return Err(err);
        }

        conn.reset_state();
        Ok(())
    }

    async fn parse_result(
        owned_indices: &[usize],
        all_ops: &Arc<Vec<BatchOperation>>,
        conn: &mut Connection,
        sender: &mut Sender<(usize, BatchRecord)>,
    ) -> Result<()> {
        let mut status = true;

        while status {
            let mut conn = BufferedConn::new(conn);

            conn.set_limit_header(8)?;
            conn.read_buffer(8).await?;
            let size = conn.buffer().read_msg_size(None);
            conn.bookmark();

            status = false;
            if size > 0 {
                conn.set_limit_body(size)?;
                match Self::parse_group(owned_indices, all_ops, &mut conn, size, sender).await {
                    Ok(stat) => status = stat,
                    Err(e @ Error::ServerError(_, _, _)) => {
                        conn.drain(conn.conn.deadline()).await?;
                        return Err(e);
                    }
                    Err(e) => return Err(e),
                }
            }
            conn.drain(conn.conn.deadline()).await?;
        }

        Ok(())
    }

    async fn parse_group(
        owned_indices: &[usize],
        all_ops: &Arc<Vec<BatchOperation>>,
        conn: &mut BufferedConn<'_>,
        size: usize,
        sender: &mut Sender<(usize, BatchRecord)>,
    ) -> Result<bool> {
        while conn.bytes_received() < size {
            conn.read_buffer(commands::buffer::MSG_REMAINING_HEADER_SIZE as usize)
                .await?;
            match parse_batch_record(conn).await {
                Ok(None) => return Ok(false),
                Ok(Some(BatchRecordIndex {
                    batch_index,
                    record,
                    result_code,
                })) => {
                    let orig = owned_indices[batch_index];
                    let mut br = all_ops[orig].batch_record();
                    br.record = record;
                    br.result_code = Some(result_code);
                    // Ignore send errors: caller dropped the receiver (stream abandoned).
                    let _ = sender.send((orig, br)).await;
                }
                Err(Error::BatchLastError(batch_index, rc, in_doubt, ref msg)) => {
                    let orig = owned_indices[batch_index as usize];
                    let mut br = all_ops[orig].batch_record();
                    br.result_code = Some(rc);
                    br.in_doubt = in_doubt;
                    let _ = sender.send((orig, br)).await;
                    return Err(Error::BatchError(batch_index, rc, in_doubt, msg.clone()));
                }
                Err(Error::BatchError(batch_index, rc, in_doubt, ..)) => {
                    let orig = owned_indices[batch_index as usize];
                    let mut br = all_ops[orig].batch_record();
                    br.result_code = Some(rc);
                    br.in_doubt = in_doubt;
                    let _ = sender.send((orig, br)).await;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(true)
    }
}
