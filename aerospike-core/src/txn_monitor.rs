// Copyright 2015-2024 Aerospike, Inc.
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

//! Transaction monitor: manages adding write keys to the MRT monitor record.

use std::sync::Arc;

use crate::cluster::Cluster;
use crate::commands::txn_add_keys_command::TxnAddKeysCommand;
use crate::errors::Result;
use crate::operations::lists::{self, ListOrderType, ListPolicy, ListWriteFlags};
use crate::operations::scalar;
use crate::operations::Operation;
use crate::policy::{BasePolicy, Expiration, WritePolicy};
use crate::txn::{get_txn_monitor_key, Txn};
use crate::{Bin, Key, Value};

const BIN_NAME_ID: &str = "id";
const BIN_NAME_DIGESTS: &str = "keyds";

const fn txn_ordered_list_policy() -> ListPolicy {
    ListPolicy {
        attributes: ListOrderType::Ordered,
        flags: ListWriteFlags::AddUnique as u8
            | ListWriteFlags::NoFail as u8
            | ListWriteFlags::Partial as u8,
    }
}

/// Add a single write key to the transaction monitor record.
pub async fn add_key(
    cluster: Arc<Cluster>,
    policy: &BasePolicy,
    txn: &Arc<Txn>,
    cmd_key: &Key,
) -> Result<()> {
    txn.verify_command()?;
    txn.set_namespace(&cmd_key.namespace)?;

    if txn.write_exists_for_key(cmd_key) {
        return Ok(());
    }

    let ops = get_txn_ops(txn, cmd_key)?;
    add_write_keys(cluster, policy, txn, &ops).await
}

/// Add write keys from batch records to the transaction monitor record.
/// Only tracks keys for write operations (writes, deletes, UDFs).
pub async fn add_keys_from_records(
    cluster: Arc<Cluster>,
    policy: &BasePolicy,
    txn: &Arc<Txn>,
    batch_ops: &[crate::batch::BatchOperation],
) -> Result<()> {
    txn.verify_command()?;

    let list_policy = txn_ordered_list_policy();
    let mut digest_values: Vec<Value> = Vec::with_capacity(batch_ops.len());

    for batch_op in batch_ops {
        txn.set_namespace(&batch_op.key().namespace)?;

        if batch_op.has_write() {
            digest_values.push(Value::Blob(batch_op.key().digest.to_vec()));
        }
    }

    if digest_values.is_empty() {
        // Read-only batch doesn't need to add key digests.
        return Ok(());
    }

    let ops = get_txn_ops_from_value_list(txn, &list_policy, digest_values);
    add_write_keys(cluster, policy, txn, &ops).await
}

/// Add multiple write keys (by Key slice) to the transaction monitor record.
pub async fn add_keys(
    cluster: Arc<Cluster>,
    policy: &BasePolicy,
    txn: &Arc<Txn>,
    keys: &[Key],
) -> Result<()> {
    txn.verify_command()?;

    let list_policy = txn_ordered_list_policy();
    let mut digest_values: Vec<Value> = Vec::with_capacity(keys.len());

    for key in keys {
        digest_values.push(Value::Blob(key.digest.to_vec()));
    }

    let ops = get_txn_ops_from_value_list(txn, &list_policy, digest_values);
    add_write_keys(cluster, policy, txn, &ops).await
}

fn get_txn_ops(txn: &Arc<Txn>, cmd_key: &Key) -> Result<Vec<Operation>> {
    let list_policy = txn_ordered_list_policy();

    if txn.monitor_exists() {
        Ok(vec![lists::append(
            &list_policy,
            BIN_NAME_DIGESTS,
            Value::Blob(cmd_key.digest.to_vec()),
        )])
    } else {
        let id_bin = Bin::new(BIN_NAME_ID.to_string(), Value::Int(txn.id()));
        Ok(vec![
            scalar::put(&id_bin),
            lists::append(
                &list_policy,
                BIN_NAME_DIGESTS,
                Value::Blob(cmd_key.digest.to_vec()),
            ),
        ])
    }
}

fn get_txn_ops_from_value_list(
    txn: &Arc<Txn>,
    list_policy: &ListPolicy,
    digest_values: Vec<Value>,
) -> Vec<Operation> {
    if txn.monitor_exists() {
        vec![lists::append_items(
            list_policy,
            BIN_NAME_DIGESTS,
            digest_values,
        )]
    } else {
        let id_bin = Bin::new(BIN_NAME_ID.to_string(), Value::Int(txn.id()));
        vec![
            scalar::put(&id_bin),
            lists::append_items(list_policy, BIN_NAME_DIGESTS, digest_values),
        ]
    }
}

async fn add_write_keys(
    cluster: Arc<Cluster>,
    policy: &BasePolicy,
    txn: &Arc<Txn>,
    ops: &[Operation],
) -> Result<()> {
    let txn_key = get_txn_monitor_key(txn).ok_or_else(|| {
        crate::errors::Error::ClientError("Transaction namespace not set".to_string())
    })?;

    let wp = copy_timeout_policy(policy, txn);

    let mut cmd = TxnAddKeysCommand::new(&wp, cluster, &txn_key, ops.to_vec(), txn.clone());
    cmd.execute().await
}

fn copy_timeout_policy(policy: &BasePolicy, txn: &Arc<Txn>) -> WritePolicy {
    let mut wp = WritePolicy::default();
    wp.base_policy.socket_timeout = policy.socket_timeout;
    wp.base_policy.total_timeout = policy.total_timeout;
    wp.base_policy.timeout_delay = policy.timeout_delay;
    wp.base_policy.max_retries = policy.max_retries;
    wp.base_policy.sleep_between_retries = policy.sleep_between_retries;
    wp.base_policy.use_compression = policy.use_compression;
    wp.respond_per_each_op = true;
    wp.expiration = Expiration::Seconds(txn.timeout_secs());
    wp
}
