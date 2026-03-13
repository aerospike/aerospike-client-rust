// Copyright 2015-2020 Aerospike, Inc.
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

use std::path::Path;
use std::sync::Arc;
use std::vec::Vec;
use std::{str, usize};

use regex::Regex;
use std::sync::LazyLock;

use aerospike_rt::{sleep, Mutex};

use crate::batch::{BatchExecutor, BatchOperation};
use crate::cluster::{Cluster, Node};
use crate::commands::admin_command::AdminCommand;
use crate::commands::buffer::Buffer;
use crate::commands::{
    DeleteCommand, ExecuteUDFCommand, ExistsCommand, OperateCommand, QueryCommand, ReadCommand,
    ScanCommand, ServerCommand, TouchCommand, WriteCommand,
};
use crate::errors::{Error, Result};
use crate::expressions::Expression;
use crate::msgpack::encoder::pack_ctx_for_index;
use crate::net::ToHosts;
use crate::operations::{CdtContext, Operation, OperationType};
use crate::policy::{AdminPolicy, BatchPolicy, ClientPolicy, QueryPolicy, ReadPolicy, WritePolicy};
use crate::query::{PartitionFilter, PartitionTracker};
use crate::task::{DropIndexTask, ExecuteTask, IndexTask, RegisterTask, UdfRemoveTask};
use crate::{
    BatchRecord, Bin, Bins, CollectionIndexType, IndexType, Key, Privilege, Record, Recordset,
    ResultCode, Role, Statement, UDFLang, User, Value,
};
use crate::{Policy, Version};
use aerospike_rt::fs::File;
#[cfg(feature = "rt-tokio")]
use aerospike_rt::io::AsyncReadExt;
use aerospike_rt::time::Duration;
use aerospike_rt::Semaphore;
#[cfg(feature = "rt-async-std")]
use futures::AsyncReadExt;

const MAX_PERMITS: usize = 256;

/// Instantiate a Client instance to access an Aerospike database cluster and perform database
/// operations.
///
/// The client is thread-safe. Only one client instance should be used per cluster. Multiple
/// threads should share this cluster instance.
///
/// Your application uses this class' API to perform database operations such as writing and
/// reading records, and selecting sets of records. Write operations include specialized
/// functionality such as append/prepend and arithmetic addition.
///
/// Each record may have multiple bins, unless the Aerospike server nodes are configured as
/// "single-bin". In "multi-bin" mode, partial records may be written or read by specifying the
/// relevant subset of bins.
///
/// # See also
///
/// * [`Client::new`] to create a client, [`Client::close`] to shut it down
/// * [`ClientPolicy`] for connection configuration
pub struct Client {
    /// Cluster management object holding the cluster map and node connections.
    ///
    /// # See also
    ///
    /// * [`nodes`](Self::nodes), [`node_names`](Self::node_names), [`get_node`](Self::get_node)
    pub cluster: Arc<Cluster>,
}

unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    /// Initializes Aerospike client with suitable hosts to seed the cluster map. The client policy
    /// is used to set defaults and size internal data structures. For each host connection that
    /// succeeds, the client will:
    ///
    /// - Add host to the cluster map
    /// - Request host's list of other nodes in cluster
    /// - Add these nodes to the cluster map
    ///
    /// In most cases, only one host is necessary to seed the cluster. The remaining hosts are
    /// added as future seeds in case of a complete network failure.
    ///
    /// If one connection succeeds, the client is ready to process database requests. If all
    /// connections fail and the policy's `fail_`
    ///
    /// The seed hosts to connect to (one or more) can be specified as a comma-separated list of
    /// hostnames or IP addresses with optional port numbers, e.g.
    ///
    /// ```text
    /// 10.0.0.1:3000,10.0.0.2:3000,10.0.0.3:3000
    /// ```
    ///
    /// Port 3000 is used by default if the port number is omitted for any of the hosts.
    ///
    /// # Arguments
    ///
    /// * `policy` — Client policy (timeouts, connection limits, authentication). Must pass [`ClientPolicy::validate`].
    /// * `hosts` — Seed hosts; any type implementing [`ToHosts`] (e.g. string like `"host1:3000,host2:3000"`).
    ///
    /// # Returns
    ///
    /// `Ok(Client)` once at least one seed host connects and the cluster map is initialized.
    ///
    /// # Errors
    ///
    /// * Returns an error if policy validation fails, no host can be reached, or cluster discovery fails.
    ///
    /// # See also
    ///
    /// * [`close`](Self::close) to shut down the client
    /// * [`ClientPolicy`] for configuration options
    ///
    /// # Examples
    ///
    /// Using an environment variable to set the list of seed hosts.
    ///
    /// ```rust,edition2021
    /// use aerospike::{Client, ClientPolicy};
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// match Client::new(&ClientPolicy::default(), &hosts).await {
    ///     Ok(client) => { /* use client */ }
    ///     Err(e) => eprintln!("Failed to connect: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn new(policy: &ClientPolicy, hosts: &(dyn ToHosts + Send + Sync)) -> Result<Self> {
        policy.validate()?;
        let hosts = hosts.to_hosts()?;
        let cluster = Cluster::new(policy.clone(), &hosts).await?;

        Ok(Client { cluster })
    }

    /// Closes the connection to the Aerospike cluster.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.close().await {
    ///     Ok(()) => { /* disconnected */ }
    ///     Err(e) => eprintln!("Error closing client: {}", e),
    /// }
    /// # }
    /// ```
    pub async fn close(&self) -> Result<()> {
        self.cluster.close().await?;
        Ok(())
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// if client.is_connected() {
    ///     println!("Connected to cluster");
    /// }
    /// # }
    /// ```
    pub fn is_connected(&self) -> bool {
        self.cluster.is_connected()
    }

    /// Returns a list of the names of the active server nodes in the cluster.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let names = client.node_names();
    /// println!("Nodes: {:?}", names);
    /// # }
    /// ```
    pub fn node_names(&self) -> Vec<String> {
        self.cluster
            .nodes()
            .iter()
            .map(|node| node.name().to_owned())
            .collect()
    }

    /// Return node given its name.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.get_node("BB9020011AC420214") {
    ///     Ok(node) => println!("Node: {}", node.name()),
    ///     Err(e) => eprintln!("Node not found: {}", e),
    /// }
    /// # }
    /// ```
    pub fn get_node(&self, name: &str) -> Result<Arc<Node>> {
        self.cluster.get_node_by_name(name)
    }

    /// Returns a list of active server nodes in the cluster.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let nodes = client.nodes();
    /// println!("{} node(s) in cluster", nodes.len());
    /// # }
    /// ```
    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.cluster.nodes()
    }

    /// Read the record for the specified key. Depending on the bins value provided, all record bins,
    /// only selected record bins, or only the record headers will be returned. The policy can be
    /// used to specify timeouts.
    ///
    /// # Arguments
    ///
    /// * `policy` — Read policy (timeouts, replica selection).
    /// * `key` — Record key (namespace, set, and key or digest).
    /// * `bins` — Which bins to return: [`Bins::All`], [`Bins::None`] (headers only), or a collection of bin names.
    ///
    /// # Returns
    ///
    /// `Ok(Record)` containing the requested bins (or headers only when using [`Bins::None`]). The record's key may be `None`; use the key you passed in for writes.
    ///
    /// # Errors
    ///
    /// * [`Error::ServerError`](crate::errors::Error::ServerError) with [`ResultCode::KeyNotFoundError`] if the record does not exist.
    /// * Other errors for network, timeout, or server failures.
    ///
    /// # Panics
    /// Panics if the return is invalid
    ///
    /// # See also
    ///
    /// * [`put`](Self::put), [`operate`](Self::operate), [`exists`](Self::exists)
    ///
    /// # Examples
    ///
    /// Fetch specified bins for a record with the given key.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get(&ReadPolicy::default(), &key, ["a", "b"]).await {
    ///     Ok(record)
    ///         => println!("a={:?}", record.bins.get("a")),
    ///     Err(Error::ServerError(ResultCode::KeyNotFoundError,..))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// # }
    /// ```
    ///
    /// Determine the remaining time-to-live of a record.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get(&ReadPolicy::default(), &key, Bins::None).await {
    ///     Ok(record) => {
    ///         match record.time_to_live() {
    ///             None => println!("record never expires"),
    ///             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
    ///         }
    ///     },
    ///     Err(Error::ServerError(ResultCode::KeyNotFoundError,..))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn get<T>(&self, policy: &ReadPolicy, key: &Key, bins: T) -> Result<Record>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let bins = bins.into();
        let mut command = ReadCommand::new(
            &policy.base_policy,
            self.cluster.clone(),
            key,
            bins,
            policy.replica,
        );
        command.execute().await?;
        Ok(command.record.unwrap())
    }

    /// Read multiple record for specified batch keys in one batch call. This method allows
    /// different namespaces/bins to be requested for each key in the batch. If the `BatchRead` key
    /// field is not found, the corresponding record field will be `None`. The policy can be used
    /// to specify timeouts and maximum concurrent threads. This method requires Aerospike Server
    /// version >= 3.6.0.
    ///
    /// # Arguments
    ///
    /// * `policy` — Batch policy (timeouts, max concurrent nodes).
    /// * `batch` — Slice of [`BatchOperation`] items (read, write, delete, UDF) keyed by [`Key`].
    ///
    /// # Returns
    ///
    /// `Ok(Vec<BatchRecord>)` with one [`BatchRecord`] per operation; each record field is `Some` if the key was found, `None` otherwise. Order matches the input batch.
    ///
    /// # Errors
    ///
    /// * Returns an error if the batch request fails (e.g. timeout, cluster error). Individual key-not-found is indicated by `record: None` in the corresponding [`BatchRecord`].
    ///
    /// # See also
    ///
    /// * [`BatchOperation`], [`BatchRecord`], [`get`](Self::get)
    ///
    /// # Examples
    ///
    /// Fetch multiple records in a single client request
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3000"));
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let bins = Bins::from(["name", "age"]);
    /// let bin1 = as_bin!("a", "a value");
    /// let bin2 = as_bin!("b", "another value");
    /// let bin3 = as_bin!("c", 42);
    ///
    /// let key1 = as_key!("test", "test", 1);
    /// let key2 = as_key!("test", "test", 2);
    /// let key3 = as_key!("test", "test", 3);
    ///
    /// let key4 = as_key!("test", "test", -1);
    /// // key does not exist
    ///
    /// let selected = Bins::from(["a"]);
    /// let all = Bins::All;
    /// let none = Bins::None;
    ///
    /// let wops = vec![
    ///     operations::put(&bin1),
    ///     operations::put(&bin2),
    ///     operations::put(&bin3),
    /// ];
    ///
    /// let rops = vec![
    ///     operations::get_bin(&bin1.name),
    ///     operations::get_bin(&bin2.name),
    ///     operations::get_header(),
    /// ];
    ///
    /// let bpolicy = BatchPolicy::default();
    /// let bpr = BatchReadPolicy::default();
    /// let bpw = BatchWritePolicy::default();
    /// let bpd = BatchDeletePolicy::default();
    /// let bpu = BatchUDFPolicy::default();
    ///
    /// let batch = vec![
    ///     BatchOperation::write(&bpw, key1.clone(), wops.clone()),
    ///     BatchOperation::read(&bpr, key1.clone(), selected),
    ///     BatchOperation::read(&bpr, key2.clone(), all),
    ///     BatchOperation::read(&bpr, key3.clone(), none.clone()),
    ///     BatchOperation::read_ops(&bpr, key3.clone(), rops),
    ///     BatchOperation::delete(&bpd, key1.clone()),
    ///     BatchOperation::udf(&bpu, key1.clone(), "test_udf", "echo", None),
    /// ];
    /// match client.batch(&bpolicy, &batch).await {
    ///     Ok(results) => {
    ///         for result in results {
    ///             match result.record {
    ///                 Some(record) => println!("{:?} => {:?}", result.key, record.bins),
    ///                 None => println!("No such record: {:?}", result.key),
    ///             }
    ///         }
    ///     }
    ///     Err(err) => println!("Error executing batch request: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn batch(
        &self,
        policy: &BatchPolicy,
        ops: &[BatchOperation],
    ) -> Result<Vec<BatchRecord>> {
        let executor = BatchExecutor::new(self.cluster.clone());
        executor.execute(policy, ops).await
    }

    /// Write record bin(s). The policy specifies the transaction timeout, record expiration, and
    /// how the transaction is handled when the record already exists.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration, generation, record-exists action).
    /// * `key` — Record key (namespace, set, key, or digest).
    /// * `bins` — Bins to write; bins not listed are left unchanged for an existing record.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// * Returns an error on write failure (e.g. generation conflict, cluster/network error).
    ///
    /// # See also
    ///
    /// * [`get`](Self::get), [`operate`](Self::operate), [`add`](Self::add), [`delete`](Self::delete)
    ///
    /// # Examples
    ///
    /// Write a record with a single integer bin.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// match client.put(&WritePolicy::default(), &key, &vec![bin]).await {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    ///
    /// Write a record with an expiration of 10 seconds.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::Seconds(10);
    /// match client.put(&policy, &key, &vec![bin]).await {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn put(&self, policy: &WritePolicy, key: &Key, bins: &[Bin]) -> Result<()> {
        let mut command = WriteCommand::new(
            policy,
            self.cluster.clone(),
            key,
            bins,
            OperationType::Write,
        );
        command.execute().await
    }

    /// Add integer bin values to existing record bin values. The policy specifies the transaction
    /// timeout, record expiration, and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration, generation).
    /// * `key` — Record key.
    /// * `bins` — Bins with integer values to add to the current bin values.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// * Returns an error if the record does not exist, bins are not integers, or on cluster/network failure.
    ///
    /// # See also
    ///
    /// * [`put`](Self::put), [`operate`](Self::operate)
    ///
    /// # Examples
    ///
    /// Add two integer values to two existing bin values.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bina = as_bin!("a", 1);
    /// let binb = as_bin!("b", 2);
    /// let bins = vec![bina, binb];
    /// match client.add(&WritePolicy::default(), &key, &bins).await {
    ///     Ok(()) => println!("Record updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn add(&self, policy: &WritePolicy, key: &Key, bins: &[Bin]) -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Incr);
        command.execute().await
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration, and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration, generation).
    /// * `key` — Record key.
    /// * `bins` — Bins with string values to append to the current bin values.
    ///
    /// # Returns
    ///
    /// `Ok(())` on success.
    ///
    /// # Errors
    ///
    /// * Returns an error if the record does not exist, bins are not strings, or on cluster/network failure.
    ///
    /// # See also
    ///
    /// * [`prepend`](Self::prepend), [`put`](Self::put), [`operate`](Self::operate)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("log", " new text");
    /// match client.append(&WritePolicy::default(), &key, &[bin]).await {
    ///     Ok(()) => println!("Appended"),
    ///     Err(err) => println!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn append(&self, policy: &WritePolicy, key: &Key, bins: &[Bin]) -> Result<()> {
        let mut command = WriteCommand::new(
            policy,
            self.cluster.clone(),
            key,
            bins,
            OperationType::Append,
        );
        command.execute().await
    }

    /// Prepend bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration, and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration, generation).
    /// * `key` — Record key.
    /// * `bins` — Bins with string values to prepend to the current bin values
    ///
    /// # Errors
    ///
    /// * Returns an error if the record does not exist, bins are not strings, or on cluster/network failure.
    ///
    /// # See also
    ///
    /// * [`append`](Self::append), [`put`](Self::put), [`operate`](Self::operate)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("log", "prefix ");
    /// match client.prepend(&WritePolicy::default(), &key, &[bin]).await {
    ///     Ok(()) => println!("Prepended"),
    ///     Err(err) => println!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn prepend(&self, policy: &WritePolicy, key: &Key, bins: &[Bin]) -> Result<()> {
        let mut command = WriteCommand::new(
            policy,
            self.cluster.clone(),
            key,
            bins,
            OperationType::Prepend,
        );
        command.execute().await
    }

    /// Delete record for a specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, generation if applicable).
    /// * `key` — Record key to delete.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the record existed and was deleted; `Ok(false)` if it did not exist.
    ///
    /// # Errors
    ///
    /// * Returns an error on cluster/network failure or timeout.
    ///
    /// # See also
    ///
    /// * [`put`](Self::put), [`get`](Self::get), [`touch`](Self::touch)
    ///
    /// # Examples
    ///
    /// Delete a record.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.delete(&WritePolicy::default(), &key).await {
    ///     Ok(true) => println!("Record deleted"),
    ///     Ok(false) => println!("Record did not exist"),
    ///     Err(err) => println!("Error deleting record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn delete(&self, policy: &WritePolicy, key: &Key) -> Result<bool> {
        let mut command = DeleteCommand::new(policy, self.cluster.clone(), key);
        command.execute().await?;
        Ok(command.existed)
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration value to set).
    /// * `key` — Record key.
    ///
    /// # Errors
    ///
    /// * Returns an error if the record does not exist or on cluster/network failure.
    ///
    /// # See also
    ///
    /// * [`put`](Self::put), [`delete`](Self::delete), [`get`](Self::get)
    ///
    /// # Examples
    ///
    /// Reset a record's time to expiration to the default ttl for the namespace.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::NamespaceDefault;
    /// match client.touch(&policy, &key).await {
    ///     Ok(()) => println!("Record expiration updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn touch(&self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let mut command = TouchCommand::new(policy, self.cluster.clone(), key);
        command.execute().await
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    ///
    /// # Arguments
    ///
    /// * `policy` — Read policy (timeouts, replica).
    /// * `key` — Record key to check.
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the record exists; `Ok(false)` otherwise.
    ///
    /// # Errors
    ///
    /// * Returns an error on cluster/network failure or timeout.
    ///
    /// # See also
    ///
    /// * [`get`](Self::get), [`put`](Self::put)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.exists(&ReadPolicy::default(), &key).await {
    ///     Ok(true) => println!("Record exists"),
    ///     Ok(false) => println!("Record does not exist"),
    ///     Err(err) => println!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn exists(&self, policy: &ReadPolicy, key: &Key) -> Result<bool> {
        let mut command = ExistsCommand::new(policy, self.cluster.clone(), key);
        command.execute().await?;
        Ok(command.exists)
    }

    /// Perform multiple read/write operations on a single key in one batch call.
    ///
    /// Operations on scalar values, lists, and maps can be performed in the same call.
    ///
    /// Operations execute in the order specified by the client application.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout, expiration, generation).
    /// * `key` — Record key.
    /// * `ops` — Ordered list of operations (e.g., read, add, put, delete) from [`operations`](crate::operations).
    ///
    /// # Returns
    ///
    /// `Ok(Record)` containing the result of the read operations in the list (e.g. bins after apply). The record may be empty if no read ops were included.
    ///
    /// # Errors
    ///
    /// * Returns an error if any operation fails (e.g., key not found, generation conflict, cluster/network error).
    ///
    /// # Panics
    ///  Panics if the return is invalid
    ///
    /// # See also
    ///
    /// * [`get`](Self::get), [`put`](Self::put), [`operations`](crate::operations)
    ///
    /// # Examples
    ///
    /// Add an integer value to an existing record and then read the result, all in one database
    /// call.
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("a", 42);
    /// let ops = vec![
    ///     operations::add(&bin),
    ///     operations::get_bin("a"),
    /// ];
    /// match client.operate(&WritePolicy::default(), &key, &ops).await {
    ///     Ok(record) => println!("The new value is {:?}", record.bins.get("a")),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn operate(
        &self,
        policy: &WritePolicy,
        key: &Key,
        ops: &[Operation],
    ) -> Result<Record> {
        let mut command = OperateCommand::new(policy, self.cluster.clone(), key, ops);
        command.execute().await?;
        Ok(command.read_command.record.unwrap())
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting language for UDFs at the moment.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `udf_body` — Raw UDF source bytes (e.g. Lua code).
    /// * `server_path` — Path name on the server (e.g. `"example.lua"`).
    /// * `language` — [`UDFLang`] (e.g. [`UDFLang::Lua`]).
    ///
    /// # Returns
    ///
    /// `Ok(RegisterTask)` to poll or wait for registration to complete across the cluster.
    ///
    /// # Errors
    ///
    /// * Returns an error if the info command fails (e.g. invalid UDF, cluster error).
    ///
    /// # See also
    ///
    /// * [`register_udf_from_file`](Self::register_udf_from_file), [`remove_udf`](Self::remove_udf), [`execute_udf`](Self::execute_udf), [`RegisterTask`]
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # extern crate aerospike;
    /// # use aerospike::*;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let code = r#"
    /// -- Validate value before writing.
    /// function writeWithValidation(r,name,value)
    ///   if (value >= 1 and value <= 10) then
    ///     if not aerospike:exists(r) then
    ///       aerospike:create(r)
    ///     end
    ///     r[name] = value
    ///     aerospike:update(r)
    ///   else
    ///       error("1000:Invalid value")
    ///   end
    /// end
    ///
    /// -- Set a particular bin only if record does not already exist.
    /// function writeUnique(r,name,value)
    ///   if not aerospike:exists(r) then
    ///     aerospike:create(r)
    ///     r[name] = value
    ///     aerospike:update(r)
    ///   end
    /// end
    /// "#;
    ///
    /// match client.register_udf(&AdminPolicy::default(), code.as_bytes(),
    ///                           "example.lua", UDFLang::Lua).await {
    ///     Ok(_task) => { /* wait for task or use it */ }
    ///     Err(err) => println!("Failed to register UDF: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn register_udf(
        &self,
        policy: &AdminPolicy,
        udf_body: &[u8],
        server_path: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        let udf_body = base64::encode(udf_body);

        let cmd = format!(
            "udf-put:filename={};content={};content-len={};udf-type={};",
            server_path,
            udf_body,
            udf_body.len(),
            language
        );
        let node = self.cluster.get_random_node()?;
        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("Error registering UDF"))?;

        Ok(RegisterTask::new(
            Arc::clone(&self.cluster),
            server_path.to_string(),
        ))
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting language for UDFs at the moment.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `client_path` — Local file path to the UDF source file.
    /// * `server_path` — Path name on the server (e.g. `"example.lua"`).
    /// * `language` — [`UDFLang`] (e.g. [`UDFLang::Lua`]).
    ///
    /// # Returns
    ///
    /// `Ok(RegisterTask)` to poll or wait for registration to complete.
    ///
    /// # Errors
    ///
    /// * Returns an error if the file cannot be read or [`register_udf`](Self::register_udf) fails.
    ///
    /// # See also
    ///
    /// * [`register_udf`](Self::register_udf), [`remove_udf`](Self::remove_udf)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy, UDFLang};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.register_udf_from_file(
    ///     &AdminPolicy::default(),
    ///     "/path/to/my_udf.lua",
    ///     "my_udf.lua",
    ///     UDFLang::Lua,
    /// ).await {
    ///     Ok(task) => { /* wait for task.wait_till_complete(None).await */ }
    ///     Err(err) => eprintln!("Failed to register UDF from file: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn register_udf_from_file(
        &self,
        policy: &AdminPolicy,
        client_path: &str,
        server_path: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        let path = Path::new(client_path);
        let mut file = File::open(&path).await?;
        let mut udf_body: Vec<u8> = vec![];
        file.read_to_end(&mut udf_body).await?;

        self.register_udf(policy, &udf_body, server_path, language)
            .await
    }

    /// Remove a user-defined function (UDF) module from the server.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `server_path` — Server path of the UDF module (e.g. `"example.lua"`).
    ///
    /// # Returns
    ///
    /// `Ok(UdfRemoveTask)` to poll or wait for removal to complete across the cluster.
    ///
    /// # Errors
    ///
    /// * Returns an error if the info command fails (e.g., path not found, cluster error).
    ///
    /// # See also
    ///
    /// * [`register_udf`](Self::register_udf), [`execute_udf`](Self::execute_udf)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.remove_udf(&AdminPolicy::default(), "example.lua").await {
    ///     Ok(task) => { /* wait for task.wait_till_complete(None).await */ }
    ///     Err(err) => eprintln!("Failed to remove UDF: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn remove_udf(
        &self,
        policy: &AdminPolicy,
        server_path: &str,
    ) -> Result<UdfRemoveTask> {
        let cmd = format!("udf-remove:filename={server_path};");
        let node = self.cluster.get_random_node()?;
        // Sample response: {"udf-remove:filename=server_path;": "ok"}
        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("UDF Remove failed"))?;

        Ok(UdfRemoveTask::new(
            Arc::clone(&self.cluster),
            server_path.to_string(),
        ))
    }

    /// Execute a user-defined function on the server and return the results. The function operates
    /// on a single record. The UDF package name is required to locate the UDF.
    ///
    /// # Arguments
    ///
    /// * `policy` — Write policy (timeout).
    /// * `key` — Record key the UDF operates on.
    /// * `server_path` — Server path of the UDF module (e.g. `"example.lua"`).
    /// * `function_name` — Name of the function to invoke.
    /// * `args` — Optional arguments to pass to the UDF (as [`Value`]s).
    ///
    /// # Returns
    ///
    /// `Ok(Some(Value))` with the UDF return value, or `Ok(None)` if the UDF returned nothing.
    ///
    /// # Errors
    ///
    /// * Returns an error if the UDF fails, the record/key is invalid, or on cluster/network failure. [`Error::UdfBadResponse`](crate::errors::Error::UdfBadResponse) if the UDF returns a failure or invalid response.
    ///
    /// # Panics
    /// Panics if the return is invalid
    ///
    /// # See also
    ///
    /// * [`register_udf`](Self::register_udf), [`remove_udf`](Self::remove_udf)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::*;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.execute_udf(
    ///     &WritePolicy::default(),
    ///     &key,
    ///     "my_module",
    ///     "my_function",
    ///     Some(&[as_val!(42)]),
    /// ).await {
    ///     Ok(Some(val)) => println!("UDF returned: {:?}", val),
    ///     Ok(None) => println!("UDF returned nothing"),
    ///     Err(err) => println!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn execute_udf(
        &self,
        policy: &WritePolicy,
        key: &Key,
        server_path: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<Option<Value>> {
        let mut command = ExecuteUDFCommand::new(
            policy,
            self.cluster.clone(),
            key,
            server_path,
            function_name,
            args,
        );

        command.execute().await?;

        let record = command.read_command.record.unwrap();

        // User defined functions don't have to return a value.
        if record.bins.is_empty() {
            return Ok(None);
        }

        for (key, value) in &record.bins {
            if key.contains("SUCCESS") {
                return Ok(Some(value.clone()));
            } else if key.contains("FAILURE") {
                return Err(Error::UdfBadResponse(value.to_string()));
            }
        }

        Err(Error::UdfBadResponse("Invalid UDF return value".into()))
    }

    /// Execute a query on all server nodes and return a record iterator. The query executor puts
    /// records on a queue in separate threads. The calling thread concurrently pops records off
    /// the queue through the record iterator.
    ///
    /// # Arguments
    ///
    /// * `policy` — Query policy (timeouts, max records, record queue size, etc.).
    /// * `partition_filter` — Which partitions to query (e.g. [`PartitionFilter::all`], or by id/range/key for pagination).
    /// * `statement` — Namespace, set, bins, and filters ([`Statement`]).
    ///
    /// # Returns
    ///
    /// `Ok(Arc<Recordset>)` — a shared record set. Consume with [`Recordset::into_stream`] and iterate the stream for records. The recordset is closed when the stream is dropped or exhausted.
    ///
    /// # Errors
    ///
    /// * Returns an error if the statement is invalid (e.g. [`Statement::validate`] fails) or initial partition assignment fails.
    ///
    /// # Panics
    /// Panics if the async block fails
    ///
    /// # See also
    ///
    /// * [`Statement`], [`PartitionFilter`], [`Recordset`]
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # extern crate aerospike;
    /// # use aerospike::*;
    /// # use futures::stream::StreamExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3000"));
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let stmt = Statement::new("test", "test", Bins::All);
    /// let mut qp = QueryPolicy::default();
    /// qp.max_records = 1000;
    /// let pf = PartitionFilter::all();
    ///
    /// match client.query(&qp, pf, stmt).await {
    ///     Ok(recordset) => {
    ///         let mut stream = recordset.into_stream();
    ///         let mut count = 0u64;
    ///         while let Some(result) = stream.next().await {
    ///             match result {
    ///                 Ok(_record) => count += 1,
    ///                 Err(err) => println!("Error executing query: {}", err),
    ///             }
    ///         }
    ///         println!("Records: {}", count);
    ///     }
    ///     Err(err) => println!("Failed to execute query: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn query(
        &self,
        policy: &QueryPolicy,
        partition_filter: PartitionFilter,
        statement: Statement,
    ) -> Result<Arc<Recordset>> {
        statement.validate()?;
        let statement = Arc::new(statement);

        let nodes: Vec<Arc<Node>> = self.cluster.nodes();
        let t_policy = policy.clone();
        let tracker = Arc::new(Mutex::new(
            PartitionTracker::new(&t_policy, Arc::new(Mutex::new(partition_filter)), nodes).await?,
        ));

        let recordset = Arc::new(Recordset::new(
            policy.record_queue_size,
            usize::MAX, // will be reset later
            tracker.clone(),
        ));

        let t_recordset = recordset.clone();
        let defer_recordset = recordset.clone();
        let cluster = self.cluster.clone();
        aerospike_rt::spawn(async move {
            Self::execute_query_timeout(
                cluster,
                &t_policy,
                tracker.clone(),
                statement.clone(),
                t_recordset,
            )
            .await;
            defer_recordset.close();
        });

        Ok(recordset)
    }

    /// Execute a query and apply operations to matching records on the server.
    /// This method sends the command to all nodes and returns an `ExecuteTask`
    /// that can be used to monitor the progress of the background job.
    ///
    /// The statement's filters determine which records are affected. If no filter
    /// is specified, all records in the namespace/set are processed (scan mode).
    ///
    /// Only write operations are allowed. Read operations will result in an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::*;
    /// # async fn example(client: &Client) -> Result<()> {
    /// let wpolicy = WritePolicy::default();
    /// let statement = Statement::new("ns", "set", Bins::All);
    /// let ops = vec![operations::put(&Bin::new("bin", 42))];
    /// let task = client.query_operate(&wpolicy, statement, &ops).await?;
    /// task.wait_till_complete(None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_operate(
        &self,
        write_policy: &WritePolicy,
        statement: Statement,
        operations: &[Operation],
    ) -> Result<ExecuteTask> {
        statement.validate()?;

        let nodes = self.cluster.nodes();
        if nodes.is_empty() {
            return Err(Error::Connection("No connections available".to_string()));
        }

        let task_id: u64 = rand::random();
        let scan = statement.filters.is_none();

        let mut last_err: Option<Error> = None;
        for node in &nodes {
            let mut cmd =
                ServerCommand::new(node.clone(), write_policy, &statement, task_id, operations);
            if let Err(err) = cmd.execute().await {
                last_err = Some(err);
            }
        }

        if let Some(err) = last_err {
            return Err(err);
        }

        Ok(ExecuteTask::new(self.cluster.clone(), task_id, scan))
    }

    async fn execute_query_timeout(
        cluster: Arc<Cluster>,
        policy: &QueryPolicy,
        tracker: Arc<Mutex<PartitionTracker>>,
        statement: Arc<Statement>,
        recordset: Arc<Recordset>,
    ) {
        if policy.total_timeout() > 0 {
            let rs_closer = recordset.clone();
            if let Err(_) = aerospike_rt::timeout(
                Duration::from_millis(u64::from(policy.total_timeout())),
                Self::execute_query(cluster, policy, tracker, statement, recordset),
            )
            .await
            {
                let _ = rs_closer
                    .push(Err(Error::Timeout("Timeout".to_string())))
                    .await;
            }
        } else {
            Self::execute_query(cluster, policy, tracker, statement, recordset).await;
        }
    }

    async fn execute_query(
        cluster: Arc<Cluster>,
        policy: &QueryPolicy,
        tracker: Arc<Mutex<PartitionTracker>>,
        statement: Arc<Statement>,
        recordset: Arc<Recordset>,
    ) {
        let namespace = statement.namespace.clone();
        loop {
            let mut timed_out = false;
            {
                let mut tracker_locked = tracker.lock().await;
                match tracker_locked
                    .assign_partitions_to_nodes(cluster.clone(), &namespace)
                    .await
                {
                    Ok(()) => (),
                    Err(e) => {
                        recordset.err(e).await;
                        tracker_locked.partition_error().await;
                        return;
                    }
                }

                let list = tracker_locked.node_partitions_list();
                let mut handles = Vec::with_capacity(list.len());
                recordset.set_instances(list.len() + 1); // +1 is for the async executor

                // used for join errors
                let err_recordset = recordset.clone();

                if recordset.is_active() {
                    let semaphore = Arc::new(Semaphore::new(if policy.max_concurrent_nodes == 0 {
                        MAX_PERMITS
                    } else {
                        policy.max_concurrent_nodes
                    }));

                    for node_partition in list {
                        let semaphore = semaphore.clone();
                        let recordset = recordset.clone();
                        let policy = policy.clone();
                        let node_partition = node_partition.clone();
                        let statement = statement.clone();
                        let handle = aerospike_rt::spawn(async move {
                            let permit = semaphore.acquire().await;
                            let result =
                                if statement.index_name.is_none() && statement.filters.is_none() {
                                    ScanCommand::new(
                                        &policy,
                                        &statement.namespace,
                                        &statement.set_name,
                                        statement.bins.clone(),
                                        recordset.clone(),
                                        node_partition,
                                    )
                                    .await
                                    .execute()
                                    .await
                                } else {
                                    QueryCommand::new(
                                        &policy,
                                        statement,
                                        recordset.clone(),
                                        node_partition,
                                    )
                                    .await
                                    .execute()
                                    .await
                                };

                            drop(permit);
                            result
                        });

                        handles.push(handle);
                    }

                    drop(tracker_locked);

                    match futures::future::try_join_all(handles).await {
                        Err(e) => err_recordset.err(Error::ClientError(e.to_string())).await,
                        #[cfg(feature = "rt-async-std")]
                        Ok(_) => (),
                        #[cfg(feature = "rt-tokio")]
                        Ok(errs) => {
                            for err in errs {
                                match err {
                                    Err(Error::Timeout(_) | Error::Io(_)) => timed_out = true,
                                    Err(e) => {
                                        tracker.lock().await.partition_error().await;
                                        err_recordset.err(e).await;
                                    }
                                    Ok(()) => (),
                                }
                            }
                        }
                    }
                }
            };

            let mut tracker = tracker.lock().await;
            let done = tracker.is_complete(policy, timed_out).await;
            match (done, recordset.is_active()) {
                (Ok(true), _) => return,
                (Ok(_), false) => return,
                (Err(e), _) => {
                    tracker.partition_error().await;
                    recordset.err(e).await;
                    return;
                }
                _ => (),
            }

            if let Some(sleep_between_retries) = policy.base_policy.sleep_between_retries() {
                sleep(sleep_between_retries).await;
            }

            recordset.reset_task_id();
        }
    }

    /// Sets XDR filter for a given datacenter name and namespace. The expression filter indicates
    /// which records XDR should ship to the datacenter.
    /// Pass nil as a filter to remove the current filter on the server.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `datacenter` — XDR datacenter name.
    /// * `namespace` — Namespace to apply the filter to.
    /// * `filter_expression` — Expression to filter which records are shipped; `None` to remove the current filter.
    ///
    /// # Errors
    ///
    /// * Returns an error if the info command fails (e.g., invalid expression, cluster error).
    ///
    /// # See also
    ///
    /// * [`Expression`](crate::expressions::Expression)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// // Remove XDR filter for a datacenter/namespace (pass None)
    /// match client.set_xdr_filter(
    ///     &AdminPolicy::default(),
    ///     "DC1",
    ///     "test",
    ///     None,
    /// ).await {
    ///     Ok(()) => { /* filter cleared */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn set_xdr_filter(
        &self,
        policy: &AdminPolicy,
        datacenter: &str,
        namespace: &str,
        filter_expression: Option<&Expression>,
    ) -> Result<()> {
        let node = self.cluster.get_random_node()?;

        let cmd = if let Some(expression) = filter_expression {
            let size = expression.size()?;
            let mut buf = Buffer::new(0);
            buf.resize_buffer(size)?;
            let _ = expression.pack(&mut Some(&mut buf));
            let exp_str = base64::encode(&buf.data_buffer);

            format!("xdr-set-filter:dc={datacenter};namespace={namespace};exp={exp_str}")
        } else {
            format!("xdr-set-filter:dc={datacenter};namespace={namespace};exp=null")
        };

        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("Error setting XDR filter"))
    }

    /// Removes all records in the specified namespace/set efficiently.
    ///
    /// This method is many orders of magnitude faster than deleting records one at a time. It
    /// requires Aerospike Server version 3.12 or later. See
    /// <https://www.aerospike.com/docs/reference/info#truncate> for further info.
    ///
    /// The `set_name` is optional; set to `""` to delete all sets in `namespace`.
    ///
    /// `before_nanos` optionally specifies a last update timestamp (lut); if it is greater than
    /// zero, only records with a lut less than `before_nanos` are deleted. Units are in
    /// nanoseconds since unix epoch (1970-01-01). Pass in zero to delete all records in the
    /// namespace/set regardless of last update time.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `namespace` — Namespace to truncate.
    /// * `set_name` — Set name, or `""` to truncate all sets in the namespace.
    /// * `before_nanos` — If greater than zero, only records with lut less than this (nanoseconds since epoch) are deleted; use `0` for all.
    ///
    /// # Errors
    ///
    /// * Returns an error if the truncate info command fails (e.g. cluster error, server version &lt; 3.12).
    ///
    /// # See also
    ///
    /// * [Truncate reference](https://www.aerospike.com/docs/reference/info#truncate)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// // Truncate all records in set "myset" in namespace "test"
    /// match client.truncate(&AdminPolicy::default(), "test", "myset", 0).await {
    ///     Ok(()) => { /* truncate completed */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn truncate(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        before_nanos: i64,
    ) -> Result<()> {
        let mut cmd = String::with_capacity(160);
        if !set_name.is_empty() {
            cmd.push_str("truncate:namespace=");
        } else {
            cmd.push_str("truncate-namespace:namespace=");
        }
        cmd.push_str(namespace);

        if !set_name.is_empty() {
            cmd.push_str(";set=");
            cmd.push_str(set_name);
        }

        if before_nanos > 0 {
            cmd.push_str(";lut=");
            cmd.push_str(&format!("{before_nanos}"));
        }

        let node = self.cluster.get_random_node()?;
        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("Error truncating ns/set"))
    }

    /// Create a secondary index on a bin. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `namespace` — Namespace for the index.
    /// * `set_name` — Set name (can be empty for namespace-wide).
    /// * `bin_name` — Bin to index.
    /// * `index_name` — Unique name for the index.
    /// * `index_type` — [`IndexType`] (e.g. Numeric, String).
    /// * `collection_index_type` — [`CollectionIndexType`] for list/map indexing.
    /// * `ctx` — Optional [`CdtContext`](crate::operations::CdtContext) for nested collection indexing.
    ///
    /// # Returns
    ///
    /// `Ok(IndexTask)` to poll or wait for index creation to complete (e.g. [`IndexTask::wait_till_complete`](crate::task::IndexTask::wait_till_complete)).
    ///
    /// # Errors
    ///
    /// * Returns an error if the index create command fails (e.g. index already exists, invalid params, cluster error).
    ///
    /// # See also
    ///
    /// * [`create_index_using_expression`](Self::create_index_using_expression), [`drop_index`](Self::drop_index), [`IndexTask`](crate::task::IndexTask)
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` and bin `baz`:
    ///
    /// ```rust,edition2021
    /// # extern crate aerospike;
    /// # use aerospike::*;
    /// # use std::env;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3000"));
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let policy = AdminPolicy::default();
    ///
    /// match client.create_index_on_bin(
    ///     &policy, "foo", "bar", "baz",
    ///     "idx_foo_bar_baz", IndexType::Numeric, CollectionIndexType::Default, None,
    /// ).await {
    ///     Err(err) => println!("Failed to create index: {}", err),
    ///     Ok(task) => { /* wait for task with task.wait_till_complete(None).await */ }
    /// }
    /// # }
    /// ```
    pub async fn create_index_on_bin(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
        collection_index_type: CollectionIndexType,
        ctx: Option<&[CdtContext]>,
    ) -> Result<IndexTask> {
        self.create_index(
            policy,
            namespace,
            set_name,
            bin_name,
            index_name,
            index_type,
            collection_index_type,
            None,
            ctx,
        )
        .await
    }

    /// Create a secondary index using an expression. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `namespace` — Namespace for the index.
    /// * `set_name` — Set name (can be empty for namespace-wide).
    /// * `index_name` — Unique name for the index.
    /// * `index_type` — [`IndexType`] (e.g. Numeric, String).
    /// * `collection_index_type` — [`CollectionIndexType`].
    /// * `expression` — [`Expression`](crate::expressions::Expression) defining the index (e.g. equality on a bin).
    ///
    /// # Returns
    ///
    /// `Ok(IndexTask)` to poll or wait for index creation to complete.
    ///
    /// # Errors
    ///
    /// * Returns an error if the index create command fails (e.g. index already exists, invalid expression, cluster error).
    ///
    /// # See also
    ///
    /// * [`create_index_on_bin`](Self::create_index_on_bin), [`drop_index`](Self::drop_index), [`crate::expressions`]
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` with the expression `int_bin("a") == 500`:
    ///
    /// ```rust,edition2021
    /// # extern crate aerospike;
    /// # use aerospike::expressions::{eq, int_bin, int_val};
    /// # use aerospike::*;
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or(String::from("127.0.0.1:3000"));
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let policy = AdminPolicy::default();
    /// let expression = eq(int_bin("a".to_string()), int_val(500));
    /// match client.create_index_using_expression(
    ///     &policy, "foo", "bar", "idx_foo_bar_baz",
    ///     IndexType::Numeric, CollectionIndexType::Default, &expression,
    /// ).await {
    ///     Err(err) => println!("Failed to create index: {}", err),
    ///     Ok(task) => { /* wait for task with task.wait_till_complete(None).await */ }
    /// }
    /// # }
    /// ```
    pub async fn create_index_using_expression(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        index_name: &str,
        index_type: IndexType,
        collection_index_type: CollectionIndexType,
        expression: &Expression,
    ) -> Result<IndexTask> {
        self.create_index(
            policy,
            namespace,
            set_name,
            "",
            index_name,
            index_type,
            collection_index_type,
            Some(expression),
            None,
        )
        .await
    }

    /// Create a secondary index on a bin or using expression. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// For internal use only. `bin_name` and expression cannot both be passed.
    async fn create_index(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
        collection_index_type: CollectionIndexType,
        expression: Option<&Expression>,
        ctx: Option<&[CdtContext]>,
    ) -> Result<IndexTask> {
        let mut cmd = String::with_capacity(1024);
        let node = self.cluster.get_random_node()?;
        let node_version = node.version();
        if node_version >= &Version::new(8, 1, 0, 0) {
            cmd.push_str("sindex-create:namespace=");
        } else {
            cmd.push_str("sindex-create:ns=");
        }
        cmd.push_str(namespace);

        if !set_name.is_empty() {
            cmd.push_str(";set=");
            cmd.push_str(set_name);
        }

        cmd.push_str(";indexname=");
        cmd.push_str(index_name);

        if let Some(ctx) = ctx {
            if !ctx.is_empty() {
                let size = pack_ctx_for_index(&mut None, ctx)?;
                let mut buf = Buffer::new(0);
                buf.resize_buffer(size)?;
                let _ = pack_ctx_for_index(&mut Some(&mut buf), ctx);
                let ctx_str = base64::encode(&buf.data_buffer);
                cmd.push_str(";context=");
                cmd.push_str(&ctx_str);
            }
        }

        if let Some(expression) = expression {
            let size = expression.size()?;
            let mut buf = Buffer::new(0);
            buf.resize_buffer(size)?;
            let _ = expression.pack(&mut Some(&mut buf));
            let exp_str = base64::encode(&buf.data_buffer);
            cmd.push_str(";exp=");
            cmd.push_str(&exp_str);
        }

        if CollectionIndexType::Default != collection_index_type {
            cmd.push_str("indextype=");
            cmd.push_str(&format!("{collection_index_type}"));
        }

        if bin_name.is_empty() {
            cmd.push_str(";type=");
        } else if node_version >= &Version::new(8, 1, 0, 0) {
            cmd.push_str(";bin=");
            cmd.push_str(bin_name);
            cmd.push_str(";type=");
        } else {
            cmd.push_str(";indexdata=");
            cmd.push_str(bin_name);
            cmd.push(',');
        }

        cmd.push_str(&format!("{index_type}"));

        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("Error creating index"))?;
        Ok(IndexTask::new(
            Arc::clone(&self.cluster),
            namespace.to_string(),
            index_name.to_string(),
        ))
    }

    /// Delete secondary index.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `namespace` — Namespace of the index.
    /// * `set_name` — Set name (can be empty for namespace-wide index).
    /// * `index_name` — Name of the index to drop.
    ///
    /// # Returns
    ///
    /// `Ok(DropIndexTask)` to poll or wait for index drop to complete across the cluster.
    ///
    /// # Errors
    ///
    /// * Returns an error if the drop command fails (e.g., index not found, cluster error).
    ///
    /// # See also
    ///
    /// * [`create_index_on_bin`](Self::create_index_on_bin), [`create_index_using_expression`](Self::create_index_using_expression), [`DropIndexTask`](crate::task::DropIndexTask)
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.drop_index(&AdminPolicy::default(), "test", "myset", "idx_mybin").await {
    ///     Ok(task) => { /* wait for task.wait_till_complete(None).await */ }
    ///     Err(err) => eprintln!("Failed to drop index: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn drop_index(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        index_name: &str,
    ) -> Result<DropIndexTask> {
        let node = self.cluster.get_random_node()?;
        let node_version = node.version();

        let mut cmd = String::with_capacity(100);

        if node_version >= &Version::new(8, 1, 0, 0) {
            cmd.push_str("sindex-delete:namespace=");
        } else {
            cmd.push_str("sindex-delete:ns=");
        }
        cmd.push_str(namespace);

        if !set_name.is_empty() {
            cmd.push_str(";set=");
            cmd.push_str(set_name);
        }

        cmd.push_str(";indexname=");
        cmd.push_str(index_name);

        self.send_info_cmd(policy, node, &cmd)
            .await
            .map_err(|e| e.chain_error("Error dropping index"))?;
        Ok(DropIndexTask::new(
            Arc::clone(&self.cluster),
            namespace.to_string(),
            index_name.to_string(),
        ))
    }

    async fn send_info_cmd(&self, policy: &AdminPolicy, node: Arc<Node>, cmd: &str) -> Result<()> {
        let response = node.info(policy, &[cmd]).await?;
        if let Some(response) = response.get(cmd) {
            if response != "ok" && !response.is_empty() {
                return Err(Self::parse_info_error(response));
            }
        }

        Ok(())
    }

    /// Creates a new user with password and roles. Clear-text password will be hashed using bcrypt
    /// before sending to server.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username.
    /// * `password` — Clear-text password (hashed with bcrypt before send).
    /// * `roles` — List of role names to assign.
    ///
    /// # Errors
    ///
    /// * Returns an error if user creation fails (e.g. user exists, invalid roles, cluster error). Requires cluster security to be enabled.
    ///
    /// # See also
    ///
    /// * [`drop_user`](Self::drop_user), [`grant_roles`](Self::grant_roles), [`query_users`](Self::query_users)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.create_user(
    ///     &AdminPolicy::default(),
    ///     "newuser",
    ///     "password",
    ///     &["read-write"],
    /// ).await {
    ///     Ok(()) => { /* user created */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn create_user(
        &self,
        policy: &AdminPolicy,
        user: &str,
        password: &str,
        roles: &[&str],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::create_user(policy, &cluster, user, password, roles).await
    }

    /// Creates a new user PKI user with roles. PKI users are authenticated via TLS and a certificate instead of a password.
    /// Supported by Aerospike Server v8.1+ Enterprise.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username (typically matches certificate CN or SAN).
    /// * `roles` — List of role names to assign.
    ///
    /// # Errors
    ///
    /// * Returns an error if PKI user creation fails. Requires Enterprise server 8.1+ and PKI auth.
    ///
    /// # See also
    ///
    /// * [`create_user`](Self::create_user), [`drop_user`](Self::drop_user)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.create_pki_user(
    ///     &AdminPolicy::default(),
    ///     "pki_user",
    ///     &["read-write"],
    /// ).await {
    ///     Ok(()) => { /* PKI user created */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn create_pki_user(
        &self,
        policy: &AdminPolicy,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::create_user(policy, &cluster, user, "nopassword", roles).await
    }

    /// Removes a user from the cluster.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username to remove.
    ///
    /// # Errors
    ///
    /// * Returns an error if the user does not exist or drop fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`create_user`](Self::create_user), [`query_users`](Self::query_users)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.drop_user(&AdminPolicy::default(), "olduser").await {
    ///     Ok(()) => { /* user removed */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn drop_user(&self, policy: &AdminPolicy, user: &str) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::drop_user(policy, &cluster, user).await
    }

    /// Changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username whose password to change.
    /// * `password` — New clear-text password (hashed with bcrypt before send).
    ///
    /// # Errors
    ///
    /// * Returns an error if the user is not found, auth mode does not allow password change (e.g., PKI), or cluster error.
    ///
    /// # See also
    ///
    /// * [`create_user`](Self::create_user)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.change_password(
    ///     &AdminPolicy::default(),
    ///     "myuser",
    ///     "new_password",
    /// ).await {
    ///     Ok(()) => { /* password changed */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn change_password(
        &self,
        policy: &AdminPolicy,
        user: &str,
        password: &str,
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        let auth_mode = self.cluster.client_policy().auth_mode;
        match auth_mode {
            crate::AuthMode::Internal(u, _) | crate::AuthMode::External(u, _) if u == user => {
                AdminCommand::change_password(policy, &cluster, user, password).await
            }
            crate::AuthMode::PKI => Err(Error::ClientError(
                "Can't change PKI user's password".into(),
            )),
            _ => AdminCommand::set_password(policy, &cluster, user, password).await,
        }
    }

    /// Adds roles to user's list of roles.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username.
    /// * `roles` — Role names to add.
    ///
    /// # Errors
    ///
    /// * Returns an error if the user or any role is invalid, or cluster/security error.
    ///
    /// # See also
    ///
    /// * [`revoke_roles`](Self::revoke_roles), [`query_users`](Self::query_users), [`create_role`](Self::create_role)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.grant_roles(
    ///     &AdminPolicy::default(),
    ///     "myuser",
    ///     &["read-write", "sys-admin"],
    /// ).await {
    ///     Ok(()) => { /* roles granted */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn grant_roles(
        &self,
        policy: &AdminPolicy,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::grant_roles(policy, &cluster, user, roles).await
    }

    /// Removes roles from user's list of roles.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Username.
    /// * `roles` — Role names to remove.
    ///
    /// # Errors
    ///
    /// * Returns an error if the user is not found or revoke fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`grant_roles`](Self::grant_roles), [`query_users`](Self::query_users)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.revoke_roles(
    ///     &AdminPolicy::default(),
    ///     "myuser",
    ///     &["sys-admin"],
    /// ).await {
    ///     Ok(()) => { /* roles revoked */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn revoke_roles(
        &self,
        policy: &AdminPolicy,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::revoke_roles(policy, &cluster, user, roles).await
    }

    /// Retrieves users and their roles.
    /// If None is passed for the user argument, all users will be returned.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `user` — Specific username to query, or `None` to return all users.
    ///
    /// # Returns
    ///
    /// `Ok(Vec<User>)` — list of users with their roles.
    ///
    /// # Errors
    ///
    /// * Returns an error if the query fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`query_roles`](Self::query_roles), [`create_user`](Self::create_user), [`User`]
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.query_users(&AdminPolicy::default(), None).await {
    ///     Ok(users) => println!("{} user(s)", users.len()),
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn query_users(&self, policy: &AdminPolicy, user: Option<&str>) -> Result<Vec<User>> {
        let cluster = self.cluster.clone();
        AdminCommand::query_users(policy, &cluster, user).await
    }

    /// Retrieves roles and their privileges.
    /// If None is passed for the role argument, all roles will be returned.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role` — Specific role name to query, or `None` to return all roles.
    ///
    /// # Returns
    ///
    /// `Ok(Vec<Role>)` — list of roles with their privileges.
    ///
    /// # Errors
    ///
    /// * Returns an error if the query fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`query_users`](Self::query_users), [`create_role`](Self::create_role), [`Role`]
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.query_roles(&AdminPolicy::default(), None).await {
    ///     Ok(roles) => println!("{} role(s)", roles.len()),
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn query_roles(&self, policy: &AdminPolicy, role: Option<&str>) -> Result<Vec<Role>> {
        let cluster = self.cluster.clone();
        AdminCommand::query_roles(policy, &cluster, role).await
    }

    /// Creates a user-defined role.
    /// Quotas require server security configuration "enable-quotas" to be set to true.
    /// Pass 0 for quota values for no limit.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Name of the role.
    /// * `privileges` — List of [`Privilege`] to grant.
    /// * `allowlist` — IP allowlist for the role (empty to allow all).
    /// * `read_quota` — Max reads per second (0 = no limit); requires enable-quotas.
    /// * `write_quota` — Max writes per second (0 = no limit); requires enable-quotas.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role already exists, privileges are invalid, or cluster/security error.
    ///
    /// # See also
    ///
    /// * [`drop_role`](Self::drop_role), [`grant_privileges`](Self::grant_privileges), [`query_roles`](Self::query_roles)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy, Privilege, PrivilegeCode};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let privileges = [Privilege::new(PrivilegeCode::ReadWrite, None, None)];
    /// match client.create_role(
    ///     &AdminPolicy::default(),
    ///     "custom_role",
    ///     &privileges,
    ///     &[],
    ///     0,
    ///     0,
    /// ).await {
    ///     Ok(()) => { /* role created */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn create_role(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
        allowlist: &[&str],
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::create_role(
            policy,
            &cluster,
            role_name,
            privileges,
            allowlist,
            read_quota,
            write_quota,
        )
        .await
    }

    /// Removes a user-defined role.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Name of the role to remove.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role does not exist or drop fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`create_role`](Self::create_role), [`grant_roles`](Self::grant_roles)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.drop_role(&AdminPolicy::default(), "old_role").await {
    ///     Ok(()) => { /* role removed */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn drop_role(&self, policy: &AdminPolicy, role_name: &str) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::drop_role(policy, &cluster, role_name).await
    }

    /// Grants privileges to a user-defined role.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Role to modify.
    /// * `privileges` — [`Privilege`] list to add.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role is not found, privileges are invalid, or cluster/security error.
    ///
    /// # See also
    ///
    /// * [`revoke_privileges`](Self::revoke_privileges), [`create_role`](Self::create_role), [`Privilege`]
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy, Privilege, PrivilegeCode};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let privileges = [Privilege::new(PrivilegeCode::Read, Some("test".to_string()), None)];
    /// match client.grant_privileges(
    ///     &AdminPolicy::default(),
    ///     "my_role",
    ///     &privileges,
    /// ).await {
    ///     Ok(()) => { /* privileges granted */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn grant_privileges(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::grant_privileges(policy, &cluster, role_name, privileges).await
    }

    /// Revokes privileges from a user-defined role.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Role to modify.
    /// * `privileges` — [`Privilege`] list to revoke.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role is not found or revoke fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`grant_privileges`](Self::grant_privileges), [`create_role`](Self::create_role)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy, Privilege, PrivilegeCode};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let privileges = [Privilege::new(PrivilegeCode::Write, None, None)];
    /// match client.revoke_privileges(
    ///     &AdminPolicy::default(),
    ///     "my_role",
    ///     &privileges,
    /// ).await {
    ///     Ok(()) => { /* privileges revoked */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn revoke_privileges(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::revoke_privileges(policy, &cluster, role_name, privileges).await
    }

    /// Sets IP address allowlist for a role.
    /// If allowlist is nil or empty, it removes existing allowlist from role.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Role to modify.
    /// * `allowlist` — IP addresses/CIDRs allowed for this role; empty to remove allowlist.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role is not found or the command fails (cluster/security error).
    ///
    /// # See also
    ///
    /// * [`create_role`](Self::create_role), [`set_quotas`](Self::set_quotas)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.set_allowlist(
    ///     &AdminPolicy::default(),
    ///     "my_role",
    ///     &["192.168.1.0/24", "10.0.0.1"],
    /// ).await {
    ///     Ok(()) => { /* allowlist set */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn set_allowlist(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        allowlist: &[&str],
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::set_allowlist(policy, &cluster, role_name, allowlist).await
    }

    /// Sets maximum reads/writes per second limits for a role.
    /// If a quota is zero, the limit is removed.
    /// Quotas require server security configuration "enable-quotas" to be set to true.
    /// Pass 0 for quota values for no limit.
    ///
    /// # Arguments
    ///
    /// * `policy` — Admin policy (timeout).
    /// * `role_name` — Role to modify.
    /// * `read_quota` — Max reads per second; 0 to remove limit.
    /// * `write_quota` — Max writes per second; 0 to remove limit.
    ///
    /// # Errors
    ///
    /// * Returns an error if the role is not found, quotas are not enabled on the server, or cluster/security error.
    ///
    /// # See also
    ///
    /// * [`create_role`](Self::create_role), [`set_allowlist`](Self::set_allowlist)
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use aerospike::{Client, ClientPolicy, AdminPolicy};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.set_quotas(
    ///     &AdminPolicy::default(),
    ///     "my_role",
    ///     1000,
    ///     500,
    /// ).await {
    ///     Ok(()) => { /* quotas set */ }
    ///     Err(err) => eprintln!("Error: {}", err),
    /// }
    /// # }
    /// ```
    pub async fn set_quotas(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        let cluster = self.cluster.clone();
        AdminCommand::set_quotas(policy, &cluster, role_name, read_quota, write_quota).await
    }

    fn parse_info_error(response: &str) -> Error {
        static RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^(?i)(fail|error)((:|=)(?P<code>[0-9]+))?((:|=)(?P<msg>.+))?$").unwrap()
        });

        if !RE.is_match(response) {
            return Error::ServerError(ResultCode::ServerError, false, response.into());
        }

        // 'm' is a 'Match', and 'as_str()' returns the matching part of the haystack.
        let parts = RE
            .captures(response)
            .map(|caps| {
                let code = caps.name("code").map_or_else(
                    || ResultCode::ServerError,
                    |code| ResultCode::from(code.as_str().parse::<u8>().unwrap()),
                );
                let msg = caps.name("msg").map_or(response, |msg| msg.as_str());

                (code, msg)
            })
            .unwrap();

        Error::ServerError(parts.0, false, parts.1.into())
    }
}
