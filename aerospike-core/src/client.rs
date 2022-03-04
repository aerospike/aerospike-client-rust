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
use std::str;
use std::sync::Arc;
use std::vec::Vec;

use crate::batch::BatchExecutor;
use crate::cluster::{Cluster, Node};
use crate::commands::{
    DeleteCommand, ExecuteUDFCommand, ExistsCommand, OperateCommand, QueryCommand, ReadCommand,
    ScanCommand, TouchCommand, WriteCommand,
};
use crate::errors::{ErrorKind, Result, ResultExt};
use crate::net::ToHosts;
use crate::operations::{Operation, OperationType};
use crate::policy::{BatchPolicy, ClientPolicy, QueryPolicy, ReadPolicy, ScanPolicy, WritePolicy};
use crate::task::{IndexTask, RegisterTask};
use crate::{
    BatchRead, Bin, Bins, CollectionIndexType, IndexType, Key, Record, Recordset, ResultCode,
    Statement, UDFLang, Value,
};
use aerospike_rt::fs::File;
#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
use aerospike_rt::io::AsyncReadExt;
#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
use futures::AsyncReadExt;

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
pub struct Client {
    cluster: Arc<Cluster>,
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
    /// # Examples
    ///
    /// Using an environment variable to set the list of seed hosts.
    ///
    /// ```rust,edition2018
    /// use aerospike::{Client, ClientPolicy};
    ///
    /// let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// ```
    pub async fn new(policy: &ClientPolicy, hosts: &(dyn ToHosts + Send + Sync)) -> Result<Self> {
        let hosts = hosts.to_hosts()?;
        let cluster = Cluster::new(policy.clone(), &hosts).await?;

        Ok(Client { cluster })
    }

    /// Closes the connection to the Aerospike cluster.
    pub async fn close(&self) -> Result<()> {
        self.cluster.close().await?;
        Ok(())
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    pub async fn is_connected(&self) -> bool {
        self.cluster.is_connected().await
    }

    /// Returns a list of the names of the active server nodes in the cluster.
    pub async fn node_names(&self) -> Vec<String> {
        self.cluster
            .nodes()
            .await
            .iter()
            .map(|node| node.name().to_owned())
            .collect()
    }

    /// Return node given its name.
    pub async fn get_node(&self, name: &str) -> Result<Arc<Node>> {
        self.cluster.get_node_by_name(name).await
    }

    /// Returns a list of active server nodes in the cluster.
    pub async fn nodes(&self) -> Vec<Arc<Node>> {
        self.cluster.nodes().await
    }

    /// Read record for the specified key. Depending on the bins value provided, all record bins,
    /// only selected record bins or only the record headers will be returned. The policy can be
    /// used to specify timeouts.
    ///
    /// # Examples
    ///
    /// Fetch specified bins for a record with the given key.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get(&ReadPolicy::default(), &key, ["a", "b"]).await {
    ///     Ok(record)
    ///         => println!("a={:?}", record.bins.get("a")),
    ///     Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// ```
    ///
    /// Determine the remaining time-to-live of a record.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
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
    ///     Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the return is invalid
    pub async fn get<T>(&self, policy: &ReadPolicy, key: &Key, bins: T) -> Result<Record>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let bins = bins.into();
        let mut command = ReadCommand::new(policy, self.cluster.clone(), key, bins);
        command.execute().await?;
        Ok(command.record.unwrap())
    }

    /// Read multiple record for specified batch keys in one batch call. This method allows
    /// different namespaces/bins to be requested for each key in the batch. If the `BatchRead` key
    /// field is not found, the corresponding record field will be `None`. The policy can be used
    /// to specify timeouts and maximum concurrent threads. This method requires Aerospike Server
    /// version >= 3.6.0.
    ///
    /// # Examples
    ///
    /// Fetch multiple records in a single client request
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let bins = Bins::from(["name", "age"]);
    /// let mut batch_reads = vec![];
    /// for i in 0..10 {
    ///   let key = as_key!("test", "test", i);
    ///   batch_reads.push(BatchRead::new(key, bins.clone()));
    /// }
    /// match client.batch_get(&BatchPolicy::default(), batch_reads).await {
    ///     Ok(results) => {
    ///       for result in results {
    ///         match result.record {
    ///           Some(record) => println!("{:?} => {:?}", result.key, record.bins),
    ///           None => println!("No such record: {:?}", result.key),
    ///         }
    ///       }
    ///     }
    ///     Err(err)
    ///         => println!("Error executing batch request: {}", err),
    /// }
    /// ```
    pub async fn batch_get(
        &self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<Vec<BatchRead>> {
        let executor = BatchExecutor::new(self.cluster.clone());
        executor.execute_batch_read(policy, batch_reads).await
    }

    /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
    /// how the transaction is handled when the record already exists.
    ///
    /// # Examples
    ///
    /// Write a record with a single integer bin.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// match client.put(&WritePolicy::default(), &key, &vec![bin]).await {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    ///
    /// Write a record with an expiration of 10 seconds.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
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
    /// ```
    pub async fn put<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<()> {
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
    /// timeout, record expiration and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    ///
    /// # Examples
    ///
    /// Add two integer values to two existing bin values.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
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
    /// ```
    pub async fn add<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Incr);
        command.execute().await
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub async fn append<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<()> {
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
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub async fn prepend<'a, 'b>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin<'b>],
    ) -> Result<()> {
        let mut command = WriteCommand::new(
            policy,
            self.cluster.clone(),
            key,
            bins,
            OperationType::Prepend,
        );
        command.execute().await
    }

    /// Delete record for specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    ///
    /// # Examples
    ///
    /// Delete a record.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.delete(&WritePolicy::default(), &key).await {
    ///     Ok(true) => println!("Record deleted"),
    ///     Ok(false) => println!("Record did not exist"),
    ///     Err(err) => println!("Error deleting record: {}", err),
    /// }
    /// ```
    pub async fn delete(&self, policy: &WritePolicy, key: &Key) -> Result<bool> {
        let mut command = DeleteCommand::new(policy, self.cluster.clone(), key);
        command.execute().await?;
        Ok(command.existed)
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    ///
    /// # Examples
    ///
    /// Reset a record's time to expiration to the default ttl for the namespace.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::NamespaceDefault;
    /// match client.touch(&policy, &key).await {
    ///     Ok(()) => println!("Record expiration updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    pub async fn touch(&self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let mut command = TouchCommand::new(policy, self.cluster.clone(), key);
        command.execute().await
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    pub async fn exists(&self, policy: &WritePolicy, key: &Key) -> Result<bool> {
        let mut command = ExistsCommand::new(policy, self.cluster.clone(), key);
        command.execute().await?;
        Ok(command.exists)
    }

    /// Perform multiple read/write operations on a single key in one batch call.
    ///
    /// Operations on scalar values, lists and maps can be performed in the same call.
    ///
    /// Operations execute in the order specified by the client application.
    ///
    /// # Examples
    ///
    /// Add an integer value to an existing record and then read the result, all in one database
    /// call.
    ///
    /// ```rust,edition2018
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("a", 42);
    /// let ops = vec![
    ///     operations::add(&bin),
    ///     operations::get_bin("a"),
    /// ];
    /// match client.operate(&WritePolicy::default(), &key, &ops).await {
    ///     Ok(record) => println!("The new value is {}", record.bins.get("a").unwrap()),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    /// # Panics
    ///  Panics if the return is invalid
    pub async fn operate(
        &self,
        policy: &WritePolicy,
        key: &Key,
        ops: &[Operation<'_>],
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
    /// Lua is the only supported scripting laungauge for UDFs at the moment.
    ///
    /// # Examples
    ///
    /// ```rust,edition2018
    /// # extern crate aerospike;
    /// # use aerospike::*;
    ///
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
    /// client.register_udf(code.as_bytes(),
    ///                     "example.lua", UDFLang::Lua).await.unwrap();
    /// ```
    pub async fn register_udf(
        &self,
        udf_body: &[u8],
        udf_name: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        let udf_body = base64::encode(udf_body);

        let cmd = format!(
            "udf-put:filename={};content={};content-len={};udf-type={};",
            udf_name,
            udf_body,
            udf_body.len(),
            language
        );
        let node = self.cluster.get_random_node().await?;
        let response = node.info(&[&cmd]).await?;

        if let Some(msg) = response.get("error") {
            let msg = base64::decode(msg)?;
            let msg = str::from_utf8(&msg)?;
            bail!(
                "UDF Registration failed: {}, file: {}, line: {}, message: {}",
                response.get("error").unwrap_or(&"-".to_string()),
                response.get("file").unwrap_or(&"-".to_string()),
                response.get("line").unwrap_or(&"-".to_string()),
                msg
            );
        }

        Ok(RegisterTask::new(
            Arc::clone(&self.cluster),
            udf_name.to_string(),
        ))
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting laungauge for UDFs at the moment.
    pub async fn register_udf_from_file(
        &self,
        client_path: &str,
        udf_name: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        let path = Path::new(client_path);
        let mut file = File::open(&path).await?;
        let mut udf_body: Vec<u8> = vec![];
        file.read_to_end(&mut udf_body).await?;

        self.register_udf(&udf_body, udf_name, language).await
    }

    /// Remove a user-defined function (UDF) module from the server.
    pub async fn remove_udf(&self, udf_name: &str, language: UDFLang) -> Result<()> {
        let cmd = format!("udf-remove:filename={}.{};", udf_name, language);
        let node = self.cluster.get_random_node().await?;
        // Sample response: {"udf-remove:filename=file_name.LUA;": "ok"}
        let response = node.info(&[&cmd]).await?;

        match response.get(&cmd).map(String::as_str) {
            Some("ok") => Ok(()),
            _ => bail!("UDF Remove failed: {:?}", response),
        }
    }

    /// Execute a user-defined function on the server and return the results. The function operates
    /// on a single record. The UDF package name is required to locate the UDF.
    ///
    /// # Panics
    /// Panics if the return is invalid
    pub async fn execute_udf(
        &self,
        policy: &WritePolicy,
        key: &Key,
        udf_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<Option<Value>> {
        let mut command = ExecuteUDFCommand::new(
            policy,
            self.cluster.clone(),
            key,
            udf_name,
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
                bail!("{:?}", value);
            }
        }

        Err("Invalid UDF return value".into())
    }

    /// Read all records in the specified namespace and set and return a record iterator. The scan
    /// executor puts records on a queue in separate threads. The calling thread concurrently pops
    /// records off the queue through the record iterator. Up to `policy.max_concurrent_nodes`
    /// nodes are scanned in parallel. If concurrent nodes is set to zero, the server nodes are
    /// read in series.
    ///
    /// # Examples
    ///
    /// ```rust,edition2018
    /// # extern crate aerospike;
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).wait.unwrap();
    /// match client.scan(&ScanPolicy::default(), "test", "demo", Bins::All).await {
    ///     Ok(records) => {
    ///         let mut count = 0;
    ///         for record in &*records {
    ///             match record {
    ///                 Ok(record) => count += 1,
    ///                 Err(err) => panic!("Error executing scan: {}", err),
    ///             }
    ///         }
    ///         println!("Records: {}", count);
    ///     },
    ///     Err(err) => println!("Failed to execute scan: {}", err),
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the async block fails
    pub async fn scan<T>(
        &self,
        policy: &ScanPolicy,
        namespace: &str,
        set_name: &str,
        bins: T,
    ) -> Result<Arc<Recordset>>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let bins = bins.into();
        let nodes = self.cluster.nodes().await;
        let recordset = Arc::new(Recordset::new(policy.record_queue_size, nodes.len()));
        for node in nodes {
            let partitions = self.cluster.node_partitions(node.as_ref(), namespace).await;
            let node = node.clone();
            let recordset = recordset.clone();
            let policy = policy.clone();
            let namespace = namespace.to_owned();
            let set_name = set_name.to_owned();
            let bins = bins.clone();

            aerospike_rt::spawn(async move {
                let mut command = ScanCommand::new(
                    &policy, node, &namespace, &set_name, bins, recordset, partitions,
                );
                command.execute().await.unwrap();
            })
            .await;
        }
        Ok(recordset)
    }

    /// Read all records in the specified namespace and set for one node only and return a record
    /// iterator. The scan executor puts records on a queue in separate threads. The calling thread
    /// concurrently pops records off the queue through the record iterator. Up to
    /// `policy.max_concurrent_nodes` nodes are scanned in parallel. If concurrent nodes is set to
    /// zero, the server nodes are read in series.
    ///
    /// # Panics
    /// panics if the async block fails
    pub async fn scan_node<T>(
        &self,
        policy: &ScanPolicy,
        node: Arc<Node>,
        namespace: &str,
        set_name: &str,
        bins: T,
    ) -> Result<Arc<Recordset>>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        let partitions = self.cluster.node_partitions(node.as_ref(), namespace).await;
        let bins = bins.into();
        let recordset = Arc::new(Recordset::new(policy.record_queue_size, 1));
        let t_recordset = recordset.clone();
        let policy = policy.clone();
        let namespace = namespace.to_owned();
        let set_name = set_name.to_owned();

        aerospike_rt::spawn(async move {
            let mut command = ScanCommand::new(
                &policy,
                node,
                &namespace,
                &set_name,
                bins,
                t_recordset,
                partitions,
            );
            command.execute().await.unwrap();
        })
        .await;

        Ok(recordset)
    }

    /// Execute a query on all server nodes and return a record iterator. The query executor puts
    /// records on a queue in separate threads. The calling thread concurrently pops records off
    /// the queue through the record iterator.
    ///
    /// # Examples
    ///
    /// ```rust,edition2018
    /// # extern crate aerospike;
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// let stmt = Statement::new("test", "test", Bins::All);
    /// match client.query(&QueryPolicy::default(), stmt).await {
    ///     Ok(records) => {
    ///         for record in &*records {
    ///             // .. process record
    ///         }
    ///     },
    ///     Err(err) => println!("Error fetching record: {}", err),
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the async block fails
    pub async fn query(
        &self,
        policy: &QueryPolicy,
        statement: Statement,
    ) -> Result<Arc<Recordset>> {
        statement.validate()?;
        let statement = Arc::new(statement);

        let nodes = self.cluster.nodes().await;
        let recordset = Arc::new(Recordset::new(policy.record_queue_size, nodes.len()));
        for node in nodes {
            let partitions = self
                .cluster
                .node_partitions(node.as_ref(), &statement.namespace)
                .await;
            let node = node.clone();
            let t_recordset = recordset.clone();
            let policy = policy.clone();
            let statement = statement.clone();
            aerospike_rt::spawn(async move {
                let mut command =
                    QueryCommand::new(&policy, node, statement, t_recordset, partitions);
                command.execute().await.unwrap();
            })
            .await;
        }
        Ok(recordset)
    }

    /// Execute a query on a single server node and return a record iterator. The query executor
    /// puts records on a queue in separate threads. The calling thread concurrently pops records
    /// off the queue through the record iterator.
    ///
    /// # Panics
    /// Panics when the async block fails
    pub async fn query_node(
        &self,
        policy: &QueryPolicy,
        node: Arc<Node>,
        statement: Statement,
    ) -> Result<Arc<Recordset>> {
        statement.validate()?;

        let recordset = Arc::new(Recordset::new(policy.record_queue_size, 1));
        let t_recordset = recordset.clone();
        let policy = policy.clone();
        let statement = Arc::new(statement);
        let partitions = self
            .cluster
            .node_partitions(node.as_ref(), &statement.namespace)
            .await;

        aerospike_rt::spawn(async move {
            let mut command = QueryCommand::new(&policy, node, statement, t_recordset, partitions);
            command.execute().await.unwrap();
        })
        .await;

        Ok(recordset)
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
    /// namespace/set recardless of last update time.
    pub async fn truncate(&self, namespace: &str, set_name: &str, before_nanos: i64) -> Result<()> {
        let mut cmd = String::with_capacity(160);
        cmd.push_str("truncate:namespace=");
        cmd.push_str(namespace);

        if !set_name.is_empty() {
            cmd.push_str(";set=");
            cmd.push_str(set_name);
        }

        if before_nanos > 0 {
            cmd.push_str(";lut=");
            cmd.push_str(&format!("{}", before_nanos));
        }

        self.send_info_cmd(&cmd)
            .await
            .chain_err(|| "Error truncating ns/set")
    }

    /// Create a secondary index on a bin containing scalar values. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` and bin `baz`:
    ///
    /// ```rust,edition2018
    /// # extern crate aerospike;
    /// # use aerospike::*;
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).await.unwrap();
    /// match client.create_index("foo", "bar", "baz",
    ///     "idx_foo_bar_baz", IndexType::Numeric).await {
    ///     Err(err) => println!("Failed to create index: {}", err),
    ///     _ => {}
    /// }
    /// ```
    pub async fn create_index(
        &self,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
    ) -> Result<IndexTask> {
        self.create_complex_index(
            namespace,
            set_name,
            bin_name,
            index_name,
            index_type,
            CollectionIndexType::Default,
        )
        .await?;
        Ok(IndexTask::new(
            Arc::clone(&self.cluster),
            namespace.to_string(),
            index_name.to_string(),
        ))
    }

    /// Create a complex secondary index on a bin containing scalar, list or map values. This
    /// asynchronous server call returns before the command is complete.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_complex_index(
        &self,
        namespace: &str,
        set_name: &str,
        bin_name: &str,
        index_name: &str,
        index_type: IndexType,
        collection_index_type: CollectionIndexType,
    ) -> Result<()> {
        let cit_str: String = if let CollectionIndexType::Default = collection_index_type {
            "".to_string()
        } else {
            format!("indextype={};", collection_index_type)
        };
        let cmd = format!(
            "sindex-create:ns={};set={};indexname={};numbins=1;{}indexdata={},{};\
             priority=normal",
            namespace, set_name, index_name, cit_str, bin_name, index_type
        );
        self.send_info_cmd(&cmd)
            .await
            .chain_err(|| "Error creating index")
    }

    /// Delete secondary index.
    pub async fn drop_index(
        &self,
        namespace: &str,
        set_name: &str,
        index_name: &str,
    ) -> Result<()> {
        let set_name: String = if let "" = set_name {
            "".to_string()
        } else {
            format!("set={};", set_name)
        };
        let cmd = format!(
            "sindex-delete:ns={};{}indexname={}",
            namespace, set_name, index_name
        );
        self.send_info_cmd(&cmd)
            .await
            .chain_err(|| "Error dropping index")
    }

    async fn send_info_cmd(&self, cmd: &str) -> Result<()> {
        let node = self.cluster.get_random_node().await?;
        let response = node.info(&[cmd]).await?;

        if let Some(v) = response.values().next() {
            if v.to_uppercase() == "OK" {
                return Ok(());
            } else if v.starts_with("FAIL:") {
                let result = v.split(':').nth(1).unwrap().parse::<u8>()?;
                bail!(ErrorKind::ServerError(ResultCode::from(result)));
            }
        }

        bail!(ErrorKind::BadResponse(
            "Unexpected sindex info command response".to_string()
        ))
    }
}
