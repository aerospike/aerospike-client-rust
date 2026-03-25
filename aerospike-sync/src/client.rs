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

use std::str;
use std::sync::Arc;
use std::vec::Vec;

use crate::expressions::Expression;
use aerospike_core::errors::Result;
use aerospike_core::operations::{CdtContext, Operation};
use aerospike_core::query::PartitionFilter;
use aerospike_core::DropIndexTask;
use aerospike_core::UdfRemoveTask;
use aerospike_core::{
    AdminPolicy, BatchOperation, BatchPolicy, BatchRecord, Bin, Bins, ClientPolicy,
    CollectionIndexType, ExecuteTask, IndexTask, IndexType, Key, Node, Privilege, QueryPolicy,
    ReadPolicy, Record, Recordset, RegisterTask, Role, Statement, ToHosts, UDFLang, User, Value,
    WritePolicy,
};
use futures::executor::block_on;

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
    async_client: aerospike_core::Client,
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
    /// connections fail and the policy's `fail_
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
    /// ```rust,edition2021
    /// use aerospike_sync::{Client, ClientPolicy};
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// ```
    pub fn new(policy: &ClientPolicy, hosts: &(dyn ToHosts + Send + Sync)) -> Result<Self> {
        let client = block_on(aerospike_core::Client::new(policy, hosts))?;
        Ok(Client {
            async_client: client,
        })
    }

    /// Closes the connection to the Aerospike cluster.
    pub fn close(&self) -> Result<()> {
        block_on(self.async_client.close())?;
        Ok(())
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    pub fn is_connected(&self) -> bool {
        self.async_client.is_connected()
    }

    /// Returns a list of the names of the active server nodes in the cluster.
    pub fn node_names(&self) -> Vec<String> {
        self.async_client.node_names()
    }

    /// Return node given its name.
    pub fn get_node(&self, name: &str) -> Result<Arc<Node>> {
        self.async_client.get_node(name)
    }

    /// Returns a list of active server nodes in the cluster.
    pub fn nodes(&self) -> Vec<Arc<Node>> {
        self.async_client.nodes()
    }

    /// Read record for the specified key. Depending on the bins value provided, all record bins,
    /// only selected record bins or only the record headers will be returned. The policy can be
    /// used to specify timeouts.
    ///
    /// # Examples
    ///
    /// Fetch specified bins for a record with the given key.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get(&ReadPolicy::default(), &key, ["a", "b"]) {
    ///     Ok(record)
    ///         => println!("a={:?}", record.bins.get("a")),
    ///     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// ```
    ///
    /// Determine the remaining time-to-live of a record.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get(&ReadPolicy::default(), &key, Bins::None) {
    ///     Ok(record) => {
    ///         match record.time_to_live() {
    ///             None => println!("record never expires"),
    ///             Some(duration) => println!("ttl: {} secs", duration.as_secs()),
    ///         }
    ///     },
    ///     Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// ```
    ///
    /// # Panics
    /// Panics if the return is invalid
    pub fn get<T>(&self, policy: &ReadPolicy, key: &Key, bins: T) -> Result<Record>
    where
        T: Into<Bins> + Send + Sync + 'static,
    {
        block_on(self.async_client.get(policy, key, bins))
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
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let bins = Bins::from(["name", "age"]);
    /// let bpr = BatchReadPolicy::default();
    /// let mut batch_ops = vec![];
    /// for i in 0..10 {
    ///   let key = as_key!("test", "test", i);
    ///   batch_ops.push(BatchOperation::read(&bpr, key, bins.clone()));
    /// }
    /// match client.batch(&BatchPolicy::default(), &batch_ops) {
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
    pub fn batch(
        &self,
        policy: &BatchPolicy,
        batch_records: &[BatchOperation],
    ) -> Result<Vec<BatchRecord>> {
        block_on(self.async_client.batch(policy, batch_records))
    }

    /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
    /// how the transaction is handled when the record already exists.
    ///
    /// # Examples
    ///
    /// Write a record with a single integer bin.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// match client.put(&WritePolicy::default(), &key, &vec![bin]) {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    ///
    /// Write a record with an expiration of 10 seconds.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::Seconds(10);
    /// match client.put(&policy, &key, &vec![bin]) {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    pub fn put<'a>(&self, policy: &'a WritePolicy, key: &'a Key, bins: &'a [Bin]) -> Result<()> {
        block_on(self.async_client.put(policy, key, bins))
    }

    /// Add integer bin values to existing record bin values. The policy specifies the transaction
    /// timeout, record expiration and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    ///
    /// # Examples
    ///
    /// Add two integer values to two existing bin values.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bina = as_bin!("a", 1);
    /// let binb = as_bin!("b", 2);
    /// let bins = vec![bina, binb];
    /// match client.add(&WritePolicy::default(), &key, &bins) {
    ///     Ok(()) => println!("Record updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    pub fn add<'a>(&self, policy: &'a WritePolicy, key: &'a Key, bins: &'a [Bin]) -> Result<()> {
        block_on(self.async_client.add(policy, key, bins))
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn append<'a>(&self, policy: &'a WritePolicy, key: &'a Key, bins: &'a [Bin]) -> Result<()> {
        block_on(self.async_client.append(policy, key, bins))
    }

    /// Prepend bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn prepend<'a>(
        &self,
        policy: &'a WritePolicy,
        key: &'a Key,
        bins: &'a [Bin],
    ) -> Result<()> {
        block_on(self.async_client.prepend(policy, key, bins))
    }

    /// Delete record for specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    ///
    /// # Examples
    ///
    /// Delete a record.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.delete(&WritePolicy::default(), &key) {
    ///     Ok(true) => println!("Record deleted"),
    ///     Ok(false) => println!("Record did not exist"),
    ///     Err(err) => println!("Error deleting record: {}", err),
    /// }
    /// ```
    pub fn delete(&self, policy: &WritePolicy, key: &Key) -> Result<bool> {
        block_on(self.async_client.delete(policy, key))
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    ///
    /// # Examples
    ///
    /// Reset a record's time to expiration to the default ttl for the namespace.
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::NamespaceDefault;
    /// match client.touch(&policy, &key) {
    ///     Ok(()) => println!("Record expiration updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    pub fn touch(&self, policy: &WritePolicy, key: &Key) -> Result<()> {
        block_on(self.async_client.touch(policy, key))
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    pub fn exists(&self, policy: &ReadPolicy, key: &Key) -> Result<bool> {
        block_on(self.async_client.exists(policy, key))
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
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("a", 42);
    /// let ops = vec![
    ///     operations::add(&bin),
    ///     operations::get_bin("a"),
    /// ];
    /// match client.operate(&WritePolicy::default(), &key, &ops) {
    ///     Ok(record) => println!("The new value is {}", record.bins.get("a").unwrap()),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// ```
    /// # Panics
    ///  Panics if the return is invalid
    pub fn operate(&self, policy: &WritePolicy, key: &Key, ops: &[Operation]) -> Result<Record> {
        block_on(self.async_client.operate(policy, key, ops))
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting language for UDFs at the moment.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
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
    /// client.register_udf(&AdminPolicy::default(), code.as_bytes(),
    ///                     "example.lua", UDFLang::Lua).unwrap();
    /// ```
    pub fn register_udf(
        &self,
        policy: &AdminPolicy,
        udf_body: &[u8],
        server_path: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        block_on(
            self.async_client
                .register_udf(policy, udf_body, server_path, language),
        )
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting language for UDFs at the moment.
    pub fn register_udf_from_file(
        &self,
        policy: &AdminPolicy,
        client_path: &str,
        server_path: &str,
        language: UDFLang,
    ) -> Result<RegisterTask> {
        block_on(self.async_client.register_udf_from_file(
            policy,
            client_path,
            server_path,
            language,
        ))
    }

    /// Remove a user-defined function (UDF) module from the server.
    pub fn remove_udf(&self, policy: &AdminPolicy, server_path: &str) -> Result<UdfRemoveTask> {
        block_on(self.async_client.remove_udf(policy, server_path))
    }

    /// Execute a user-defined function on the server and return the results. The function operates
    /// on a single record. The UDF package name is required to locate the UDF.
    ///
    /// # Panics
    /// Panics if the return is invalid
    pub fn execute_udf(
        &self,
        policy: &WritePolicy,
        key: &Key,
        server_path: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<Option<Value>> {
        block_on(
            self.async_client
                .execute_udf(policy, key, server_path, function_name, args),
        )
    }

    /// Execute a query on all server nodes and return a record iterator. The query executor puts
    /// records on a queue in separate threads. The calling thread concurrently pops records off
    /// the queue through the record iterator.
    ///
    /// # Examples
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let stmt = Statement::new("test", "test", Bins::All);
    /// let pf = PartitionFilter::all();
    /// match client.query(&QueryPolicy::default(), pf, stmt) {
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
    pub fn query(
        &self,
        policy: &QueryPolicy,
        partition_filter: PartitionFilter,
        statement: Statement,
    ) -> Result<Arc<Recordset>> {
        block_on(self.async_client.query(policy, partition_filter, statement))
    }

    /// Execute a query and apply operations to matching records on the server.
    /// Returns an `ExecuteTask` that can be used to monitor the progress of the
    /// background job.
    pub fn query_operate(
        &self,
        write_policy: &WritePolicy,
        statement: Statement,
        operations: &[Operation],
    ) -> Result<ExecuteTask> {
        block_on(
            self.async_client
                .query_operate(write_policy, statement, operations),
        )
    }

    /// Apply a user-defined function to records matching the statement filter.
    /// Returns an `ExecuteTask` that can be used to monitor the progress of the
    /// background job.
    pub fn query_execute_udf(
        &self,
        write_policy: &WritePolicy,
        statement: Statement,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<ExecuteTask> {
        block_on(self.async_client.query_execute_udf(
            write_policy,
            statement,
            package_name,
            function_name,
            args,
        ))
    }

    /// Sets XDR filter for given datacenter name and namespace. The expression filter indicates
    /// which records XDR should ship to the datacenter.
    /// Pass nil as filter to remove the current filter on the server.
    pub async fn set_xdr_filter(
        &self,
        policy: &AdminPolicy,
        datacenter: &str,
        namespace: &str,
        filter_expression: Option<&Expression>,
    ) -> Result<()> {
        block_on(
            self.async_client
                .set_xdr_filter(policy, datacenter, namespace, filter_expression),
        )
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
    pub fn truncate(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        before_nanos: i64,
    ) -> Result<()> {
        block_on(
            self.async_client
                .truncate(policy, namespace, set_name, before_nanos),
        )
    }

    /// Create a secondary index on a bin. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` and bin `baz`:
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// // Returns a Future; use futures::executor::block_on or similar from sync code.
    /// let _ = client.create_index_on_bin(&AdminPolicy::default(), "foo", "bar", "baz",
    ///     "idx_foo_bar_baz", IndexType::Numeric, CollectionIndexType::Default, None);
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
        block_on(self.async_client.create_index_on_bin(
            policy,
            namespace,
            set_name,
            bin_name,
            index_name,
            index_type,
            collection_index_type,
            ctx,
        ))
    }

    /// Create a secondary index using an expression. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Examples
    ///
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` with the expression `int_bin("a") == 500`:
    ///
    /// ```rust,edition2021
    /// # use aerospike_sync::*;
    /// # use aerospike_sync::expressions::{Expression, eq, int_bin, int_val};
    /// # let rt = tokio::runtime::Runtime::new().unwrap();
    /// # let _guard = rt.enter();
    ///
    /// # let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    /// # let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// let fe: Expression = eq(int_bin("a".to_string()), int_val(500));
    /// // Returns a Future; use futures::executor::block_on or similar from sync code.
    /// let _ = client.create_index_using_expression(&AdminPolicy::default(), "foo", "bar",
    ///     "idx_foo_bar_baz", IndexType::Numeric, CollectionIndexType::Default, &fe);
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
        block_on(self.async_client.create_index_using_expression(
            policy,
            namespace,
            set_name,
            index_name,
            index_type,
            collection_index_type,
            expression,
        ))
    }

    /// Delete secondary index.
    pub fn drop_index(
        &self,
        policy: &AdminPolicy,
        namespace: &str,
        set_name: &str,
        index_name: &str,
    ) -> Result<DropIndexTask> {
        block_on(
            self.async_client
                .drop_index(policy, namespace, set_name, index_name),
        )
    }

    /// Creates a new user with password and roles. Clear-text password will be hashed using bcrypt
    /// before sending to server.
    pub async fn create_user(
        &self,
        policy: &AdminPolicy,
        user: &str,
        password: &str,
        roles: &[&str],
    ) -> Result<()> {
        block_on(self.async_client.create_user(policy, user, password, roles))
    }

    /// Removes a user from the cluster.
    pub async fn drop_user(&self, policy: &AdminPolicy, user: &str) -> Result<()> {
        block_on(self.async_client.drop_user(policy, user))
    }

    /// Changes a user's password. Clear-text password will be hashed using bcrypt before sending to server.
    pub async fn change_password(
        &self,
        policy: &AdminPolicy,
        user: &str,
        password: &str,
    ) -> Result<()> {
        block_on(self.async_client.change_password(policy, user, password))
    }

    /// Adds roles to user's list of roles.
    pub async fn grant_roles(
        &self,
        policy: &AdminPolicy,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        block_on(self.async_client.grant_roles(policy, user, roles))
    }

    /// Removes roles from user's list of roles.
    pub async fn revoke_roles(
        &self,
        policy: &AdminPolicy,
        user: &str,
        roles: &[&str],
    ) -> Result<()> {
        block_on(self.async_client.revoke_roles(policy, user, roles))
    }

    // Retrieves users and their roles.
    // If None is passed for the user argument, all users will be returned.
    pub async fn query_users(&self, policy: &AdminPolicy, user: Option<&str>) -> Result<Vec<User>> {
        block_on(self.async_client.query_users(policy, user))
    }

    /// Creates a user-defined role.
    /// Quotas require server security configuration "enable-quotas" to be set to true.
    /// Pass 0 for quota values for no limit.
    pub async fn create_role(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
        allowlist: &[&str],
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        block_on(self.async_client.create_role(
            policy,
            role_name,
            privileges,
            allowlist,
            read_quota,
            write_quota,
        ))
    }

    /// Retrieves roles and their privileges.
    /// If None is passed for the role argument, all roles will be returned.
    pub async fn query_roles(&self, policy: &AdminPolicy, role: Option<&str>) -> Result<Vec<Role>> {
        block_on(self.async_client.query_roles(policy, role))
    }

    /// Removes a user-defined role.
    pub async fn drop_role(&self, policy: &AdminPolicy, role_name: &str) -> Result<()> {
        block_on(self.async_client.drop_role(policy, role_name))
    }

    /// Grants privileges to a user-defined role.
    pub async fn grant_privileges(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        block_on(
            self.async_client
                .grant_privileges(policy, role_name, privileges),
        )
    }

    /// Revokes privileges from a user-defined role.
    pub async fn revoke_privileges(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        privileges: &[Privilege],
    ) -> Result<()> {
        block_on(
            self.async_client
                .revoke_privileges(policy, role_name, privileges),
        )
    }

    /// Sets IP address allowlist for a role.
    /// If allowlist is nil or empty, it removes existing allowlist from role.
    pub async fn set_allowlist(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        allowlist: &[&str],
    ) -> Result<()> {
        block_on(
            self.async_client
                .set_allowlist(policy, role_name, allowlist),
        )
    }

    /// Sets maximum reads/writes per second limits for a role.
    /// If a quota is zero, the limit is removed.
    /// Quotas require server security configuration "enable-quotas" to be set to true.
    /// Pass 0 for quota values for no limit.
    pub async fn set_quotas(
        &self,
        policy: &AdminPolicy,
        role_name: &str,
        read_quota: u32,
        write_quota: u32,
    ) -> Result<()> {
        block_on(
            self.async_client
                .set_quotas(policy, role_name, read_quota, write_quota),
        )
    }
}
