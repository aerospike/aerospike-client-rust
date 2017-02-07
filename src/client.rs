// Copyright 2015-2017 Aerospike, Inc.
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

use std::sync::Arc;
use std::vec::Vec;
use std::thread;
use std::str;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use rustc_serialize::base64::{ToBase64, FromBase64, STANDARD};
use threadpool::ThreadPool;

use errors::*;
use Bin;
use CollectionIndexType;
use IndexType;
use Key;
use Record;
use Recordset;
use ResultCode;
use Statement;
use UDFLang;
use Value;
use cluster::{Cluster, Node};
use commands::{ReadCommand, WriteCommand, DeleteCommand, TouchCommand, ExistsCommand,
               ReadHeaderCommand, OperateCommand, ExecuteUDFCommand, ScanCommand, QueryCommand};
use net::ToHosts;
use operations::{Operation, OperationType};
use policy::{ClientPolicy, ReadPolicy, WritePolicy, ScanPolicy, QueryPolicy};


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
    thread_pool: ThreadPool,
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
    /// ```rust
    /// use aerospike::{Client, ClientPolicy};
    ///
    /// let hosts = std::env::var("AEROSPIKE_HOSTS").unwrap();
    /// let client = Client::new(&ClientPolicy::default(), &hosts).unwrap();
    /// ```
    pub fn new(policy: &ClientPolicy, hosts: &ToHosts) -> Result<Self> {
        let hosts = try!(hosts.to_hosts());
        let cluster = try!(Cluster::new(policy.clone(), &hosts));
        let thread_pool = ThreadPool::new(policy.thread_pool_size);

        Ok(Client {
            cluster: cluster,
            thread_pool: thread_pool,
        })
    }

    /// Closes the connection to the Aerospike cluster.
    pub fn close(&self) -> Result<()> {
        self.cluster.close()
    }

    /// Returns `true` if the client is connected to any cluster nodes.
    pub fn is_connected(&self) -> bool {
        self.cluster.is_connected()
    }

    /// Read record header and bins for the specified key. The policy can be used to specify
    /// timeouts. If no bin names are specified (`bin_names = None`), then the entire record will
    /// be read.
    ///
    /// # Examples
    ///
    /// Fetch specified bins for a record with the given key.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bins = vec!["a"];
    /// match client.get(&ReadPolicy::default(), &key, Some(&bins)) {
    ///     Ok(record)
    ///         => println!("a={:?}", record.bins.get("a")),
    ///     Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _))
    ///         => println!("No such record: {}", key),
    ///     Err(err)
    ///         => println!("Error fetching record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn get(&self,
                       policy: &ReadPolicy,
                       key: &Key,
                       bin_names: Option<&[&str]>)
                       -> Result<Record> {
        let mut command = ReadCommand::new(policy, self.cluster.clone(), key, bin_names);
        try!(command.execute());
        Ok(command.record.unwrap())
    }

    /// Read record generation and expiration for the specified key. Bins are not read. The policy
    /// can be used to specify timeouts.
    ///
    /// # Examples
    ///
    /// Determine the remaining time-to-live of a record.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.get_header(&ReadPolicy::default(), &key) {
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
    /// # }
    /// ```
    pub fn get_header(&self,
                              policy: &ReadPolicy,
                              key: &Key)
                              -> Result<Record> {
        let mut command = ReadHeaderCommand::new(policy, self.cluster.clone(), key);
        try!(command.execute());
        Ok(command.record.unwrap())
    }

    /// Write record bin(s). The policy specifies the transaction timeout, record expiration and
    /// how the transaction is handled when the record already exists.
    ///
    /// # Examples
    ///
    /// Write a record with a single integer bin.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// match client.put(&WritePolicy::default(), &key, &vec![&bin]) {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    ///
    /// Write a record with an expiration of 10 seconds.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bin = as_bin!("i", 42);
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::Seconds(10);
    /// match client.put(&policy, &key, &vec![&bin]) {
    ///     Ok(()) => println!("Record written"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn put(&self,
                       policy: &WritePolicy,
                       key: &Key,
                       bins: &[&Bin])
                       -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Write);
        command.execute()
    }

    /// Add integer bin values to existing record bin values. The policy specifies the transaction
    /// timeout, record expiration and how the transaction is handled when the record already
    /// exists. This call only works for integer values.
    ///
    /// # Examples
    ///
    /// Add two integer values to two existing bin values.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let bina = as_bin!("a", 1);
    /// let binb = as_bin!("b", 2);
    /// let bins = vec![&bina, &binb];
    /// match client.add(&WritePolicy::default(), &key, &bins) {
    ///     Ok(()) => println!("Record updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn add(&self,
                       policy: &WritePolicy,
                       key: &Key,
                       bins: &[&Bin])
                       -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Incr);
        command.execute()
    }

    /// Append bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn append(&self,
                          policy: &WritePolicy,
                          key: &Key,
                          bins: &[&Bin])
                          -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Append);
        command.execute()
    }

    /// Prepend bin string values to existing record bin values. The policy specifies the
    /// transaction timeout, record expiration and how the transaction is handled when the record
    /// already exists. This call only works for string values.
    pub fn prepend(&self,
                           policy: &WritePolicy,
                           key: &Key,
                           bins: &[&Bin])
                           -> Result<()> {
        let mut command =
            WriteCommand::new(policy, self.cluster.clone(), key, bins, OperationType::Prepend);
        command.execute()
    }

    /// Delete record for specified key. The policy specifies the transaction timeout.
    /// The call returns `true` if the record existed on the server before deletion.
    ///
    /// # Examples
    ///
    /// Delete a record.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// match client.delete(&WritePolicy::default(), &key) {
    ///     Ok(true) => println!("Record deleted"),
    ///     Ok(false) => println!("Record did not exist"),
    ///     Err(err) => println!("Error deleting record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn delete(&self,
                          policy: &WritePolicy,
                          key: &Key)
                          -> Result<bool> {
        let mut command = DeleteCommand::new(policy, self.cluster.clone(), key);
        try!(command.execute());
        Ok(command.existed)
    }

    /// Reset record's time to expiration using the policy's expiration. Fail if the record does
    /// not exist.
    ///
    /// # Examples
    ///
    /// Reset a record's time to expiration to the default ttl for the namespace.
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let key = as_key!("test", "test", "mykey");
    /// let mut policy = WritePolicy::default();
    /// policy.expiration = policy::Expiration::NamespaceDefault;
    /// match client.touch(&policy, &key) {
    ///     Ok(()) => println!("Record expiration updated"),
    ///     Err(err) => println!("Error writing record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn touch(&self, policy: &WritePolicy, key: &Key) -> Result<()> {
        let mut command = TouchCommand::new(policy, self.cluster.clone(), key);
        command.execute()
    }

    /// Determine if a record key exists. The policy can be used to specify timeouts.
    pub fn exists(&self,
                          policy: &WritePolicy,
                          key: &Key)
                          -> Result<bool> {
        let mut command = ExistsCommand::new(policy, self.cluster.clone(), key);
        try!(command.execute());
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
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
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
    /// # }
    /// ```
    pub fn operate(&self,
                           policy: &WritePolicy,
                           key: &Key,
                           ops: &[Operation])
                           -> Result<Record> {
        let mut command = OperateCommand::new(policy, self.cluster.clone(), key, ops);
        try!(command.execute());
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
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
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
    /// client.register_udf(&WritePolicy::default(), code.as_bytes(), "example.lua", UDFLang::Lua).unwrap();
    /// # }
    /// ```
    pub fn register_udf(&self,
                                policy: &WritePolicy,
                                udf_body: &[u8],
                                udf_name: &str,
                                language: UDFLang)
                                -> Result<()> {
        let udf_body = udf_body.to_base64(STANDARD);

        let cmd = format!("udf-put:filename={};content={};content-len={};udf-type={};",
                          udf_name,
                          udf_body,
                          udf_body.len(),
                          language);
        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(policy.base_policy.timeout, &[&cmd]));

        if let Some(msg) = response.get("error") {
            let msg = try!(msg.from_base64());
            let msg = try!(str::from_utf8(&msg));
            bail!("UDF Registration failed: {}, file: {}, line: {}, message: {}",
                  response.get("error").unwrap_or(&"-".to_string()),
                  response.get("file").unwrap_or(&"-".to_string()),
                  response.get("line").unwrap_or(&"-".to_string()),
                  msg);
        }

        Ok(())
    }

    /// Register a package containing user-defined functions (UDF) with the cluster. This
    /// asynchronous server call will return before the command is complete. The client registers
    /// the UDF package with a single, random cluster node; from there a copy will get distributed
    /// to all other cluster nodes automatically.
    ///
    /// Lua is the only supported scripting laungauge for UDFs at the moment.
    pub fn register_udf_from_file(&self,
                                          policy: &WritePolicy,
                                          client_path: &str,
                                          udf_name: &str,
                                          language: UDFLang)
                                          -> Result<()> {

        let path = Path::new(client_path);
        let mut file = try!(File::open(&path));
        let mut udf_body: Vec<u8> = vec![];
        try!(file.read_to_end(&mut udf_body));

        self.register_udf(policy, &udf_body, udf_name, language)
    }

    /// Remove a user-defined function (UDF) module from the server.
    pub fn remove_udf(&self,
                              policy: &WritePolicy,
                              udf_name: &str,
                              language: UDFLang)
                              -> Result<()> {

        let cmd = format!("udf-remove:filename={}.{};", udf_name, language);
        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(policy.base_policy.timeout, &[&cmd]));

        if let Some(_) = response.get("ok") {
            return Ok(());
        }

        bail!("UDF Remove failed: {:?}", response)
    }

    /// Execute a user-defined function on the server and return the results. The function operates
    /// on a single record. The UDF package name is required to locate the UDF.
    pub fn execute_udf(&self,
                               policy: &WritePolicy,
                               key: &Key,
                               udf_name: &str,
                               function_name: &str,
                               args: Option<&[Value]>)
                               -> Result<Option<Value>> {

        let mut command = ExecuteUDFCommand::new(policy,
                                                      self.cluster.clone(),
                                                      key,
                                                      udf_name,
                                                      function_name,
                                                      args);
        try!(command.execute());

        let record = command.read_command.record.as_ref().unwrap().clone();

        // User defined functions don't have to return a value.
        if record.bins.len() == 0 {
            return Ok(None);
        }

        for (key, value) in record.bins.iter() {
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
    /// records off the queue through the record iterator. Up to `policy.max_concurrent_nodes` nodes are
    /// scanned in parallel. If concurrent nodes is set to zero, the server nodes are read in
    /// series.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// match client.scan(&ScanPolicy::default(), "test", "demo", None) {
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
    /// # }
    /// ```
    pub fn scan(&self,
                        policy: &ScanPolicy,
                        namespace: &str,
                        set_name: &str,
                        bin_names: Option<&[&str]>)
                        -> Result<Arc<Recordset>> {

        let bin_names = match bin_names {
            None => None,
            Some(bin_names) => {
                let bin_names: Vec<_> = bin_names.iter().cloned().map(String::from).collect();
                Some(bin_names)
            }
        };

        let nodes = self.cluster.nodes();
        let recordset = Arc::new(Recordset::new(policy.record_queue_size, nodes.len()));
        for node in nodes {
            let node = node.clone();
            let recordset = recordset.clone();
            let policy = policy.to_owned();
            let namespace = namespace.to_owned();
            let set_name = set_name.to_owned();
            let bin_names = bin_names.to_owned();

            thread::spawn(move || {
                let mut command = ScanCommand::new(&policy, node, &namespace, &set_name, &bin_names, recordset);
                command.execute().unwrap();
            });

        }
        Ok(recordset)
    }

    /// Read all records in the specified namespace and set for one node only and return a record
    /// iterator. The scan executor puts records on a queue in separate threads. The calling thread
    /// concurrently pops records off the queue through the record iterator. Up to
    /// `policy.max_concurrent_nodes` nodes are scanned in parallel. If concurrent nodes is set to
    /// zero, the server nodes are read in series.
    pub fn scan_node(&self,
                             policy: &ScanPolicy,
                             node: Node,
                             namespace: &str,
                             set_name: &str,
                             bin_names: Option<&[&str]>)
                             -> Result<Arc<Recordset>> {


        let bin_names = match bin_names {
            None => None,
            Some(bin_names) => {
                let bin_names: Vec<_> = bin_names.iter().cloned().map(String::from).collect();
                Some(bin_names)
            }
        };

        let recordset = Arc::new(Recordset::new(policy.record_queue_size, 1));
        let node = Arc::new(node).clone();
        let t_recordset = recordset.clone();
        let policy = policy.to_owned();
        let namespace = namespace.to_owned();
        let set_name = set_name.to_owned();
        let bin_names = bin_names.to_owned();

        self.thread_pool.execute(move || {
            let mut command = ScanCommand::new(&policy,
                                               node,
                                               &namespace,
                                               &set_name,
                                               &bin_names,
                                               t_recordset);
            command.execute().unwrap();
        });

        Ok(recordset)
    }

    /// Execute a query on all server nodes and return a record iterator. The query executor puts
    /// records on a queue in separate threads. The calling thread concurrently pops records off
    /// the queue through the record iterator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// let stmt = Statement::new("test", "test", None);
    /// match client.query(&QueryPolicy::default(), stmt) {
    ///     Ok(records) => {
    ///         for record in &*records {
    ///             // .. process record
    ///         }
    ///     },
    ///     Err(err) => println!("Error fetching record: {}", err),
    /// }
    /// # }
    /// ```
    pub fn query(&self,
                         policy: &QueryPolicy,
                         statement: Statement)
                         -> Result<Arc<Recordset>> {

        try!(statement.validate());
        let statement = Arc::new(statement);

        let nodes = self.cluster.nodes();
        let recordset = Arc::new(Recordset::new(policy.record_queue_size, nodes.len()));
        for node in nodes {
            let node = node.clone();
            let t_recordset = recordset.clone();
            let policy = policy.to_owned();
            let statement = statement.clone();

            self.thread_pool.execute(move || {
                let mut command = QueryCommand::new(&policy, node, statement, t_recordset);
                command.execute().unwrap();
            });
        }
        Ok(recordset)
    }

    /// Execute a query on a single server node and return a record iterator. The query executor
    /// puts records on a queue in separate threads. The calling thread concurrently pops records
    /// off the queue through the record iterator.
    pub fn query_node(&self,
                              policy: &QueryPolicy,
                              node: Node,
                              statement: Statement)
                              -> Result<Arc<Recordset>> {

        try!(statement.validate());

        let recordset = Arc::new(Recordset::new(policy.record_queue_size, 1));
        let node = Arc::new(node).clone();
        let t_recordset = recordset.clone();
        let policy = policy.to_owned();
        let statement = Arc::new(statement).clone();

        self.thread_pool.execute(move || {
            let mut command = QueryCommand::new(&policy, node, statement, t_recordset);
            command.execute().unwrap();
        });

        Ok(recordset)
    }

    /// Create a secondary index on a bin containing scalar values. This asynchronous server call
    /// returns before the command is complete.
    ///
    /// # Examples
    /// 
    /// The following example creates an index `idx_foo_bar_baz`. The index is in namespace `foo`
    /// within set `bar` and bin `baz`:
    ///
    /// ```rust
    /// # #[macro_use] extern crate aerospike;
    /// # use aerospike::*;
    /// # fn main() {
    /// # let client = Client::new(&ClientPolicy::default(), &std::env::var("AEROSPIKE_HOSTS").unwrap()).unwrap();
    /// match client.create_index(&WritePolicy::default(), "foo", "bar", "baz",
    ///     "idx_foo_bar_baz", IndexType::Numeric) {
    ///     Err(err) => println!("Failed to create index: {}", err),
    ///     _ => {}
    /// }
    /// # }
    /// ```
    pub fn create_index(&self,
                        policy: &WritePolicy,
                        namespace: &str,
                        set_name: &str,
                        bin_name: &str,
                        index_name: &str,
                        index_type: IndexType)
                        -> Result<()> {
        self.create_complex_index(policy,
                                  namespace,
                                  set_name,
                                  bin_name,
                                  index_name,
                                  index_type,
                                  CollectionIndexType::Default)

    }

    /// Create a complex secondary index on a bin containing scalar, list or map values. This
    /// asynchronous server call returns before the command is complete.
    pub fn create_complex_index(&self,
                                policy: &WritePolicy,
                                namespace: &str,
                                set_name: &str,
                                bin_name: &str,
                                index_name: &str,
                                index_type: IndexType,
                                collection_index_type: CollectionIndexType)
                                -> Result<()> {
        let cit_str: String = match collection_index_type {
            CollectionIndexType::Default => "".to_string(),
            _ => format!("indextype={};", collection_index_type),
        };
        let cmd = format!("sindex-create:ns={};set={};indexname={};numbins=1;{}indexdata={},{};priority=normal",
                          namespace, set_name, index_name, cit_str, bin_name, index_type);
        self.send_sindex_cmd(cmd, &policy).chain_err(|| "Error creating index")
    }

    /// Delete secondary index.
    pub fn drop_index(&self,
                      policy: &WritePolicy,
                      namespace: &str,
                      set_name: &str,
                      index_name: &str)
                      -> Result<()> {

        let set_name: String = match set_name {
            "" => "".to_string(),
            _ => format!("set={};", set_name),
        };
        let cmd = format!("sindex-delete:ns={};{}indexname={}",
                          namespace, set_name, index_name);
        self.send_sindex_cmd(cmd, &policy).chain_err(|| "Error dropping index")
    }

    fn send_sindex_cmd(&self, cmd: String, policy: &WritePolicy) -> Result<()> {
        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(policy.base_policy.timeout, &[&cmd]));

        for v in response.values() {
            if v.to_uppercase() == "OK" {
                return Ok(())
            } else if v.starts_with("FAIL:200") {
                bail!(ErrorKind::ServerError(ResultCode::from(200)));
            } else if v.starts_with("FAIL:201") {
                bail!(ErrorKind::ServerError(ResultCode::from(201)));
            } else {
                break;
            }
        }

        bail!(ErrorKind::BadResponse("Unexpected sindex info command response".to_string()))
    }
}
