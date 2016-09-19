// Copyright 2015-2016 Aerospike, Inc.
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

use log::LogLevel;
use env_logger;

use std::collections::{HashMap, VecDeque};
use std::sync::{RwLock, Arc, Mutex};
use std::vec::Vec;
use std::thread;
use std::str;
use std::time::{Instant, Duration};
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use rustc_serialize::base64::{ToBase64, FromBase64, STANDARD};
use threadpool::ThreadPool;

use internal::wait_group::WaitGroup;
use net::Host;
use cluster::{Cluster, Node};
use common::operation;
use common::{Key, Record, Bin, UDFLang, Recordset, Statement, IndexType, CollectionIndexType};
use command::read_command::ReadCommand;
use command::write_command::WriteCommand;
use command::delete_command::DeleteCommand;
use command::touch_command::TouchCommand;
use command::exists_command::ExistsCommand;
use command::read_header_command::ReadHeaderCommand;
use command::operate_command::OperateCommand;
use command::execute_udf_command::ExecuteUDFCommand;
use command::scan_command::ScanCommand;
use command::query_command::QueryCommand;
use value::Value;

use policy::{ClientPolicy, ReadPolicy, WritePolicy, ScanPolicy, QueryPolicy};
use error::{AerospikeResult, ResultCode, AerospikeError};


// Client encapsulates an Aerospike cluster.
// All database operations are available against this object.
pub struct Client {
    pub cluster: Arc<Cluster>,
    thread_pool: ThreadPool,
}

unsafe impl Send for Client {}
unsafe impl Sync for Client {}

impl Client {
    pub fn new(policy: &ClientPolicy, hosts: &[Host]) -> AerospikeResult<Self> {
        let cluster = try!(Cluster::new(policy.clone(), hosts));

        Ok(Client {
            cluster: cluster,
            thread_pool: ThreadPool::new(policy.thread_pool_size),
        })
    }

    pub fn close(&self) -> AerospikeResult<()> {
        self.cluster.close()
    }

    pub fn is_connected(&self) -> bool {
        self.cluster.is_connected()
    }

    pub fn nodes(&self) -> AerospikeResult<Vec<Arc<Node>>> {
        Ok(self.cluster.nodes())
    }

    pub fn get<'a, 'b>(&'a self,
                       policy: &'a ReadPolicy,
                       key: &'a Key,
                       bin_names: Option<&'a [&'b str]>)
                       -> AerospikeResult<Record> {
        let mut command = try!(ReadCommand::new(policy, self.cluster.clone(), key, bin_names));
        try!(command.execute());
        Ok(command.record.unwrap())
    }

    pub fn get_header<'a, 'b>(&'a self,
                              policy: &'a ReadPolicy,
                              key: &'a Key)
                              -> AerospikeResult<Arc<Record>> {
        let mut command = try!(ReadHeaderCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.record.as_ref().unwrap().clone())
    }

    pub fn put<'a, 'b>(&'a self,
                       policy: &'a WritePolicy,
                       key: &'a Key,
                       bins: &'a [&'b Bin])
                       -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy,
                                                 self.cluster.clone(),
                                                 key,
                                                 bins,
                                                 operation::WRITE));
        command.execute()
    }

    pub fn add<'a, 'b>(&'a self,
                       policy: &'a WritePolicy,
                       key: &'a Key,
                       bins: &'a [&'b Bin])
                       -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy,
                                                 self.cluster.clone(),
                                                 key,
                                                 bins,
                                                 operation::ADD));
        command.execute()
    }

    pub fn append<'a, 'b>(&'a self,
                          policy: &'a WritePolicy,
                          key: &'a Key,
                          bins: &'a [&'b Bin])
                          -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy,
                                                 self.cluster.clone(),
                                                 key,
                                                 bins,
                                                 operation::APPEND));
        command.execute()
    }

    pub fn prepend<'a, 'b>(&'a self,
                           policy: &'a WritePolicy,
                           key: &'a Key,
                           bins: &'a [&'b Bin])
                           -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy,
                                                 self.cluster.clone(),
                                                 key,
                                                 bins,
                                                 operation::PREPEND));
        command.execute()
    }

    pub fn delete<'a, 'b>(&'a self,
                          policy: &'a WritePolicy,
                          key: &'a Key)
                          -> AerospikeResult<bool> {
        let mut command = try!(DeleteCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.existed)
    }

    pub fn touch<'a, 'b>(&'a self, policy: &'a WritePolicy, key: &'a Key) -> AerospikeResult<()> {
        let mut command = try!(TouchCommand::new(policy, self.cluster.clone(), key));
        command.execute()
    }

    pub fn exists<'a, 'b>(&'a self,
                          policy: &'a WritePolicy,
                          key: &'a Key)
                          -> AerospikeResult<bool> {
        let mut command = try!(ExistsCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.exists)
    }

    pub fn operate<'a, 'b>(&'a self,
                           policy: &'a WritePolicy,
                           key: &'a Key,
                           ops: &'a [operation::Operation<'a>])
                           -> AerospikeResult<Record> {
        let mut command = try!(OperateCommand::new(policy, self.cluster.clone(), key, ops));
        try!(command.execute());
        Ok(command.read_command.record.unwrap())
    }

    /// //////////////////////////////////////////////////////////////////////////

    pub fn register_udf<'a, 'b>(&'a self,
                                policy: &'a WritePolicy,
                                udf_body: &'a [u8],
                                udf_name: &'a str,
                                language: UDFLang)
                                -> AerospikeResult<()> {
        let udf_body = udf_body.to_base64(STANDARD);

        let cmd = format!("udf-put:filename={};content={};content-len={};udf-type={};",
                          udf_name,
                          udf_body,
                          udf_body.len(),
                          language);
        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(&[&cmd]));

        if let Some(msg) = response.get("error") {
            let msg = try!(msg.from_base64());
            let msg = try!(str::from_utf8(&msg));
            return Err(AerospikeError::new(ResultCode::COMMAND_REJECTED,
                                           Some(format!("UDF Registration failed: {}\nFile: \
                                                         {}\nLine: {}\nMessage: {}",
                                                        response.get("error")
                                                                .unwrap_or(&"-".to_string()),
                                                        response.get("file")
                                                                .unwrap_or(&"-".to_string()),
                                                        response.get("line")
                                                                .unwrap_or(&"-".to_string()),
                                                        msg))));
        }

        Ok(())
    }

    pub fn register_udf_from_file<'a, 'b>(&'a self,
                                          policy: &'a WritePolicy,
                                          client_path: &'a str,
                                          udf_name: &'a str,
                                          language: UDFLang)
                                          -> AerospikeResult<()> {

        let path = Path::new(client_path);
        let mut file = try!(File::open(&path));
        let mut udf_body: Vec<u8> = vec![];
        try!(file.read_to_end(&mut udf_body));

        self.register_udf(policy, &udf_body, udf_name, language)
    }

    pub fn remove_udf<'a, 'b>(&'a self,
                              policy: &'a WritePolicy,
                              udf_name: &'a str,
                              language: UDFLang)
                              -> AerospikeResult<()> {

        let cmd = format!("udf-remove:filename={}.{};", udf_name, language);
        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(&[&cmd]));

        if let Some(msg) = response.get("ok") {
            return Ok(());
        }

        Err(AerospikeError::new(ResultCode::COMMAND_REJECTED,
                                Some(format!("UDF Remove failed: {:?}", response))))
    }

    pub fn execute_udf<'a, 'b>(&'a self,
                               policy: &'a WritePolicy,
                               key: &'a Key,
                               udf_name: &'a str,
                               function_name: &'a str,
                               args: Option<&[Value]>)
                               -> AerospikeResult<Option<Value>> {

        let mut command = try!(ExecuteUDFCommand::new(policy,
                                                      self.cluster.clone(),
                                                      key,
                                                      udf_name,
                                                      function_name,
                                                      args));
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
                return Err(AerospikeError::new(ResultCode::SERVER_ERROR,
                                               Some(format!("{:?}", value))));
            }
        }

        Err(AerospikeError::new(ResultCode::UDF_BAD_RESPONSE,
                                Some("Invalid UDF return value".to_string())))
    }

    pub fn scan<'a, 'b>(&'a self,
                        policy: &'a ScanPolicy,
                        namespace: &'a str,
                        set_name: &'a str,
                        bin_names: Option<&'a [&'b str]>)
                        -> AerospikeResult<Arc<Recordset>> {


        let bin_names = match bin_names {
            None => None,
            Some(bin_names) => {
                let bin_names: Vec<_> = bin_names.iter().cloned().map(String::from).collect();
                Some(bin_names)
            }
        };

        let nodes = self.cluster.nodes();
        let mut recordset = Arc::new(try!(Recordset::new(policy.record_queue_size, nodes.len())));
        for node in nodes {
            let node = node.clone();
            let recordset = recordset.clone();
            let policy = policy.to_owned();
            let namespace = namespace.to_owned();
            let set_name = set_name.to_owned();
            let bin_names = bin_names.to_owned();

            thread::spawn(move || {
                let mut command = ScanCommand::new(&policy,
                                                   node,
                                                   &namespace,
                                                   &set_name,
                                                   &bin_names,
                                                   recordset)
                                      .unwrap();
                command.execute().unwrap();
            });

        }
        Ok(recordset)
    }

    pub fn scan_node<'a, 'b>(&'a self,
                             policy: &'a ScanPolicy,
                             node: Arc<Node>,
                             namespace: &'a str,
                             set_name: &'a str,
                             bin_names: Option<&'a [&'b str]>)
                             -> AerospikeResult<Arc<Recordset>> {


        let bin_names = match bin_names {
            None => None,
            Some(bin_names) => {
                let bin_names: Vec<_> = bin_names.iter().cloned().map(String::from).collect();
                Some(bin_names)
            }
        };

        let mut recordset = Arc::new(try!(Recordset::new(policy.record_queue_size, 1)));
        let node = node.clone();
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
                                               t_recordset)
                                  .unwrap();
            command.execute().unwrap();
        });

        Ok(recordset)
    }

    pub fn query<'a, 'b>(&'a self,
                         policy: &QueryPolicy,
                         statement: Arc<Statement>)
                         -> AerospikeResult<Arc<Recordset>> {

        try!(statement.validate());

        let nodes = self.cluster.nodes();
        let mut recordset = Arc::new(try!(Recordset::new(policy.record_queue_size, nodes.len())));
        for node in nodes {
            let node = node.clone();
            let t_recordset = recordset.clone();
            let policy = policy.to_owned();
            let statement = statement.clone();

            self.thread_pool.execute(move || {
                let mut command = QueryCommand::new(&policy, node, statement, t_recordset).unwrap();
                command.execute().unwrap();
            });
        }
        Ok(recordset)
    }

    pub fn query_node<'a, 'b>(&self,
                              policy: &QueryPolicy,
                              node: Arc<Node>,
                              statement: Arc<Statement>)
                              -> AerospikeResult<Arc<Recordset>> {

        try!(statement.validate());

        let mut recordset = Arc::new(try!(Recordset::new(policy.record_queue_size, 1)));
        let node = node.clone();
        let t_recordset = recordset.clone();
        let policy = policy.to_owned();
        let statement = statement.clone();

        self.thread_pool.execute(move || {
            let mut command = QueryCommand::new(&policy, node, statement, t_recordset).unwrap();
            command.execute().unwrap();
        });

        Ok(recordset)
    }

    pub fn create_index(&self,
                        policy: &WritePolicy,
                        namespace: &str,
                        set_name: &str,
                        bin_name: &str,
                        index_name: &str,
                        index_type: IndexType)
                        -> AerospikeResult<()> {
        self.create_complex_index(policy,
                                  namespace,
                                  set_name,
                                  bin_name,
                                  index_name,
                                  index_type,
                                  CollectionIndexType::Default)

    }

    pub fn create_complex_index(&self,
                                policy: &WritePolicy,
                                namespace: &str,
                                set_name: &str,
                                bin_name: &str,
                                index_name: &str,
                                index_type: IndexType,
                                collection_index_type: CollectionIndexType)
                                -> AerospikeResult<()> {
        let cit_str: String = match collection_index_type {
            CollectionIndexType::Default => "".to_string(),
            _ => format!("indextype={};", collection_index_type),
        };

        let cmd = format!("sindex-create:ns={};set={};indexname={};numbins=1;{}indexdata={},{};priority=normal",
                          namespace,
                          set_name,
                          index_name,
                          cit_str,
                          bin_name,
                          index_type,
                          );

        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(&[&cmd]));

        for v in response.values() {
            match v {
                _ if v.to_uppercase() == "OK" => return Ok(()),
                _ if v.to_uppercase().contains("FAIL:200") => {
                    return Err(AerospikeError::new(ResultCode::INDEX_FOUND, None));
                }
                _ => return Err(AerospikeError::new(ResultCode::INDEX_GENERIC, Some(v.to_owned()))),
            };
        }

        Err(AerospikeError::new(ResultCode::INDEX_GENERIC, None))
    }


    pub fn drop_index(&self,
                      policy: &WritePolicy,
                      namespace: &str,
                      set_name: &str,
                      index_name: &str)
                      -> AerospikeResult<()> {

        let set_name: String = match set_name {
            "" => "".to_string(),
            _ => format!("set={};", set_name),
        };

        let cmd = format!("sindex-delete:ns={};{}indexname={}",
                          namespace,
                          set_name,
                          index_name,
                          );

        let node = try!(self.cluster.get_random_node());
        let response = try!(node.info(&[&cmd]));

        for v in response.values() {
            match v {
                _ if v.to_uppercase() == "OK" => return Ok(()),
                _ if v.to_uppercase().contains("FAIL:201") => {
                    return Err(AerospikeError::new(ResultCode::INDEX_GENERIC, None));
                }
                _ => return Err(AerospikeError::new(ResultCode::INDEX_GENERIC, Some(v.to_owned()))),
            };
        }

        Err(AerospikeError::new(ResultCode::INDEX_GENERIC, None))
    }
}
