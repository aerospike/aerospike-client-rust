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
use std::time::{Instant, Duration};

use internal::wait_group::WaitGroup;
use net::Host;
use cluster::{Cluster, Node};
use common::operation;
use common::{Key, Record, Bin};
use command::read_command::{ReadCommand};
use command::write_command::{WriteCommand};
use command::delete_command::{DeleteCommand};
use command::touch_command::{TouchCommand};
use command::exists_command::{ExistsCommand};
use command::read_header_command::{ReadHeaderCommand};
use value::{Value, IntValue, StringValue};

use policy::{ClientPolicy, ReadPolicy, WritePolicy};
use error::{AerospikeResult};


// Client encapsulates an Aerospike cluster.
// All database operations are available against this object.
pub struct Client {
    pub cluster: Arc<Cluster>,
}

impl Client {
    pub fn new(host_name: String, port: u16) -> AerospikeResult<Self> {
        let hosts = vec![Host::new(host_name, port)];
        let policy: ClientPolicy = Default::default();
        let cluster = try!(Cluster::new(policy, &hosts));

        Ok(Client {
        	cluster: cluster,
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
                         key: &'a Key<'a>,
                         bin_names: Option<&'a [&'b str]>)
                         -> AerospikeResult<Arc<Record>> {
        let mut command = try!(ReadCommand::new(policy, self.cluster.clone(), key, bin_names));
        try!(command.execute());
        Ok(command.record.as_ref().unwrap().clone())
    }

    pub fn get_header<'a, 'b>(&'a self,
                         policy: &'a ReadPolicy,
                         key: &'a Key<'a>)
                         -> AerospikeResult<Arc<Record>> {
        let mut command = try!(ReadHeaderCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.record.as_ref().unwrap().clone())
    }

    pub fn put<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>,
                         bins: &'a [&'b Bin])
                         -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy, self.cluster.clone(), key, bins, operation::WRITE));
        command.execute()
    }

    pub fn add<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>,
                         bins: &'a [&'b Bin])
                         -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy, self.cluster.clone(), key, bins, operation::ADD));
        command.execute()
    }

    pub fn append<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>,
                         bins: &'a [&'b Bin])
                         -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy, self.cluster.clone(), key, bins, operation::APPEND));
        command.execute()
    }

    pub fn PREPEND<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>,
                         bins: &'a [&'b Bin])
                         -> AerospikeResult<()> {
        let mut command = try!(WriteCommand::new(policy, self.cluster.clone(), key, bins, operation::PREPEND));
        command.execute()
    }

    pub fn delete<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>)
                         -> AerospikeResult<bool> {
        let mut command = try!(DeleteCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.existed)
    }

    pub fn touch<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>)
                         -> AerospikeResult<()> {
        let mut command = try!(TouchCommand::new(policy, self.cluster.clone(), key));
        command.execute()
    }

    pub fn exists<'a, 'b>(&'a self,
                         policy: &'a WritePolicy,
                         key: &'a Key<'a>)
                         -> AerospikeResult<bool> {
        let mut command = try!(ExistsCommand::new(policy, self.cluster.clone(), key));
        try!(command.execute());
        Ok(command.exists)
    }
}

#[test]
fn connect() {
    env_logger::init().unwrap();

    let client: Arc<Client> = Arc::new(Client::new("172.16.224.150".to_string(), 3000).unwrap());

    let mut threads = vec![];
    for _ in 0..2 {
    	let client = client.clone();
	    let t = thread::spawn(move || {
		    let policy = ReadPolicy::default();

		    let wpolicy = WritePolicy::default();
		    let v = IntValue::new(-1);
		    let key = Key::new("test", "test", &v).unwrap();
		    let wbin = Bin::new("bin999", &v);
		    let bins = vec![&wbin];

			client.put(&wpolicy, &key, &bins).unwrap();
		    let rec = client.get(&policy, &key, None);
		    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());

			client.touch(&wpolicy, &key).unwrap();
		    let rec = client.get(&policy, &key, None);
		    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());

		    let rec = client.get_header(&policy, &key);
		    println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@/// {}", rec.unwrap());

			let exists = client.exists(&wpolicy, &key).unwrap();
			println!("exists: {}", exists);

			let existed = client.delete(&wpolicy, &key).unwrap();
			println!("existed: {}", existed);

			let existed = client.delete(&wpolicy, &key).unwrap();
			println!("existed: {}", existed);
		});
		threads.push(t);
	}

	for t in threads {
		t.join();
	}

 //    let now = Instant::now();

 //    for i in 1..10_000 {
	//     let v = IntValue::new(i % 10000);
	//     let key = Key::new("test", "test", &v).unwrap();
	//     let rec = client.get(&policy, &key, None);
	//     // println!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ {}", rec.unwrap());
	// }

	// println!("total time: {:?}", now.elapsed());

 //    for _ in 1..100 {
 //        let cluster = client.cluster.clone();
 //        println!("{:?}", cluster.nodes().len());
 //        thread::sleep(Duration::from_millis(1000));
 //    }
 //    assert_eq!(2, 2);
}
