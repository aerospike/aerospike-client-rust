//! Provides an API for implementing a serializable Aerospike Record
//! 

// /////////////////////////////////////////////////////////////////////
use std::convert::{From, Into};
use std::default::Default;
use std::env::var;
use std::error::Error;
use std::fmt::Debug;
use std::marker::Sized;
use std::result::Result;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::{Bin, Bins, Client, Expiration, Key, ReadPolicy, Record, RecordExistsAction, ScanPolicy, Value, WritePolicy};

type BoxedResult<T> = Result<T, Box<dyn Error>>;

/// Provides a mechanism to access cache
pub trait AerospikeRecord: Sized + Serialize + DeserializeOwned + Default {
    /// Builds a stringified version of the record's key
    ///
    ///  Note: Aerospike does not support u64 natively, so it must be cast
    fn aerospike_record_key(&self) -> String;
   
    /// Builds aerospike bin name for this record
    /// Note: aerospike bin names must be 14 characters or less
    fn aerospike_bin_name(&self) -> String {
        "payload".to_string()
    }

    /// Returns the timeout in seconds
    ///   A timeout of 0 indicates that the value will never be culled
    fn aerospike_timeout(&self) -> u32 {
        match var("AEROSPIKE_TIMEOUT") {
            Ok(namespace) => namespace.parse::<u32>().unwrap_or(0),
            Err(why) => {
                println!("{}", why);
                0
            }
        }
    }

    /// Returns the namespace
    fn aerospike_namespace() -> String {
        match var("AEROSPIKE_NAMESPACE") {
            Ok(namespace) => namespace,
            Err(why) => {
                println!("{}", why);
                "undefined".to_string()
            }
        }
    }

    /// Returns the set name
    fn aerospike_set_name() -> String {
        match var("AEROSPIKE_SET_NAME") {
            Ok(namespace) => namespace,
            Err(why) => {
                println!("{}", why);
                "undefined".to_string()
            }
        }
    }

    /// Builds record data for aerospike entry
    fn aerospike_as_bins(&self) -> Vec<Bin> {
        let key = self.aerospike_record_key();
        let bin_name = self.aerospike_bin_name();
        vec![
            // Bin names must be 14 or less characters
            as_bin!("key", key),
            as_bin!(&bin_name, serde_json::to_string(self).unwrap_or_default()),
        ]
    }

    /// Builds an aerospike key from record
    fn aerospike_key(&self) -> Key {
        let namespace: String = Self::aerospike_namespace();
        let set_name: String = Self::aerospike_set_name();
        self.aerospike_build_key(&namespace, &set_name)
    }

    /// Builds an aerospike key using parameters
    fn aerospike_build_key(&self, namespace: &str, set_name: &str) -> Key {
        let key = self.aerospike_record_key();
        as_key!(namespace, set_name, &key)
    }

    /// Build dyn AerospikeRecord from aerospike::Record
    fn aerospike_from_record(&self, aerospike_record: Record) -> Self
    where
        Self: DeserializeOwned,
    {
        let default = Self::default();
        let payload = match_string("payload", &aerospike_record);
        match payload.as_str() {
            "" => default,
            _ => serde_json::from_str(&payload).unwrap_or(default),
        }
    }
}

impl<T> AerospikeRecord for Vec<T> 
where
   T:  AerospikeRecord + Sized + Serialize + DeserializeOwned + Default
{
    fn aerospike_record_key(&self) -> String {
        let default = T::default();
        let value: &T = self.first().unwrap_or(&default);
        value.aerospike_record_key()
    }
} 

/// Determines if record exists
///
/// Arguments:
/// * `namespace` - identifies which namespace to use
/// * `set_name` - identifies the set found within the namespace to use
/// * `key` - identifies the key to retrieve within the namespace and set
/// * `client` - the prebuilt Aerospike client
///
pub fn exists_record<T>(record: T, client: &Client) -> BoxedResult<bool>
where
    T: AerospikeRecord + Debug,
{
    let policy = WritePolicy::default();
    let aerospike_key = record.aerospike_key();
    let exists = client.exists(&policy, &aerospike_key).unwrap_or_else(|why| {
        println!("Could not access: {}.{} => {:#?}", T::aerospike_namespace(), T::aerospike_set_name(), record);
        panic!("{}", why);
    });
    Ok(exists)
}

/// Retrieves a value from Aerospike using the record information
///
/// Arguments:
/// * `key` - identifies the key to retrieve within the namespace and set
/// * `client` - the prebuilt Aerospike client
///
pub fn get_record<T>(key: impl Into<Value>, client: &Client) -> BoxedResult<T>
where
    T: From<Record> + AerospikeRecord,
{
    let namespace = T::aerospike_namespace();
    let set_name = T::aerospike_set_name();
    let rpolicy = ReadPolicy::default();
    let aerospike_key = as_key!(namespace, set_name, key.into());
    client.get(&rpolicy, &aerospike_key, Bins::All).map(T::from).map_err(|e| e.into())
}

/// Removes a key
///
/// Arguments:
/// * `key` - identifies the key to retrieve within the namespace and set
/// * `client` - the prebuilt Aerospike client
///
pub fn remove_record<T>(record: T, client: &Client) -> BoxedResult<bool>
where
    T: AerospikeRecord,
{
    let policy = WritePolicy::default();
    let aerospike_key = record.aerospike_key();
    client.delete(&policy, &aerospike_key).map_err(|e| e.into())
}

/// Retrieves all records from Aerospike using the record's data
///
/// Arguments:
/// * `client` - the prebuilt Aerospike client
///
pub fn scan_record<T>(client: &Client) -> BoxedResult<Vec<T>>
where
    T: From<Record> + AerospikeRecord,
{
    let spolicy = ScanPolicy::default();
    let namespace = T::aerospike_namespace();
    let set_name = T::aerospike_set_name();
    let records: Vec<T> = match client.scan(&spolicy, &namespace, &set_name, Bins::All) {
        Ok(aerospike_records) => aerospike_records
            .into_iter()
            .map(|r| match r {
                Ok(record) => T::from(record),
                Err(why) => panic!("{}", why),
            })
            .collect(),
        Err(why) => panic!("{}", why),
    };
    Ok(records)
}

/// Sets a value in Aerospike
///
/// Arguments:
/// * `record` - the record to use
/// * `client` - the prebuilt Aerospike client
///
pub fn set_record<T>(record: T, client: &Client) -> BoxedResult<()>
where
    T: Clone + AerospikeRecord,
{
    let namespace = T::aerospike_namespace();
    let set_name = T::aerospike_set_name();
    let aerospike_key = record.aerospike_build_key(&namespace, &set_name);
    let timeout = record.aerospike_timeout();
    let mut policy = match timeout > 0 {
        true => WritePolicy {
            expiration: Expiration::Seconds(timeout),
            ..Default::default()
        },
        false => WritePolicy::default(),
    };
    policy.record_exists_action = RecordExistsAction::Replace;
    let data: Vec<Bin> = record.aerospike_as_bins();
    client.put(&policy, &aerospike_key, &data).map_err(|e| e.into())
}

// Deserializes bin name as a string
fn match_string(bin_name: &str, record: &Record) -> String {
    let default = String::default();
    match record.bins.get(bin_name) {
        Some(value) => match value {
            Value::String(id) => id.to_string(),
            _ => default,
        },
        None => default,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use serde::Deserialize;
    use crate::ClientPolicy;
    use super::*;

    /// Creates a client
    pub fn build_client(hosts: &str) -> BoxedResult<Arc<Client>> {
        let cpolicy = ClientPolicy {
            timeout: Some(Duration::from_millis(300)),
            ..Default::default()
        };
        let client = Client::new(&cpolicy, &hosts)?;
        // This will let client be thread safe
        Ok(Arc::new(client))
    }

    /// Simple example of a test record
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    struct TestRecord {
        foo: String,
        bar: String,
    }

    impl Default for TestRecord {
        fn default() -> Self {
            TestRecord {
                foo: String::default(),
                bar: String::default(),
            }
        }
    }

    impl AerospikeRecord for TestRecord {
        fn aerospike_record_key(&self) -> String {
            self.foo.clone()
        }
    }

    impl From<Record> for TestRecord {
        fn from(aerospike_record: Record) -> Self {
            let default = Self::default();
            let payload = match_string("payload", &aerospike_record);
            match payload.as_str() {
                "" => default,
                _ => serde_json::from_str(&payload).unwrap_or_else(|_| default),
            }
        }
    }

    impl Into<Vec<Bin>> for TestRecord {
        fn into(self) -> Vec<Bin> {
            let json_string = serde_json::to_string(&self).unwrap_or_else(|_| String::default());
            vec![as_bin!("key", self.aerospike_record_key()), as_bin!("payload", json_string)]
        }
    }

    /// Test: Record interface with full CRUD
    #[test]
    fn test_aerospike_record_crud() {
        dotenv::dotenv().ok();
        let hosts = var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
        let key = "1".to_string();
        let value = TestRecord {
            foo: key.clone(),
            bar: "2".to_string(),
        };
        if let Ok(client) = build_client(&hosts) {
            let found = exists_record(value.clone(), &client).unwrap();
            assert_eq!(found, false);
            // Create
            set_record(value.clone(), &client).unwrap();
            let found = exists_record(value.clone(), &client).unwrap();
            assert_eq!(found, true);
            // Read
            let actual_value: TestRecord = get_record(key.clone(), &client).unwrap();
            assert_eq!(actual_value, value.clone());
            // Update
            let new_key = "foo".to_string();
            let new_value = TestRecord {
                foo: new_key.clone(),
                bar: "bar".to_string(),
            };
            set_record(new_value.clone(), &client).unwrap();
            let actual_value: TestRecord = get_record(new_key.clone(), &client).unwrap();
            assert_eq!(actual_value, new_value.clone());
            let found = exists_record(new_value.clone(), &client).unwrap();
            assert_eq!(found, true);
            // Delete
            remove_record(value.clone(), &client).unwrap();
            remove_record(new_value.clone(), &client).unwrap();
            let found = exists_record(value.clone(), &client).unwrap();
            assert_eq!(found, false);
            let found = exists_record(new_value.clone(), &client).unwrap();
            assert_eq!(found, false);
        }
    }
}