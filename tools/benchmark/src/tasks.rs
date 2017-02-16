use std::ops::Range;

use aerospike::{Key, Bin, Client, WritePolicy};

use cli::Options;

pub struct InsertTask<'a> {
    client: &'a Client,
    policy: WritePolicy,
    namespace: &'a str,
    set: &'a str,
    key_range: Range<i64>,
}

impl<'a> InsertTask<'a> {
    pub fn new(client: &'a Client, key_range: Range<i64>, options: &'a Options) -> Self {
        InsertTask {
            client: client,
            policy: WritePolicy::default(),
            namespace: &options.namespace,
            set: &options.set,
            key_range: key_range,
        }
    }

    pub fn run(self) {
        for i in self.key_range.clone() {
            let key = as_key!(self.namespace, self.set, i);
            let bin = as_bin!("1", i);
            self.insert(&key, &[&bin]);
        }
    }

    fn insert(&self, key: &Key, bins: &[&Bin]) {
        trace!("Inserting {}", key);
        self.client.put(&self.policy, key, bins).unwrap()
    }
}
