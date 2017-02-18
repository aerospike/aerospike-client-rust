use std::ops::Range;
use std::sync::Arc;

use aerospike::{Key, Bin, Client, WritePolicy};

use cli::Options;
use counters::Counters;

pub struct InsertTask {
    client: Arc<Client>,
    policy: WritePolicy,
    namespace: String,
    set: String,
    key_range: Range<i64>,
    counters: Arc<Counters>,
}

impl InsertTask {
    pub fn new(client: Arc<Client>,
               key_range: Range<i64>,
               options: &Options,
               counters: Arc<Counters>)
               -> Self {
        InsertTask {
            client: client,
            policy: WritePolicy::default(),
            namespace: options.namespace.clone(),
            set: options.set.clone(),
            key_range: key_range,
            counters: counters,
        }
    }

    pub fn run(self) {
        for i in self.key_range.clone() {
            let key = as_key!(self.namespace.clone(), self.set.clone(), i);
            let bin = as_bin!("1", i);
            self.insert(&key, &[&bin]);
        }
    }

    fn insert(&self, key: &Key, bins: &[&Bin]) {
        trace!("Inserting {}", key);
        self.counters.track_request(|| self.client.put(&self.policy, key, bins));
    }
}
