// Copyright 2015-2018 Aerospike, Inc.
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

use aerospike::Key;

#[derive(Debug)]
pub struct KeyPartitions {
    namespace: String,
    set: String,
    index: i64,
    end: i64,
    keys_per_partition: i64,
    remainder: i64,
}

impl KeyPartitions {
    pub fn new(
        namespace: String,
        set: String,
        start_key: i64,
        count: i64,
        partitions: i64,
    ) -> Self {
        KeyPartitions {
            namespace: namespace,
            set: set,
            index: start_key,
            end: start_key + count,
            keys_per_partition: count / partitions,
            remainder: count % partitions,
        }
    }
}

impl Iterator for KeyPartitions {
    type Item = KeyRange;

    fn next(&mut self) -> Option<KeyRange> {
        if self.index < self.end {
            let mut count = self.keys_per_partition;
            if self.remainder > 0 {
                count += 1;
                self.remainder -= 1;
            }
            let range = KeyRange::new(self.namespace.clone(), self.set.clone(), self.index, count);
            self.index += count;
            Some(range)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct KeyRange {
    namespace: String,
    set: String,
    index: i64,
    end: i64,
}

impl KeyRange {
    pub fn new(namespace: String, set: String, start: i64, count: i64) -> Self {
        KeyRange {
            namespace: namespace,
            set: set,
            index: start,
            end: start + count,
        }
    }
}

impl Iterator for KeyRange {
    type Item = Key;

    fn next(&mut self) -> Option<Key> {
        if self.index < self.end {
            let key = as_key!(self.namespace.clone(), self.set.clone(), self.index);
            self.index += 1;
            Some(key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_key_range() {
        let mut range = KeyRange::new("foo".into(), "bar".into(), 0, 3);
        assert_eq!(range.next(), Some(as_key!("foo", "bar", 0)));
        assert_eq!(range.next(), Some(as_key!("foo", "bar", 1)));
        assert_eq!(range.next(), Some(as_key!("foo", "bar", 2)));
        assert!(range.next().is_none());
    }

    #[test]
    fn test_key_partitions() {
        let partitions = KeyPartitions::new("foo".into(), "bar".into(), 0, 10, 3);
        let mut parts = 0;
        let mut keys = 0;
        for part in partitions {
            for key in part {
                assert_eq!(key, as_key!("foo", "bar", keys));
                keys += 1;
            }
            parts += 1
        }
        assert_eq!(parts, 3);
        assert_eq!(keys, 10);
    }
}
