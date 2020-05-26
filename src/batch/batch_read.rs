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

use crate::Bins;
use crate::Key;
use crate::Record;

/// Key and bin names used in batch read commands where variable bins are needed for each key.
pub struct BatchRead<'a> {
    /// Key.
    pub key: Key,

    /// Bins to retrieve for this key.
    pub bins: &'a Bins,

    /// Will contain the record after the batch read operation.
    pub record: Option<Record>,
}

impl<'a> BatchRead<'a> {
    /// Create a new `BatchRead` instance for the given key and bin selector.
    pub const fn new(key: Key, bins: &'a Bins) -> Self {
        BatchRead {
            key,
            bins,
            record: None,
        }
    }

    #[doc(hidden)]
    pub fn match_header(&self, other: &BatchRead<'a>, match_set: bool) -> bool {
        let key = &self.key;
        let other_key = &other.key;
        (key.namespace == other_key.namespace)
            && (match_set && (key.set_name == other_key.set_name))
            && (self.bins == other.bins)
    }
}
