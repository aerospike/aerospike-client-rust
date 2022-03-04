// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache Licenseersion 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#[cfg(feature = "serialization")]
use serde::Serialize;

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::Key;
use crate::Value;

lazy_static! {
  // Fri Jan  1 00:00:00 UTC 2010
  pub static ref CITRUSLEAF_EPOCH: SystemTime = UNIX_EPOCH + Duration::new(1_262_304_000, 0);
}

/// Container object for a database record.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serialization", derive(Serialize))]
pub struct Record {
    /// Record key. When reading a record from the database, the key is not set in the returned
    /// Record struct.
    pub key: Option<Key>,

    /// Map of named record bins.
    pub bins: HashMap<String, Value>,

    /// Record modification count.
    pub generation: u32,

    /// Date record will expire, in seconds from Jan 01 2010, 00:00:00 UTC.
    expiration: u32,
}

impl Record {
    /// Construct a new Record. For internal use only.
    #[doc(hidden)]
    pub const fn new(
        key: Option<Key>,
        bins: HashMap<String, Value>,
        generation: u32,
        expiration: u32,
    ) -> Self {
        Record {
            key,
            bins,
            generation,
            expiration,
        }
    }

    /// Returns the remaining time-to-live (TTL, a.k.a. expiration time) for the record or `None`
    /// if the record never expires.
    pub fn time_to_live(&self) -> Option<Duration> {
        match self.expiration {
            0 => None,
            secs_since_epoch => {
                let expiration = *CITRUSLEAF_EPOCH + Duration::new(u64::from(secs_since_epoch), 0);
                match expiration.duration_since(SystemTime::now()) {
                    Ok(d) => Some(d),
                    // Record was not expired at server but it looks expired at client
                    // because of delay or clock difference, present it as not-expired.
                    Err(_) => Some(Duration::new(1u64, 0)),
                }
            }
        }
    }
}

impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "key: {:?}", self.key)?;
        write!(f, ", bins: {{")?;
        for (i, (k, v)) in self.bins.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", k, v)?;
        }
        write!(f, "}}, generation: {}", self.generation)?;
        write!(f, ", ttl: ")?;
        match self.time_to_live() {
            None => "none".fmt(f),
            Some(duration) => duration.as_secs().fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Record, CITRUSLEAF_EPOCH};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    #[test]
    fn ttl_expiration_future() {
        let expiration = SystemTime::now() + Duration::new(1000, 0);
        let secs_since_epoch = expiration
            .duration_since(*CITRUSLEAF_EPOCH)
            .unwrap()
            .as_secs();
        let record = Record::new(None, HashMap::new(), 0, secs_since_epoch as u32);
        let ttl = record.time_to_live();
        assert!(ttl.is_some());
        assert!(1000 - ttl.unwrap().as_secs() <= 1);
    }

    #[test]
    fn ttl_expiration_past() {
        let record = Record::new(None, HashMap::new(), 0, 0x0d00_d21c);
        assert_eq!(record.time_to_live(), Some(Duration::new(1u64, 0)));
    }

    #[test]
    fn ttl_never_expires() {
        let record = Record::new(None, HashMap::new(), 0, 0);
        assert_eq!(record.time_to_live(), None);
    }
}
