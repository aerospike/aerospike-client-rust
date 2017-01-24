// Copyright 2015-2017 Aerospike, Inc.
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

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use Value;
use Key;

lazy_static! {
  // Fri Jan  1 00:00:00 UTC 2010
  pub static ref CITRUSLEAF_EPOCH: SystemTime = UNIX_EPOCH + Duration::new(1262304000, 0);
}

#[derive(Debug)]
pub struct Record {
    pub key: Option<Key>,
    pub bins: HashMap<String, Value>,
    pub generation: u32,
    expiration: u32,
}

impl Record {
    pub fn new(key: Option<Key>,
               bins: HashMap<String, Value>,
               generation: u32,
               expiration: u32)
               -> Self {
        Record {
            key: key,
            bins: bins,
            generation: generation,
            expiration: expiration,
        }
    }

    pub fn time_to_live(&self) -> Option<Duration> {
        match self.expiration {
          0 => None, // record never expires
          secs_since_epoch @ _ => {
              let expiration = *CITRUSLEAF_EPOCH + Duration::new(secs_since_epoch as u64, 0);
              match expiration.duration_since(SystemTime::now()) {
                  Ok(d) => Some(d),
                  // Record was not expired at server but it looks expired at client
                  // because of delay or clock difference, present it as not-expired.
                  _ => Some(Duration::new(1u64, 0))
              }
          }
        }
    }
}


impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        try!(write!(f, "key: {:?}", self.key));
        try!(write!(f, ", bins: {{"));
        for (i, (k, v)) in self.bins.iter().enumerate() {
            if i > 0 {
                try!(write!(f, ", "))
            }
            try!(write!(f, "{}: {}", k, v));
        }
        try!(write!(f, "}}, generation: {}", self.generation));
        try!(write!(f, ", ttl: "));
        match self.time_to_live() {
            None => "none".fmt(f),
            Some(duration) => duration.as_secs().fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime};
    use std::collections::HashMap;

    #[test]
    fn ttl_expiration_future() {
        let expiration = SystemTime::now() + Duration::new(1000, 0);
        let secs_since_epoch = expiration.duration_since(*CITRUSLEAF_EPOCH).unwrap().as_secs();
        let record = Record::new(None, HashMap::new(), 0, secs_since_epoch as u32);
        let ttl = record.time_to_live();
        assert!(ttl.is_some());
        assert!(1000 - ttl.unwrap().as_secs() <= 1);
    }

    #[test]
    fn ttl_expiration_past() {
        let record = Record::new(None, HashMap::new(), 0, 0x0d00d21c);
        assert_eq!(record.time_to_live(), Some(Duration::new(1u64, 0)));
    }

    #[test]
    fn ttl_never_expires() {
        let record = Record::new(None, HashMap::new(), 0, 0);
        assert_eq!(record.time_to_live(), None);
    }
}
