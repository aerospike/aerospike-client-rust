// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// User and assigned roles.
#[derive(Clone, Debug, PartialEq)]
pub struct User {
    /// User name.
    pub user: String,

    /// List of assigned roles.
    pub roles: Vec<String>,

    /// List of read statistics. List may be nil.
    /// Current statistics by offset are:
    ///
    /// 0: read quota in records per second
    /// 1: single record read command rate (TPS)
    /// 2: read scan/query record per second rate (RPS)
    /// 3: number of limitless read scans/queries
    ///
    /// Future server releases may add additional statistics.
    pub read_info: Vec<u32>,

    /// List of write statistics. List may be nil.
    /// Current statistics by offset are:
    ///
    /// 0: write quota in records per second
    /// 1: single record write command rate (TPS)
    /// 2: write scan/query record per second rate (RPS)
    /// 3: number of limitless write scans/queries
    ///
    /// Future server releases may add additional statistics.
    pub write_info: Vec<u32>,

    /// Number of currently open connections for the user
    pub conns_in_use: u32,
}
