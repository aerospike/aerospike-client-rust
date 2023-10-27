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

/// Specifies whether a command, that needs to be executed on multiple cluster nodes, should be
/// executed sequentially, one node at a time, or in parallel on multiple nodes using the client's
/// thread pool.
#[derive(Debug, Clone, Copy)]
pub enum Concurrency {
    /// Issue commands sequentially. This mode has a performance advantage for small to
    /// medium sized batch sizes because requests can be issued in the main transaction thread.
    /// This is the default.
    Sequential,

    /// Issue all commands in parallel threads. This mode has a performance advantage for
    /// extremely large batch sizes because each node can process the request immediately. The
    /// downside is extra threads will need to be created (or takedn from a thread pool).
    Parallel,
}
