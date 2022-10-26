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

/// Priority of operations on database server.
#[derive(Debug, Clone)]
pub enum Priority {
    /// Default determines that the server defines the priority.
    Default = 0,

    /// Low determines that the server should run the operation in a background thread.
    Low = 1,

    /// Medium determines that the server should run the operation at medium priority.
    Medium = 2,

    /// High determines that the server should run the operation at the highest priority.
    High = 3,
}

impl Default for Priority {
    fn default() -> Priority {
        Priority::Default
    }
}
