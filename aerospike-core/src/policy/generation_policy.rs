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
//

/// `GenerationPolicy` determines how to handle record writes based on record generation.
#[derive(Debug, PartialEq, Clone)]
pub enum GenerationPolicy {
    /// None means: Do not use record generation to restrict writes.
    None = 0,

    /// ExpectGenEqual means: Update/delete record if expected generation is equal to server
    /// generation. Otherwise, fail.
    ExpectGenEqual = 1,

    /// ExpectGenGreater means: Update/delete record if expected generation greater than the server
    /// generation. Otherwise, fail. This is useful for restore after backup.
    ExpectGenGreater = 2,
}

impl Default for GenerationPolicy {
    fn default() -> GenerationPolicy {
        GenerationPolicy::None
    }
}
