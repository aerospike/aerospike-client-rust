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

/// `RecordExistsAction` determines how to handle record writes based on record generation.
#[derive(Debug, PartialEq, Clone)]
pub enum RecordExistsAction {
    /// Update means: Create or update record.
    /// Merge write command bins with existing bins.
    Update = 0,

    /// UpdateOnly means: Update record only. Fail if record does not exist.
    /// Merge write command bins with existing bins.
    UpdateOnly,

    /// Replace means: Create or replace record.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    Replace,

    /// ReplaceOnly means: Replace record only. Fail if record does not exist.
    /// Delete existing bins not referenced by write command bins.
    /// Supported by Aerospike 2 server versions >= 2.7.5 and
    /// Aerospike 3 server versions >= 3.1.6.
    ReplaceOnly,

    /// CreateOnly means: Create only. Fail if record exists.
    CreateOnly,
}

impl Default for RecordExistsAction {
    fn default() -> RecordExistsAction {
        RecordExistsAction::Update
    }
}
