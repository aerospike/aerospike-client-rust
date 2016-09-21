// Copyright 2015-2016 Aerospike, Inc.
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

pub use self::priority::Priority;
pub use self::consistency_level::ConsistencyLevel;
pub use self::generation_policy::GenerationPolicy;
pub use self::record_exists_action::RecordExistsAction;
pub use self::commit_level::CommitLevel;

pub use self::policy::{Policy, BasePolicy};
pub use self::client_policy::ClientPolicy;
pub use self::read_policy::ReadPolicy;
pub use self::write_policy::WritePolicy;
pub use self::scan_policy::ScanPolicy;
pub use self::query_policy::QueryPolicy;
pub use self::admin_policy::AdminPolicy;

pub mod priority;
pub mod consistency_level;
pub mod generation_policy;
pub mod record_exists_action;
pub mod commit_level;

pub mod policy;
pub mod client_policy;
pub mod read_policy;
pub mod write_policy;
pub mod scan_policy;
pub mod query_policy;
pub mod admin_policy;
