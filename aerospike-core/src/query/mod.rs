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

//! Types and methods used for database queries and scans.
#![allow(clippy::missing_errors_doc)]

pub use self::filter::Filter;
pub use self::index_types::{CollectionIndexType, IndexType};
pub(crate) use self::node_partitions::NodePartitions;
pub use self::partition_filter::PartitionFilter;
pub use self::partition_status::PartitionStatus;
pub(crate) use self::partition_tracker::PartitionTracker;
pub use self::recordset::Recordset;
pub use self::statement::Statement;
pub use self::udf::UDFLang;

mod filter;
mod index_types;
mod node_partitions;
mod partition_filter;
mod partition_status;
mod partition_tracker;
mod recordset;
mod statement;
mod udf;
