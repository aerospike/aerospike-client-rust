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

pub use self::filter::{EqFilterValue, Filter, RangeFilterValue};
pub use self::index_types::{CollectionIndexType, IndexType};
pub(crate) use self::node_partitions::NodePartitions;
pub use self::order_by::{Order, OrderBy, OrderByFlags, OrderByType};
pub use self::partition_filter::PartitionFilter;
pub use self::partition_status::PartitionStatus;
pub use self::plan::{
    QueryPlan, QuerySelection, QueryWhereWire, FLAG_ENC_VARINT, FLAG_EXPLAIN, FLAG_HARD_HINT,
    FLAG_KNOWN, FLAG_REQUIRE_INDEX,
};
pub(crate) use self::partition_tracker::PartitionTracker;
pub use self::recordset::RecordStream;
pub use self::recordset::Recordset;
#[cfg(feature = "lua")]
pub use self::result_set::{ResultSet, ResultStream};
pub use self::statement::Statement;
pub(crate) use self::top_k_merge::TopKMerger;
pub use self::udf::UDFLang;

/// Query filter definitions and filter value traits.
pub mod filter;
mod index_types;
mod node_partitions;
/// Types for `ORDER BY <bin> LIMIT k` ("Top-K") queries.
pub mod order_by;
mod partition_filter;
mod partition_status;
mod partition_tracker;
pub mod plan;
mod recordset;
#[cfg(feature = "lua")]
mod result_set;
mod statement;
mod top_k_merge;
mod udf;
