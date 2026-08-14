// Copyright 2015-2026 Aerospike, Inc.
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

//! Two-phase server query selection: field `44` WHERE explain → execute.

mod index_range_wire;
mod query_plan;
mod query_selection;
mod query_where_wire;

pub(crate) use index_range_wire::IndexRangeWire;
pub use query_plan::QueryPlan;
pub use query_selection::QuerySelection;
pub use query_where_wire::{
    QueryWhereWire, FLAG_ENC_VARINT, FLAG_EXPLAIN, FLAG_HARD_HINT, FLAG_KNOWN, FLAG_REQUIRE_INDEX,
};
