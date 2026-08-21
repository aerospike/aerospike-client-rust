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

use crate::commands::as_msg_fields::AsMsgFields;
use crate::commands::field_type::FieldType;
use crate::errors::{Error, Result};
use crate::query::plan::index_range_wire::IndexRangeWire;
use crate::query::plan::query_selection::QuerySelection;
use crate::query::plan::query_where_wire::QueryWhereWire;
use crate::query::{CollectionIndexType, Filter};
use crate::ResultCode;

/// Result of a server query explain (phase 1).
#[derive(Debug, Clone)]
pub struct QueryPlan {
    selection: QuerySelection,
    namespace: String,
    set_name: Option<String>,
    explain_where_bytes: Vec<u8>,
    index_name: Option<String>,
    index_range_bytes: Option<Vec<u8>>,
    index_type: CollectionIndexType,
}

impl QueryPlan {
    /// Builds a plan from an explain `result_code` and parsed response fields.
    pub(crate) fn from_explain_response(
        result_code: ResultCode,
        namespace: &str,
        set_name: Option<&str>,
        explain_where_bytes: Vec<u8>,
        fields: &AsMsgFields,
    ) -> Result<Self> {
        if result_code == ResultCode::FilteredOut {
            return Ok(Self {
                selection: QuerySelection::FilteredOut,
                namespace: namespace.to_owned(),
                set_name: set_name.map(str::to_owned),
                explain_where_bytes,
                index_name: None,
                index_range_bytes: None,
                index_type: CollectionIndexType::Default,
            });
        }

        if result_code != ResultCode::Ok {
            return Err(Error::server_error(
                result_code,
                String::new(),
                None,
            ));
        }

        let index_name = fields.utf8_field(FieldType::IndexName);
        let index_range_bytes = fields.field(FieldType::IndexRange).map(<[u8]>::to_vec);
        let index_type = Self::index_collection_type_from_fields(fields)?;

        let has_index_name = index_name.is_some();
        let has_index_range = index_range_bytes.is_some();

        if has_index_name != has_index_range {
            return Err(Error::bad_response(
                "INDEX_NAME and INDEX_RANGE must both be present or both absent",
            ));
        }

        if has_index_name {
            let index_name = index_name.unwrap_or_default();
            let index_range_bytes = index_range_bytes.unwrap_or_default();
            if index_name.is_empty() || index_range_bytes.is_empty() {
                return Err(Error::bad_response(
                    "INDEX_NAME and INDEX_RANGE must be non-empty on SI explain",
                ));
            }
            Self::validate_si_explain_range(&index_range_bytes)?;
            return Ok(Self {
                selection: QuerySelection::SecondaryIndex,
                namespace: namespace.to_owned(),
                set_name: set_name.map(str::to_owned),
                explain_where_bytes,
                index_name: Some(index_name),
                index_range_bytes: Some(index_range_bytes),
                index_type,
            });
        }

        Ok(Self {
            selection: QuerySelection::PrimaryIndex,
            namespace: namespace.to_owned(),
            set_name: set_name.map(str::to_owned),
            explain_where_bytes,
            index_name: None,
            index_range_bytes: None,
            index_type: CollectionIndexType::Default,
        })
    }

    pub fn selection(&self) -> QuerySelection {
        self.selection
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn set_name(&self) -> Option<&str> {
        self.set_name.as_deref()
    }

    /// AEL source text from the explain field `44` payload.
    pub fn ael(&self) -> Result<String> {
        QueryWhereWire::ael(&self.explain_where_bytes)
    }

    /// Field `44` body sent on explain (`EXPLAIN` flag set).
    pub(crate) fn explain_where_bytes(&self) -> &[u8] {
        &self.explain_where_bytes
    }

    /// Field `44` body for execute (`EXPLAIN` flag cleared).
    ///
    /// Clears the explain flag in place and returns the payload buffer.
    pub(crate) fn into_execute_where_bytes(mut self) -> Vec<u8> {
        QueryWhereWire::clear_explain_in_place(&mut self.explain_where_bytes)
            .expect("explain WHERE payload built by QueryWhereWire");
        self.explain_where_bytes
    }

    /// Secondary-index registry name from explain field `21`, or `None` on PI / filtered-out.
    pub fn index_name(&self) -> Option<&str> {
        self.index_name.as_deref()
    }

    /// Opaque `INDEX_RANGE` bytes from explain field `22` (explain shape), or `None` on PI.
    pub fn index_range_bytes(&self) -> Option<&[u8]> {
        self.index_range_bytes.as_deref()
    }

    pub fn index_type(&self) -> &CollectionIndexType {
        &self.index_type
    }

    pub fn is_primary_index(&self) -> bool {
        self.selection == QuerySelection::PrimaryIndex
    }

    pub fn is_secondary_index(&self) -> bool {
        self.selection == QuerySelection::SecondaryIndex
    }

    pub fn is_filtered_out(&self) -> bool {
        self.selection == QuerySelection::FilteredOut
    }

    /// Builds the execute `Filter` for an SI plan (transforms field `22` to execute shape).
    pub fn filter_for_execute(&self) -> Result<Option<Filter>> {
        match self.selection {
            QuerySelection::PrimaryIndex | QuerySelection::FilteredOut => Ok(None),
            QuerySelection::SecondaryIndex => {
                // Both fields are `Some` whenever phase 1 selects SecondaryIndex.
                let index_name = self.index_name.as_deref().unwrap();
                let probe_range = self.index_range_bytes.as_deref().unwrap();
                let execute_range = IndexRangeWire::for_execute_with_index_name(probe_range)?;
                Ok(Some(Filter::from_wire_range(
                    index_name,
                    execute_range,
                    self.index_type.clone(),
                )))
            }
        }
    }

    fn validate_si_explain_range(range: &[u8]) -> Result<()> {
        if range.len() < 2 || range[0] != 1 {
            return Err(Error::bad_response(
                "INDEX_RANGE must start with n_ranges=1",
            ));
        }

        let bin_name_len = range[1] as usize;
        if bin_name_len > 0 && range.len() < 2 + bin_name_len + 1 {
            return Err(Error::bad_response("INDEX_RANGE truncated"));
        }
        Ok(())
    }

    fn index_collection_type_from_fields(fields: &AsMsgFields) -> Result<CollectionIndexType> {
        let data = fields.field(FieldType::IndexType);
        let Some(data) = data else {
            return Ok(CollectionIndexType::Default);
        };
        if data.is_empty() {
            return Ok(CollectionIndexType::Default);
        }
        match data[0] {
            0 => Ok(CollectionIndexType::Default),
            1 => Ok(CollectionIndexType::List),
            2 => Ok(CollectionIndexType::MapKeys),
            3 => Ok(CollectionIndexType::MapValues),
            ordinal => Err(Error::bad_response(format!(
                "Invalid INDEX_TYPE ordinal {ordinal}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::field_type::FieldType;
    use crate::query::plan::{FLAG_EXPLAIN, FLAG_HARD_HINT, FLAG_REQUIRE_INDEX};

    const AEL: &str = "$.age > 30";
    // n_ranges, bin-name length/name, then the particle type.
    const RANGE: &[u8] = &[1, 3, b'a', b'g', b'e', 1];

    fn fields_of(entries: &[(u8, &[u8])]) -> AsMsgFields {
        let mut body = Vec::new();
        for (ftype, value) in entries {
            let len = 1 + value.len();
            body.extend_from_slice(&(len as u32).to_be_bytes());
            body.push(*ftype);
            body.extend_from_slice(value);
        }
        AsMsgFields::from_buffer(&body, 0, entries.len()).unwrap()
    }

    #[test]
    fn primary_index_plan_when_no_index_fields() {
        let explain_where = QueryWhereWire::for_explain(AEL).unwrap();
        let plan = QueryPlan::from_explain_response(
            ResultCode::Ok,
            "test",
            Some("users"),
            explain_where.clone(),
            &fields_of(&[]),
        )
        .unwrap();

        assert_eq!(plan.selection(), QuerySelection::PrimaryIndex);
        assert!(plan.is_primary_index());
        assert_eq!(plan.namespace(), "test");
        assert_eq!(plan.set_name(), Some("users"));
        assert_eq!(plan.ael().unwrap(), AEL);
        assert_eq!(plan.explain_where_bytes(), &explain_where);
        assert!(plan.index_name().is_none());
        assert!(plan.index_range_bytes().is_none());
        assert_eq!(plan.index_type(), &CollectionIndexType::Default);
        assert_eq!(
            plan.into_execute_where_bytes(),
            QueryWhereWire::for_execute(AEL).unwrap()
        );
    }

    #[test]
    fn secondary_index_plan_when_name_range_and_type_present() {
        let explain_where = QueryWhereWire::for_explain(AEL).unwrap();
        let fields = fields_of(&[
            (FieldType::IndexName as u8, b"age_idx"),
            (
                FieldType::IndexType as u8,
                &[CollectionIndexType::List as u8],
            ),
            (FieldType::IndexRange as u8, RANGE),
        ]);
        let plan = QueryPlan::from_explain_response(
            ResultCode::Ok,
            "test",
            None,
            explain_where,
            &fields,
        )
        .unwrap();

        assert_eq!(plan.selection(), QuerySelection::SecondaryIndex);
        assert!(plan.is_secondary_index());
        assert_eq!(plan.index_name(), Some("age_idx"));
        assert_eq!(plan.index_range_bytes(), Some(RANGE));
        assert_eq!(plan.index_type(), &CollectionIndexType::List);
    }

    #[test]
    fn execute_where_clears_tier_d_explain_flags() {
        let explain_where = QueryWhereWire::for_explain_with_flags(
            FLAG_EXPLAIN | FLAG_REQUIRE_INDEX | FLAG_HARD_HINT,
            AEL,
        )
        .unwrap();
        let plan = QueryPlan::from_explain_response(
            ResultCode::Ok,
            "test",
            Some("users"),
            explain_where,
            &fields_of(&[]),
        )
        .unwrap();

        assert_eq!(
            plan.into_execute_where_bytes(),
            QueryWhereWire::for_execute(AEL).unwrap()
        );
    }

    #[test]
    fn filtered_out_plan() {
        let explain_where = QueryWhereWire::for_explain(AEL).unwrap();
        let plan = QueryPlan::from_explain_response(
            ResultCode::FilteredOut,
            "test",
            Some("users"),
            explain_where,
            &fields_of(&[]),
        )
        .unwrap();

        assert_eq!(plan.selection(), QuerySelection::FilteredOut);
        assert!(plan.is_filtered_out());
    }
}
