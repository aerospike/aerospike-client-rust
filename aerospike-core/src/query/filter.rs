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

use crate::commands::{buffer::Buffer, ParticleType};
use crate::errors::Result;
use crate::expressions::Expression;
use crate::operations::CdtContext;
use crate::{CollectionIndexType, Value};

/// Marker trait for types valid in equality and contains filters.
///
/// Supported types: integers (`i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`, `isize`,
/// `usize`), strings (`String`, `&str`), and blobs (`Vec<u8>`, `&[u8]`).
pub trait EqFilterValue {
    /// Converts this value into a `Value` for use in a filter.
    fn into_filter_value(self) -> Value;
}

/// Marker trait for types valid in range filters.
///
/// Only integer types are supported: `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64`,
/// `isize`, `usize`.
pub trait RangeFilterValue {
    /// Converts this value into a `Value` for use in a range filter.
    fn into_filter_value(self) -> Value;
}

// --- EqFilterValue implementations ---

// Integer types (also valid for range)
macro_rules! impl_eq_filter_int {
    ($($t:ty),*) => {
        $(
            impl EqFilterValue for $t {
                fn into_filter_value(self) -> Value { Value::from(self) }
            }
            impl EqFilterValue for &$t {
                fn into_filter_value(self) -> Value { Value::from(*self) }
            }
            impl RangeFilterValue for $t {
                fn into_filter_value(self) -> Value { Value::from(self) }
            }
            impl RangeFilterValue for &$t {
                fn into_filter_value(self) -> Value { Value::from(*self) }
            }
        )*
    }
}

impl_eq_filter_int!(i8, u8, i16, u16, i32, u32, i64, u64, isize, usize);

// String types
impl EqFilterValue for String {
    fn into_filter_value(self) -> Value {
        Value::from(self)
    }
}

impl EqFilterValue for &String {
    fn into_filter_value(self) -> Value {
        Value::from(self.clone())
    }
}

impl EqFilterValue for &str {
    fn into_filter_value(self) -> Value {
        Value::from(self.to_owned())
    }
}

// Blob types
impl EqFilterValue for Vec<u8> {
    fn into_filter_value(self) -> Value {
        Value::from(self)
    }
}

impl EqFilterValue for &Vec<u8> {
    fn into_filter_value(self) -> Value {
        Value::from(self.clone())
    }
}

impl EqFilterValue for &[u8] {
    fn into_filter_value(self) -> Value {
        Value::from(self.to_vec())
    }
}

// Value impls — allows passing pre-constructed Value instances (e.g. from proptests).
// These perform a runtime check since the type information is erased.
impl EqFilterValue for Value {
    fn into_filter_value(self) -> Value {
        assert!(
            matches!(
                self.particle_type(),
                ParticleType::INTEGER | ParticleType::STRING | ParticleType::BLOB
            ),
            "equality/contains filter value must be integer, string, or blob"
        );
        self
    }
}

impl RangeFilterValue for Value {
    fn into_filter_value(self) -> Value {
        assert!(
            matches!(self.particle_type(), ParticleType::INTEGER),
            "range filter value must be integer"
        );
        self
    }
}

// Helper to build the AeroCircle GeoJSON string
fn geo_circle_json(lng: f64, lat: f64, radius: f64) -> Value {
    Value::String(format!(
        "{{ \"type\": \"AeroCircle\", \"coordinates\": [[{lng:.8}, {lat:.8}], {radius}] }}"
    ))
}

/// Query filter definition. Currently, only one filter is allowed in a Statement, and must be on a
/// bin which has a secondary index defined.
///
/// Filter instances should be created using the associated methods on this type, such as
/// [`Filter::equal`], [`Filter::range`], [`Filter::contains`], etc.
///
/// # Expression-based secondary index
///
/// When querying against an expression-based secondary index (created via
/// [`Client::create_index_using_expression`](crate::Client::create_index_using_expression)),
/// attach the same [`Expression`] to the filter using the [`Filter::expression`] builder.
/// This tells the server which index to use for the lookup.
///
/// **Note:** This is different from
/// [`BasePolicy::filter_expression`](crate::policy::BasePolicy::filter_expression), which is
/// a post-filter applied to records *after* the index lookup.
///
/// ```rust,no_run
/// # use aerospike_core::query::Filter;
/// # use aerospike_core::expressions;
/// let exp = expressions::int_bin("a".to_string());
/// let f = Filter::range("a", 0_i64, 100_i64)
///     .expression(exp);
/// ```
///
/// # CDT Context
///
/// For secondary indexes on elements within a CDT (Collection Data Type), a
/// [`CdtContext`] can be attached to any filter using the [`Filter::context`] builder method.
/// The context specifies the path to the element within the nested CDT structure.
///
/// ```rust,no_run
/// # use aerospike_core::query::Filter;
/// # use aerospike_core::operations::cdt_context::{CdtContext, ctx_list_index};
/// // Filter on a secondary index within a nested list element
/// let f = Filter::equal("bin_name", 42_i64)
///     .context(vec![ctx_list_index(0)]);
/// ```
///
/// Both builders can be combined:
///
/// ```rust,no_run
/// # use aerospike_core::query::Filter;
/// # use aerospike_core::operations::cdt_context::ctx_list_index;
/// # use aerospike_core::expressions;
/// let exp = expressions::int_bin("bin_name".to_string());
/// let f = Filter::range("bin_name", 0_i64, 100_i64)
///     .expression(exp)
///     .context(vec![ctx_list_index(0)]);
/// ```
#[derive(Debug, Clone)]
pub struct Filter {
    pub(crate) bin_name: String,
    pub(crate) collection_index_type: CollectionIndexType,
    pub(crate) value_particle_type: ParticleType,

    pub(crate) begin: Value,

    pub(crate) end: Value,

    /// Optional secondary index name. When set, the server uses this specific
    /// index for the query instead of performing an index lookup by bin name.
    pub(crate) index_name: Option<String>,

    /// Optional packed CDT context bytes for the secondary index.
    pub(crate) context: Option<Vec<CdtContext>>,

    /// Optional expression identifying which expression-based secondary index to use.
    /// Serialized as `FieldType::IndexExpression` (24) on the wire.
    pub(crate) expression: Option<Expression>,
}

impl Filter {
    /// Creates a new filter instance. For internal use only.
    pub(crate) fn new(
        bin_name: &str,
        collection_index_type: CollectionIndexType,
        value_particle_type: ParticleType,
        begin: Value,
        end: Value,
    ) -> Self {
        Filter {
            bin_name: bin_name.to_owned(),
            collection_index_type,
            value_particle_type,
            begin,
            end,
            index_name: None,
            context: None,
            expression: None,
        }
    }

    /// Creates a new filter instance that targets a specific secondary index by name.
    pub(crate) fn new_by_index(
        index_name: &str,
        collection_index_type: CollectionIndexType,
        value_particle_type: ParticleType,
        begin: Value,
        end: Value,
    ) -> Self {
        Filter {
            bin_name: String::new(),
            collection_index_type,
            value_particle_type,
            begin,
            end,
            index_name: Some(index_name.to_owned()),
            context: None,
            expression: None,
        }
    }

    // ========================================================================
    // Equality filters
    // ========================================================================

    /// Creates an equality filter for queries.
    ///
    /// Supports integer, string, and blob values (compile-time validated).
    ///
    /// # Examples
    /// ```
    /// # use aerospike_core::query::Filter;
    /// let f = Filter::equal("bin_name", 42_i64);
    /// let f = Filter::equal("bin_name", "hello");
    /// let f = Filter::equal("bin_name", vec![1u8, 2, 3]);
    /// ```
    pub fn equal(bin_name: &str, value: impl EqFilterValue) -> Self {
        let val = value.into_filter_value();
        Filter::new(
            bin_name,
            CollectionIndexType::Default,
            val.particle_type(),
            val.clone(),
            val,
        )
    }

    /// Creates an equality filter for query targeting a specific secondary index by name.
    pub fn equal_by_index(index_name: &str, value: impl EqFilterValue) -> Self {
        let val = value.into_filter_value();
        Filter::new_by_index(
            index_name,
            CollectionIndexType::Default,
            val.particle_type(),
            val.clone(),
            val,
        )
    }

    // ========================================================================
    // Range filters
    // ========================================================================

    /// Creates a range filter for queries. Only integer values are supported (compile-time
    /// validated).
    ///
    /// # Examples
    /// ```
    /// # use aerospike_core::query::Filter;
    /// let f = Filter::range("bin_name", 0_i64, 100_i64);
    /// ```
    pub fn range(bin_name: &str, begin: impl RangeFilterValue, end: impl RangeFilterValue) -> Self {
        let begin = begin.into_filter_value();
        let end = end.into_filter_value();
        Filter::new(
            bin_name,
            CollectionIndexType::Default,
            begin.particle_type(),
            begin,
            end,
        )
    }

    /// Creates a range filter for query targeting a specific secondary index by name.
    pub fn range_by_index(
        index_name: &str,
        begin: impl RangeFilterValue,
        end: impl RangeFilterValue,
    ) -> Self {
        let begin = begin.into_filter_value();
        let end = end.into_filter_value();
        Filter::new_by_index(
            index_name,
            CollectionIndexType::Default,
            begin.particle_type(),
            begin,
            end,
        )
    }

    // ========================================================================
    // Contains filters
    // ========================================================================

    /// Creates a contains filter for queries on a collection index.
    ///
    /// Supports integer, string, and blob values (compile-time validated).
    ///
    /// # Examples
    /// ```
    /// # use aerospike_core::query::Filter;
    /// # use aerospike_core::CollectionIndexType;
    /// let f = Filter::contains("bin_name", 42_i64, CollectionIndexType::List);
    /// ```
    pub fn contains(bin_name: &str, value: impl EqFilterValue, cit: CollectionIndexType) -> Self {
        let val = value.into_filter_value();
        Filter::new(bin_name, cit, val.particle_type(), val.clone(), val)
    }

    /// Creates a contains filter for query on a collection index targeting a specific secondary
    /// index by name.
    pub fn contains_by_index(
        index_name: &str,
        value: impl EqFilterValue,
        cit: CollectionIndexType,
    ) -> Self {
        let val = value.into_filter_value();
        Filter::new_by_index(index_name, cit, val.particle_type(), val.clone(), val)
    }

    // ========================================================================
    // Contains range filters
    // ========================================================================

    /// Creates a contains range filter for queries on a collection index. Only integer values
    /// are supported (compile-time validated).
    ///
    /// # Examples
    /// ```
    /// # use aerospike_core::query::Filter;
    /// # use aerospike_core::CollectionIndexType;
    /// let f = Filter::contains_range("bin_name", 0_i64, 100_i64, CollectionIndexType::List);
    /// ```
    pub fn contains_range(
        bin_name: &str,
        begin: impl RangeFilterValue,
        end: impl RangeFilterValue,
        cit: CollectionIndexType,
    ) -> Self {
        let begin = begin.into_filter_value();
        let end = end.into_filter_value();
        Filter::new(bin_name, cit, begin.particle_type(), begin, end)
    }

    /// Creates a contains range filter for query on a collection index targeting a specific
    /// secondary index by name.
    pub fn contains_range_by_index(
        index_name: &str,
        begin: impl RangeFilterValue,
        end: impl RangeFilterValue,
        cit: CollectionIndexType,
    ) -> Self {
        let begin = begin.into_filter_value();
        let end = end.into_filter_value();
        Filter::new_by_index(index_name, cit, begin.particle_type(), begin, end)
    }

    // ========================================================================
    // Geo "within region" filters
    // ========================================================================

    /// Creates a geo-spatial "points within region" filter for queries.
    ///
    /// For queries on a collection index, use [`Filter::geo_within_region_cit`].
    pub fn geo_within_region(bin_name: &str, region: &str) -> Self {
        let region = Value::String(region.to_owned());
        Filter::new(
            bin_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            region.clone(),
            region,
        )
    }

    /// Creates a geo-spatial "points within region" filter for queries on a collection index.
    pub fn geo_within_region_cit(bin_name: &str, region: &str, cit: CollectionIndexType) -> Self {
        let region = Value::String(region.to_owned());
        Filter::new(bin_name, cit, ParticleType::GEOJSON, region.clone(), region)
    }

    /// Creates a geo-spatial "points within region" filter targeting a specific secondary index
    /// by name.
    pub fn geo_within_region_by_index(index_name: &str, region: &str) -> Self {
        let region = Value::String(region.to_owned());
        Filter::new_by_index(
            index_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            region.clone(),
            region,
        )
    }

    /// Creates a geo-spatial "points within region" filter targeting a specific secondary index
    /// by name on a collection index.
    pub fn geo_within_region_by_index_cit(
        index_name: &str,
        region: &str,
        cit: CollectionIndexType,
    ) -> Self {
        let region = Value::String(region.to_owned());
        Filter::new_by_index(
            index_name,
            cit,
            ParticleType::GEOJSON,
            region.clone(),
            region,
        )
    }

    // ========================================================================
    // Geo "within radius" filters
    // ========================================================================

    /// Creates a geo-spatial "points within radius" filter for queries.
    ///
    /// For queries on a collection index, use [`Filter::geo_within_radius_cit`].
    pub fn geo_within_radius(bin_name: &str, lng: f64, lat: f64, radius: f64) -> Self {
        let geo_json = geo_circle_json(lng, lat, radius);
        Filter::new(
            bin_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json,
        )
    }

    /// Creates a geo-spatial "points within radius" filter for queries on a collection index.
    pub fn geo_within_radius_cit(
        bin_name: &str,
        lng: f64,
        lat: f64,
        radius: f64,
        cit: CollectionIndexType,
    ) -> Self {
        let geo_json = geo_circle_json(lng, lat, radius);
        Filter::new(
            bin_name,
            cit,
            ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json,
        )
    }

    /// Creates a geo-spatial "points within radius" filter targeting a specific secondary index
    /// by name.
    pub fn geo_within_radius_by_index(index_name: &str, lng: f64, lat: f64, radius: f64) -> Self {
        let geo_json = geo_circle_json(lng, lat, radius);
        Filter::new_by_index(
            index_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json,
        )
    }

    /// Creates a geo-spatial "points within radius" filter targeting a specific secondary index
    /// by name on a collection index.
    pub fn geo_within_radius_by_index_cit(
        index_name: &str,
        lng: f64,
        lat: f64,
        radius: f64,
        cit: CollectionIndexType,
    ) -> Self {
        let geo_json = geo_circle_json(lng, lat, radius);
        Filter::new_by_index(
            index_name,
            cit,
            ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json,
        )
    }

    // ========================================================================
    // Geo "regions containing point" filters
    // ========================================================================

    /// Creates a geo-spatial "regions containing point" filter for queries.
    ///
    /// For queries on a collection index, use [`Filter::geo_contains_cit`].
    pub fn geo_contains(bin_name: &str, point: &str) -> Self {
        let point = Value::String(point.to_owned());
        Filter::new(
            bin_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            point.clone(),
            point,
        )
    }

    /// Creates a geo-spatial "regions containing point" filter for queries on a collection index.
    pub fn geo_contains_cit(bin_name: &str, point: &str, cit: CollectionIndexType) -> Self {
        let point = Value::String(point.to_owned());
        Filter::new(bin_name, cit, ParticleType::GEOJSON, point.clone(), point)
    }

    /// Creates a geo-spatial "regions containing point" filter targeting a specific secondary
    /// index by name.
    pub fn geo_contains_by_index(index_name: &str, point: &str) -> Self {
        let point = Value::String(point.to_owned());
        Filter::new_by_index(
            index_name,
            CollectionIndexType::Default,
            ParticleType::GEOJSON,
            point.clone(),
            point,
        )
    }

    /// Creates a geo-spatial "regions containing point" filter targeting a specific secondary
    /// index by name on a collection index.
    pub fn geo_contains_by_index_cit(
        index_name: &str,
        point: &str,
        cit: CollectionIndexType,
    ) -> Self {
        let point = Value::String(point.to_owned());
        Filter::new_by_index(index_name, cit, ParticleType::GEOJSON, point.clone(), point)
    }

    // ========================================================================
    // Builder methods
    // ========================================================================

    /// Specifies which **expression-based secondary index** to use for this query filter.
    ///
    /// The expression must match the one used when the index was created via
    /// [`Client::create_index_using_expression`](crate::Client::create_index_using_expression).
    /// The server uses this to locate the correct secondary index.
    ///
    /// This is different from [`BasePolicy::filter_expression`](crate::policy::BasePolicy::filter_expression),
    /// which is a **post-filter** applied to records after the index lookup.
    ///
    /// Returns `self` for method chaining.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use aerospike_core::query::Filter;
    /// # use aerospike_core::expressions;
    /// // The expression must match the one used in create_index_using_expression
    /// let exp = expressions::int_bin("a".to_string());
    /// let f = Filter::range("a", 0_i64, 100_i64)
    ///     .expression(exp);
    /// ```
    #[must_use]
    pub fn expression(mut self, exp: Expression) -> Self {
        self.expression = Some(exp);
        self.bin_name = String::new();
        self
    }

    /// Sets the CDT context for this filter, specifying the path to an element within a nested
    /// CDT structure. This is used for secondary indexes on elements within a CDT.
    ///
    /// Returns `self` for method chaining.
    ///
    /// # Examples
    /// ```rust,no_run
    /// # use aerospike_core::query::Filter;
    /// # use aerospike_core::operations::cdt_context::ctx_list_index;
    /// let f = Filter::range("bin_name", 0_i64, 100_i64)
    ///     .context(vec![ctx_list_index(0)]);
    /// ```
    #[must_use]
    pub fn context(mut self, ctx: Vec<CdtContext>) -> Self {
        if !ctx.is_empty() {
            self.context = Some(ctx);
        }
        self
    }

    pub(crate) fn estimate_size(&self) -> Result<usize> {
        // bin name size(1) + particle type size(1)
        //     + begin particle size(4) + end particle size(4) = 10
        Ok(self.bin_name.len() + self.begin.estimate_size()? + self.end.estimate_size()? + 10)
    }

    pub(crate) fn write(&self, buffer: &mut Buffer) -> Result<()> {
        buffer.write_u8(self.bin_name.len() as u8);
        buffer.write_str(&self.bin_name);
        buffer.write_u8(self.value_particle_type.clone() as u8);

        buffer.write_u32(self.begin.estimate_size()? as u32);
        self.begin.write_to(buffer)?;

        buffer.write_u32(self.end.estimate_size()? as u32);
        self.end.write_to(buffer)?;
        Ok(())
    }
}

// ============================================================================
// Deprecated macros — prefer Filter methods instead
// ============================================================================

/// Creates equality filter for queries.
///
/// **Deprecated**: Use [`Filter::equal`] instead.
#[deprecated(note = "Use Filter::equal() instead")]
#[macro_export]
macro_rules! as_eq {
    ($bin_name:expr, $val:expr) => {{
        use $crate::query::filter::EqFilterValue;
        let val = EqFilterValue::into_filter_value($val);
        $crate::query::Filter::new(
            $bin_name,
            $crate::CollectionIndexType::Default,
            val.particle_type(),
            val.clone(),
            val.clone(),
        )
    }};
}

/// Creates range filter for queries.
///
/// **Deprecated**: Use [`Filter::range`] instead.
#[deprecated(note = "Use Filter::range() instead")]
#[macro_export]
macro_rules! as_range {
    ($bin_name:expr, $begin:expr, $end:expr) => {{
        use $crate::query::filter::RangeFilterValue;
        let begin = RangeFilterValue::into_filter_value($begin);
        let end = RangeFilterValue::into_filter_value($end);
        $crate::query::Filter::new(
            $bin_name,
            $crate::CollectionIndexType::Default,
            begin.particle_type(),
            begin,
            end,
        )
    }};
}

/// Creates contains filter for queries on a collection index.
///
/// **Deprecated**: Use [`Filter::contains`] instead.
#[deprecated(note = "Use Filter::contains() instead")]
#[macro_export]
macro_rules! as_contains {
    ($bin_name:expr, $val:expr, $cit:expr) => {{
        use $crate::query::filter::EqFilterValue;
        let val = EqFilterValue::into_filter_value($val);
        $crate::query::Filter::new(
            $bin_name,
            $cit,
            val.particle_type(),
            val.clone(),
            val.clone(),
        )
    }};
}

/// Creates contains range filter for queries on a collection index.
///
/// **Deprecated**: Use [`Filter::contains_range`] instead.
#[deprecated(note = "Use Filter::contains_range() instead")]
#[macro_export]
macro_rules! as_contains_range {
    ($bin_name:expr, $begin:expr, $end:expr, $cit:expr) => {{
        use $crate::query::filter::RangeFilterValue;
        let begin = RangeFilterValue::into_filter_value($begin);
        let end = RangeFilterValue::into_filter_value($end);
        $crate::query::Filter::new($bin_name, $cit, begin.particle_type(), begin, end)
    }};
}

/// Creates geo-spatial "points within region" filter for queries.
///
/// **Deprecated**: Use [`Filter::geo_within_region`] or [`Filter::geo_within_region_cit`]
/// instead.
#[deprecated(note = "Use Filter::geo_within_region() instead")]
#[macro_export]
macro_rules! as_within_region {
    ($bin_name:expr, $region:expr) => {{
        let region = $crate::Value::String(String::from($region));
        $crate::query::Filter::new(
            $bin_name,
            $crate::CollectionIndexType::Default,
            $crate::ParticleType::GEOJSON,
            region.clone(),
            region.clone(),
        )
    }};
    ($bin_name:expr, $region:expr, $cit:expr) => {{
        let region = $crate::Value::String(String::from($region));
        $crate::query::Filter::new(
            $bin_name,
            $cit,
            $crate::ParticleType::GEOJSON,
            region.clone(),
            region.clone(),
        )
    }};
}

/// Creates geo-spatial "points within radius" filter for queries.
///
/// **Deprecated**: Use [`Filter::geo_within_radius`] or [`Filter::geo_within_radius_cit`]
/// instead.
#[deprecated(note = "Use Filter::geo_within_radius() instead")]
#[macro_export]
macro_rules! as_within_radius {
    ($bin_name:expr, $lat:expr, $lng:expr, $radius:expr) => {{
        let lat = $lat as f64;
        let lng = $lng as f64;
        let radius = $radius as f64;
        let geo_json = format!(
            "{{ \"type\": \"AeroCircle\", \"coordinates\": [[{:.8}, {:.8}], {}] }}",
            lng, lat, radius
        );
        let geo_json = $crate::Value::String(geo_json);
        $crate::query::Filter::new(
            $bin_name,
            $crate::CollectionIndexType::Default,
            $crate::ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json.clone(),
        )
    }};
    ($bin_name:expr, $lat:expr, $lng:expr, $radius:expr, $cit:expr) => {{
        let lat = $lat as f64;
        let lng = $lng as f64;
        let radius = $radius as f64;
        let geo_json = format!(
            "{{ \"type\": \"AeroCircle\", \"coordinates\": [[{:.8}, {:.8}], {}] }}",
            lng, lat, radius
        );
        let geo_json = $crate::Value::String(geo_json);
        $crate::query::Filter::new(
            $bin_name,
            $cit,
            $crate::ParticleType::GEOJSON,
            geo_json.clone(),
            geo_json.clone(),
        )
    }};
}

/// Creates geo-spatial "regions containing point" filter for queries.
///
/// **Deprecated**: Use [`Filter::geo_contains`] or [`Filter::geo_contains_cit`] instead.
#[deprecated(note = "Use Filter::geo_contains() instead")]
#[macro_export]
macro_rules! as_regions_containing_point {
    ($bin_name:expr, $point:expr) => {{
        let point = $crate::Value::String(String::from($point));
        $crate::query::Filter::new(
            $bin_name,
            $crate::CollectionIndexType::Default,
            $crate::ParticleType::GEOJSON,
            point.clone(),
            point.clone(),
        )
    }};
    ($bin_name:expr, $point:expr, $cit:expr) => {{
        let point = $crate::Value::String(String::from($point));
        $crate::query::Filter::new(
            $bin_name,
            $cit,
            $crate::ParticleType::GEOJSON,
            point.clone(),
            point.clone(),
        )
    }};
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;

    // ====================================================================
    // Method-based tests
    // ====================================================================

    #[test]
    fn equal_int() {
        let f = Filter::equal("bin1", 42_i64);
        assert_eq!(f.bin_name, "bin1");
        assert_eq!(f.begin, Value::from(42_i64));
        assert_eq!(f.end, Value::from(42_i64));
    }

    #[test]
    fn equal_string() {
        let f = Filter::equal("bin1", "hello");
        assert_eq!(f.bin_name, "bin1");
        assert_eq!(f.begin, Value::from("hello".to_owned()));
    }

    #[test]
    fn equal_blob() {
        let f = Filter::equal("bin1", vec![1u8, 2, 3]);
        assert_eq!(f.bin_name, "bin1");
        assert_eq!(f.begin, Value::from(vec![1u8, 2, 3]));
    }

    #[test]
    fn equal_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);
        let f = Filter::equal("bin1", 42_i64).expression(exp);
        assert_eq!(f.bin_name, "bin1");
        assert!(f.expression.is_some());
    }

    #[test]
    fn equal_by_index() {
        let f = Filter::equal_by_index("my_index", 42_i64);
        assert_eq!(f.bin_name, "");
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn range_int() {
        let f = Filter::range("bin1", 0_i64, 100_i64);
        assert_eq!(f.bin_name, "bin1");
        assert_eq!(f.begin, Value::from(0_i64));
        assert_eq!(f.end, Value::from(100_i64));
    }

    #[test]
    fn range_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);
        let f = Filter::range("bin1", 0_i64, 100_i64).expression(exp);
        assert_eq!(f.bin_name, "bin1");
        assert!(f.expression.is_some());
    }

    #[test]
    fn range_by_index() {
        let f = Filter::range_by_index("my_index", 0_i64, 100_i64);
        assert_eq!(f.bin_name, "");
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn contains_int() {
        let f = Filter::contains("bin1", 42_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");
    }

    #[test]
    fn contains_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);
        let f = Filter::contains("bin1", 42_i64, CollectionIndexType::List).expression(exp);
        assert_eq!(f.bin_name, "bin1");
        assert!(f.expression.is_some());
    }

    #[test]
    fn contains_by_index() {
        let f = Filter::contains_by_index("my_index", 42_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "");
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn contains_range_int() {
        let f = Filter::contains_range("bin1", 0_i64, 100_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");
    }

    #[test]
    fn contains_range_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);
        let f = Filter::contains_range("bin1", 0_i64, 100_i64, CollectionIndexType::List)
            .expression(exp);
        assert_eq!(f.bin_name, "bin1");
        assert!(f.expression.is_some());
    }

    #[test]
    fn contains_range_by_index() {
        let f =
            Filter::contains_range_by_index("my_index", 0_i64, 100_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "");
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn geo_within_region_methods() {
        let f = Filter::geo_within_region("bin1", "{}");
        assert_eq!(f.bin_name, "bin1");

        let f = Filter::geo_within_region_cit("bin1", "{}", CollectionIndexType::MapValues);
        assert_eq!(f.bin_name, "bin1");
    }

    #[test]
    fn geo_within_region_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);

        let f = Filter::geo_within_region("bin1", "{}").expression(exp.clone());
        assert!(f.expression.is_some());

        let f = Filter::geo_within_region_cit("bin1", "{}", CollectionIndexType::MapValues)
            .expression(exp.clone());
        assert!(f.expression.is_some());
    }

    #[test]
    fn geo_within_region_by_index_methods() {
        let f = Filter::geo_within_region_by_index("my_index", "{}");
        assert_eq!(f.index_name, Some("my_index".to_owned()));

        let f = Filter::geo_within_region_by_index_cit(
            "my_index",
            "{}",
            CollectionIndexType::MapValues,
        );
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn geo_within_radius_methods() {
        let f = Filter::geo_within_radius("bin1", 3.0, 1.0, 7.0);
        assert_eq!(f.bin_name, "bin1");

        let f = Filter::geo_within_radius_cit("bin1", 3.0, 1.0, 7.0, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");
    }

    #[test]
    fn geo_within_radius_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);

        let f = Filter::geo_within_radius("bin1", 3.0, 1.0, 7.0).expression(exp.clone());
        assert!(f.expression.is_some());

        let f = Filter::geo_within_radius_cit("bin1", 3.0, 1.0, 7.0, CollectionIndexType::List)
            .expression(exp.clone());
        assert!(f.expression.is_some());
    }

    #[test]
    fn geo_within_radius_by_index_methods() {
        let f = Filter::geo_within_radius_by_index("my_index", 3.0, 1.0, 7.0);
        assert_eq!(f.index_name, Some("my_index".to_owned()));

        let f = Filter::geo_within_radius_by_index_cit(
            "my_index",
            3.0,
            1.0,
            7.0,
            CollectionIndexType::List,
        );
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    #[test]
    fn geo_contains_methods() {
        let f = Filter::geo_contains("bin1", "{}");
        assert_eq!(f.bin_name, "bin1");

        let f = Filter::geo_contains_cit("bin1", "{}", CollectionIndexType::MapValues);
        assert_eq!(f.bin_name, "bin1");
    }

    #[test]
    fn geo_contains_with_expression() {
        use crate::expressions;
        let exp = expressions::int_val(1);

        let f = Filter::geo_contains("bin1", "{}").expression(exp.clone());
        assert!(f.expression.is_some());

        let f = Filter::geo_contains_cit("bin1", "{}", CollectionIndexType::MapValues)
            .expression(exp.clone());
        assert!(f.expression.is_some());
    }

    #[test]
    fn geo_contains_by_index_methods() {
        let f = Filter::geo_contains_by_index("my_index", "{}");
        assert_eq!(f.index_name, Some("my_index".to_owned()));

        let f = Filter::geo_contains_by_index_cit("my_index", "{}", CollectionIndexType::MapValues);
        assert_eq!(f.index_name, Some("my_index".to_owned()));
    }

    // ====================================================================
    // Context builder tests
    // ====================================================================

    #[test]
    fn ctx_builder() {
        use crate::operations::cdt_context::ctx_list_index;

        let f = Filter::equal("bin1", 42_i64).context(vec![ctx_list_index(0)]);
        assert!(f.context.is_some());
        assert_eq!(f.context.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn ctx_empty_is_none() {
        let f = Filter::equal("bin1", 42_i64).context(vec![]);
        assert!(f.context.is_none());
    }

    #[test]
    fn ctx_with_range() {
        use crate::operations::cdt_context::ctx_list_index;

        let f = Filter::range("bin1", 0_i64, 100_i64).context(vec![ctx_list_index(0)]);
        assert!(f.context.is_some());
        assert_eq!(f.bin_name, "bin1");
    }

    // ====================================================================
    // Deprecated macro tests (ensure they still work)
    // ====================================================================

    #[test]
    fn deprecated_macros_still_work() {
        let f = as_eq!("bin1", 42_i64);
        assert_eq!(f.bin_name, "bin1");

        let f = as_range!("bin1", 0_i64, 100_i64);
        assert_eq!(f.bin_name, "bin1");

        let f = as_contains!("bin1", 42_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");

        let f = as_contains_range!("bin1", 0_i64, 100_i64, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");

        let f = as_within_region!("bin1", "{}");
        assert_eq!(f.bin_name, "bin1");

        let f = as_within_region!("bin1", "{}", CollectionIndexType::MapValues);
        assert_eq!(f.bin_name, "bin1");

        let f = as_within_radius!("bin1", 1, 3, 7);
        assert_eq!(f.bin_name, "bin1");

        let f = as_within_radius!("bin1", 1, 3, 7, CollectionIndexType::List);
        assert_eq!(f.bin_name, "bin1");

        let f = as_regions_containing_point!("bin1", "{}");
        assert_eq!(f.bin_name, "bin1");

        let f = as_regions_containing_point!("bin1", "{}", CollectionIndexType::MapValues);
        assert_eq!(f.bin_name, "bin1");
    }
}
