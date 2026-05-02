// Copyright 2015-2020 Aerospike, Inc.
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

//! Operation Context for nested Operations
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};

use crate::commands::buffer::Buffer;
use crate::errors::{Error, Result};
use crate::expressions::{self, Expression};
use crate::msgpack::encoder::pack_ctx_for_index;
use crate::operations::lists::{list_order_flag, ListOrderType};
use crate::operations::MapOrder;
use crate::Value;

// Empty Context for scalar operations
pub(crate) const DEFAULT_CTX: Vec<CdtContext> = vec![];

pub(crate) enum CtxType {
    Expression = 0x04,
    ListIndex = 0x10,
    ListRank = 0x11,
    ListValue = 0x13,
    MapIndex = 0x20,
    MapRank = 0x21,
    MapKey = 0x22,
    MapValue = 0x23,
    /// Map keys-in context: select map entries whose keys are contained
    /// in the provided list. Java: `CTX.mapKeysIn(...)`.
    MapKeysIn = 0x2a,
}

/// AND filter bit added on top of [`CtxType::Expression`] for
/// [`ctx_and_filter`]. Mirrors Java's `Exp.CTX_AND = 0x200` OR'd with
/// `Exp.CTX_EXP = 0x04` to produce the `0x204` context-tag value.
const CTX_AND_BIT: u16 = 0x200;

/// `CdtContext` defines Nested CDT context. Identifies the location of nested list/map to apply the operation.
///
/// for the current level.
/// An array of CTX identifies location of the list/map on multiple
/// levels on nesting.
#[derive(Debug, Clone, PartialEq)]
pub struct CdtContext {
    /// Context Type — full encoded tag including any high-bit flags
    /// such as the AND filter bit. Wider than 8 bits to fit the
    /// `0x204` tag value used by [`ctx_and_filter`].
    pub id: u16,

    /// Per-entry create-flag overlay (e.g. list/map order, pad). Stays
    /// `u8`-wide because every flag in this layer fits in 8 bits; widened
    /// only if the wire format ever needs more.
    pub flags: u8,

    /// Context Value
    pub value: Value,

    /// Pre-packed expression bytes for expression-based contexts
    pub(crate) expression: Option<Expression>,
}

/// Converts a context array to base64 to be used in info commands.
pub fn to_base64(ctx: &[CdtContext]) -> Result<String> {
    let size = pack_ctx_for_index(&mut None, ctx)?;
    let mut buf = Buffer::new(0);
    buf.resize_buffer(size)?;
    let _ = pack_ctx_for_index(&mut Some(&mut buf), ctx);
    Ok(BASE64.encode(&buf.data_buffer))
}

/// Restore a context array from the byte stream emitted by [`to_base64`]
/// (after base64-decode) or any other source that produced the same
/// CTX wire layout.
///
/// Expression-form entries (e.g. created via [`ctx_all_children`],
/// [`ctx_all_children_with_filter`], or [`ctx_and_filter`]) are restored
/// with their raw packed bytes preserved — the expression tree itself is
/// *not* reconstructed structurally. That's enough to round-trip the CTX
/// (re-packing the restored CTX produces the same bytes), but reading the
/// inner expression as Rust types requires the original
/// [`Expression`](crate::expressions::Expression) builder.
///
/// # Errors
/// - `BadResponse` on a malformed array header / odd-length entry list.
pub fn ctx_from_bytes(bytes: &[u8]) -> Result<Vec<CdtContext>> {
    let mut buf = Buffer::new(0);
    buf.data_buffer = bytes.to_vec();
    buf.data_offset = 0;

    // Top-level array of (id, value) pairs.
    let header = buf.read_u8(None);
    let total = match header {
        0x90..=0x9f => (header & 0x0f) as usize,
        0xdc => buf.read_u16(None) as usize,
        0xdd => buf.read_u32(None) as usize,
        _ => {
            return Err(Error::BadResponse(format!(
                "Invalid CTX array header: 0x{header:02x}"
            )))
        }
    };
    if !total.is_multiple_of(2) {
        return Err(Error::BadResponse(format!(
            "CTX byte stream must contain (id, value) pairs; got {total} elements"
        )));
    }

    let count = total / 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let id = read_msgpack_uint(&mut buf)?;
        let next = buf.peek();
        // bin8/bin16/bin32 → packed expression bytes. Anything else → value.
        if matches!(next, 0xc4..=0xc6) {
            let exp_bytes = read_msgpack_bin(&mut buf)?;
            out.push(CdtContext {
                id,
                flags: 0,
                value: Value::Nil,
                expression: Some(expressions::from_packed_bytes(exp_bytes)),
            });
        } else {
            let value = crate::msgpack::decoder::unpack_value(&mut buf)?;
            out.push(CdtContext {
                id,
                flags: 0,
                value,
                expression: None,
            });
        }
    }
    Ok(out)
}

/// Restore a context array from a base64-encoded byte stream — the
/// reverse of [`to_base64`].
pub fn ctx_from_base64(b64: &str) -> Result<Vec<CdtContext>> {
    let bytes = BASE64
        .decode(b64)
        .map_err(|e| Error::BadResponse(format!("Invalid base64 CTX: {e}")))?;
    ctx_from_bytes(&bytes)
}

/// Read an unsigned msgpack integer wide enough to fit Java's CTX `int`
/// type (full 32-bit range, including the high `CTX_AND` bit). We narrow
/// to `u16` because that's what the Rust `CdtContext::id` field carries.
fn read_msgpack_uint(buf: &mut Buffer) -> Result<u16> {
    let header = buf.read_u8(None);
    let v: u32 = match header {
        // positive fixint
        0x00..=0x7f => u32::from(header),
        0xcc => u32::from(buf.read_u8(None)),
        0xcd => u32::from(buf.read_u16(None)),
        0xce => buf.read_u32(None),
        // signed forms — only positive values make sense for a CTX id.
        0xd0 => {
            let v = i64::from(buf.read_u8(None) as i8);
            if v < 0 {
                return Err(Error::BadResponse(format!("Negative CTX id: {v}")));
            }
            v as u32
        }
        0xd1 => {
            let v = i64::from(buf.read_u16(None) as i16);
            if v < 0 {
                return Err(Error::BadResponse(format!("Negative CTX id: {v}")));
            }
            v as u32
        }
        0xd2 => {
            let v = i64::from(buf.read_u32(None) as i32);
            if v < 0 {
                return Err(Error::BadResponse(format!("Negative CTX id: {v}")));
            }
            v as u32
        }
        _ => {
            return Err(Error::BadResponse(format!(
                "Unexpected msgpack header in CTX id: 0x{header:02x}"
            )))
        }
    };
    if v > u32::from(u16::MAX) {
        return Err(Error::BadResponse(format!(
            "CTX id {v} exceeds 16-bit range"
        )));
    }
    Ok(v as u16)
}

/// Read a msgpack `bin8/bin16/bin32` body. Caller must have already
/// confirmed the prefix byte is one of `0xc4 / 0xc5 / 0xc6`.
fn read_msgpack_bin(buf: &mut Buffer) -> Result<Vec<u8>> {
    let header = buf.read_u8(None);
    let len = match header {
        0xc4 => buf.read_u8(None) as usize,
        0xc5 => buf.read_u16(None) as usize,
        0xc6 => buf.read_u32(None) as usize,
        _ => unreachable!("caller already peeked a bin prefix"),
    };
    if buf.data_offset + len > buf.data_buffer.len() {
        return Err(Error::BadResponse(format!(
            "CTX expression bin overflow: need {len} bytes at offset {}",
            buf.data_offset
        )));
    }
    let bytes = buf.data_buffer[buf.data_offset..buf.data_offset + len].to_vec();
    buf.data_offset += len;
    Ok(bytes)
}

/// Defines Lookup list by index offset.
///
/// If the index is negative, the resolved index starts backwards from end of list.
/// If an index is out of bounds, a parameter error will be returned.
/// Examples:
/// 0: First item.
/// 4: Fifth item.
/// -1: Last item.
/// -3: Third to last item.
pub const fn ctx_list_index(index: i64) -> CdtContext {
    CdtContext {
        id: CtxType::ListIndex as u16,
        flags: 0,
        value: Value::Int(index),
        expression: None,
    }
}

/// list with given type at index offset, given an order and pad.
pub const fn ctx_list_index_create(index: i64, order: ListOrderType, pad: bool) -> CdtContext {
    CdtContext {
        id: CtxType::ListIndex as u16,
        flags: list_order_flag(order, pad),
        value: Value::Int(index),
        expression: None,
    }
}

/// Defines Lookup list by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub const fn ctx_list_rank(rank: i64) -> CdtContext {
    CdtContext {
        id: CtxType::ListRank as u16,
        flags: 0,
        value: Value::Int(rank),
        expression: None,
    }
}

/// Defines Lookup list by value.
pub const fn ctx_list_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::ListValue as u16,
        flags: 0,
        value: key,
        expression: None,
    }
}
/// Defines Lookup map by index offset.
///
/// If the index is negative, the resolved index starts backwards from end of map.
/// If an index is out of bounds, a parameter error will be returned.
/// Examples:
/// 0: First item.
/// 4: Fifth item.
/// -1: Last item.
/// -3: Third to last item.
///
/// Takes an integer position, not an arbitrary `Value`.
pub const fn ctx_map_index(index: i64) -> CdtContext {
    CdtContext {
        id: CtxType::MapIndex as u16,
        flags: 0,
        value: Value::Int(index),
        expression: None,
    }
}

/// Defines Lookup map by rank.
/// 0 = smallest value
/// N = Nth smallest value
/// -1 = largest value
pub const fn ctx_map_rank(rank: i64) -> CdtContext {
    CdtContext {
        id: CtxType::MapRank as u16,
        flags: 0,
        value: Value::Int(rank),
        expression: None,
    }
}

/// Defines Lookup map by key.
pub const fn ctx_map_key(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u16,
        flags: 0,
        value: key,
        expression: None,
    }
}

/// Creates map with given type at map key.
pub const fn ctx_map_key_create(key: Value, order: MapOrder) -> CdtContext {
    CdtContext {
        id: CtxType::MapKey as u16,
        flags: order.flag(),
        value: key,
        expression: None,
    }
}

/// Defines Lookup map by value.
pub const fn ctx_map_value(key: Value) -> CdtContext {
    CdtContext {
        id: CtxType::MapValue as u16,
        flags: 0,
        value: key,
        expression: None,
    }
}

/// Creates a `CdtContext` for all children of the current collection that match the given
/// filter expression. Requires Aerospike Server version >= 8.1.1.
///
/// # Errors
///
/// Returns an error if the expression cannot be packed.
pub const fn ctx_all_children_with_filter(exp: Expression) -> CdtContext {
    CdtContext {
        id: CtxType::Expression as u16,
        flags: 0,
        value: Value::Nil,
        expression: Some(exp),
    }
}

/// Select map entries whose keys are contained in the provided list.
///
/// Combine with [`ctx_and_filter`] to apply additional per-entry
/// filtering on the selected keys.
/// Requires Aerospike Server version >= 8.1.2.
///
/// # Examples
/// ```ignore
/// // Given map: {alpha: 10, beta: 20, gamma: 30}
/// // Select only the entries whose keys are "alpha" or "gamma".
/// let ctx = ctx_map_keys_in(["alpha", "gamma"]);
/// ```
pub fn ctx_map_keys_in<I, V>(keys: I) -> CdtContext
where
    I: IntoIterator<Item = V>,
    V: Into<Value>,
{
    let list: Vec<Value> = keys.into_iter().map(Into::into).collect();
    CdtContext {
        id: CtxType::MapKeysIn as u16,
        flags: 0,
        value: Value::List(list),
        expression: None,
    }
}

/// Apply an additional expression filter at the current context level.
///
/// Creates an AND filter combined with the preceding CTX entry. Entries
/// must satisfy *both* the preceding context and this filter expression
/// to be included.
/// Requires Aerospike Server version >= 8.1.2.
///
/// # Restrictions
///
/// - Only one `and_filter` is allowed per context level. Combine multiple
///   conditions inside the expression using boolean ops instead of
///   chaining multiple `and_filter` entries.
/// - The preceding CTX entry must not itself be an expression
///   (`ctx_all_children` / `ctx_all_children_with_filter`).
/// - `and_filter` cannot be the first entry in the chain.
pub const fn ctx_and_filter(exp: Expression) -> CdtContext {
    CdtContext {
        // CTX_AND | CTX_EXP = 0x200 | 0x04 = 0x204
        id: CTX_AND_BIT | (CtxType::Expression as u16),
        flags: 0,
        value: Value::Nil,
        expression: Some(exp),
    }
}

/// Selects all children (elements/entries) of the current collection context.
/// Requires Aerospike Server version >= 8.1.1.
pub fn ctx_all_children() -> CdtContext {
    CdtContext {
        id: CtxType::Expression as u16,
        flags: 0,
        value: Value::Nil,
        expression: Some(expressions::bool_val(true)),
    }
}

// ===== Fluent path builder =================================================

/// Fluent builder for CDT context paths. Chains [`CdtContext`] entries
/// that target progressively-deeper levels of a nested list/map structure.
///
/// `Path` is a strict superset of the standalone `ctx_*` factories — every
/// step has a method on `Path` that delegates to the corresponding free
/// function. The builder form is just easier to read for paths longer than
/// a couple of steps.
///
/// # Examples
///
/// ```ignore
/// use aerospike::operations::cdt_context::Path;
/// use aerospike::operations::path::{select_by_path, SelectFlag};
///
/// // Equivalent of:  bin["book"].each.where(price > 10)
/// let path = Path::new()
///     .map_key("book")
///     .all_children()
///     .map_key("price")
///     .build();
///
/// let op = select_by_path("myBin", SelectFlag::VALUE, &path);
/// ```
#[derive(Debug, Default, Clone)]
pub struct Path {
    entries: Vec<CdtContext>,
}

impl Path {
    /// Start a new (empty) path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append [`ctx_list_index`].
    #[must_use]
    pub fn list_index(mut self, index: i64) -> Self {
        self.entries.push(ctx_list_index(index));
        self
    }

    /// Append [`ctx_list_index_create`].
    #[must_use]
    pub fn list_index_create(mut self, index: i64, order: ListOrderType, pad: bool) -> Self {
        self.entries.push(ctx_list_index_create(index, order, pad));
        self
    }

    /// Append [`ctx_list_rank`].
    #[must_use]
    pub fn list_rank(mut self, rank: i64) -> Self {
        self.entries.push(ctx_list_rank(rank));
        self
    }

    /// Append [`ctx_list_value`].
    #[must_use]
    pub fn list_value<V: Into<Value>>(mut self, value: V) -> Self {
        self.entries.push(ctx_list_value(value.into()));
        self
    }

    /// Append [`ctx_map_index`].
    #[must_use]
    pub fn map_index(mut self, index: i64) -> Self {
        self.entries.push(ctx_map_index(index));
        self
    }

    /// Append [`ctx_map_rank`].
    #[must_use]
    pub fn map_rank(mut self, rank: i64) -> Self {
        self.entries.push(ctx_map_rank(rank));
        self
    }

    /// Append [`ctx_map_key`]. Accepts anything that converts to a
    /// [`Value`] so callers can write `.map_key("name")` or
    /// `.map_key(42_i64)` directly.
    #[must_use]
    pub fn map_key<K: Into<Value>>(mut self, key: K) -> Self {
        self.entries.push(ctx_map_key(key.into()));
        self
    }

    /// Append [`ctx_map_key_create`].
    #[must_use]
    pub fn map_key_create<K: Into<Value>>(mut self, key: K, order: MapOrder) -> Self {
        self.entries.push(ctx_map_key_create(key.into(), order));
        self
    }

    /// Append [`ctx_map_value`].
    #[must_use]
    pub fn map_value<V: Into<Value>>(mut self, value: V) -> Self {
        self.entries.push(ctx_map_value(value.into()));
        self
    }

    /// Append [`ctx_map_keys_in`].
    /// Requires Aerospike Server version >= 8.1.2.
    #[must_use]
    pub fn map_keys_in<I, V>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        self.entries.push(ctx_map_keys_in(keys));
        self
    }

    /// Append [`ctx_all_children`].
    #[must_use]
    pub fn all_children(mut self) -> Self {
        self.entries.push(ctx_all_children());
        self
    }

    /// Append [`ctx_all_children_with_filter`].
    #[must_use]
    pub fn all_children_with_filter(mut self, exp: Expression) -> Self {
        self.entries.push(ctx_all_children_with_filter(exp));
        self
    }

    /// Append [`ctx_and_filter`].
    #[must_use]
    pub fn and_filter(mut self, exp: Expression) -> Self {
        self.entries.push(ctx_and_filter(exp));
        self
    }

    /// Append a pre-built [`CdtContext`] (escape hatch for entries that
    /// don't have a dedicated builder method, e.g. results of
    /// [`ctx_from_bytes`]).
    #[must_use]
    pub fn push(mut self, ctx: CdtContext) -> Self {
        self.entries.push(ctx);
        self
    }

    /// Append every entry from another path. Useful for composing reusable
    /// path fragments.
    #[must_use]
    pub fn extend<I: IntoIterator<Item = CdtContext>>(mut self, other: I) -> Self {
        self.entries.extend(other);
        self
    }

    /// Number of path entries chained so far.
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` when no entries have been appended.
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// View of the path as a slice. Use this to pass into
    /// [`select_by_path`](crate::operations::path::select_by_path) or
    /// [`modify_by_path`](crate::operations::path::modify_by_path) without
    /// cloning the underlying vector.
    pub fn as_slice(&self) -> &[CdtContext] {
        &self.entries
    }

    /// Consume the builder and return the underlying entry vector.
    pub fn build(self) -> Vec<CdtContext> {
        self.entries
    }
}

impl From<Path> for Vec<CdtContext> {
    fn from(p: Path) -> Self {
        p.entries
    }
}

impl AsRef<[CdtContext]> for Path {
    fn as_ref(&self) -> &[CdtContext] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- signature fixes -------------------------------------------------

    #[test]
    fn ctx_map_index_takes_integer() {
        // Regression: the old signature accepted an arbitrary Value; the
        // fixed signature must be i64-only and pack as Value::Int.
        let c = ctx_map_index(3);
        assert_eq!(c.id, CtxType::MapIndex as u16);
        assert_eq!(c.value, Value::Int(3));

        let c_neg = ctx_map_index(-1);
        assert_eq!(c_neg.value, Value::Int(-1));
    }

    // ---- mapKeysIn -------------------------------------------------------

    #[test]
    fn ctx_map_keys_in_strings() {
        let c = ctx_map_keys_in(["alpha", "gamma"]);
        assert_eq!(c.id, CtxType::MapKeysIn as u16);
        match c.value {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Value::String("alpha".into()));
                assert_eq!(items[1], Value::String("gamma".into()));
            }
            other => panic!("expected Value::List, got {:?}", other),
        }
    }

    #[test]
    fn ctx_map_keys_in_integers() {
        let c = ctx_map_keys_in([1_i64, 2, 3]);
        match c.value {
            Value::List(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], Value::Int(1));
                assert_eq!(items[2], Value::Int(3));
            }
            other => panic!("expected Value::List, got {:?}", other),
        }
    }

    // ---- andFilter -------------------------------------------------------

    #[test]
    fn ctx_and_filter_sets_combined_tag() {
        // Tag must be CTX_AND | CTX_EXP (= 0x204) so the server reads
        // it as an AND filter on top of the previous CTX entry.
        let exp = expressions::bool_val(true);
        let c = ctx_and_filter(exp);
        assert_eq!(c.id, CTX_AND_BIT | (CtxType::Expression as u16));
        assert_eq!(c.id, 0x204);
        assert!(c.expression.is_some());
    }

    // ---- to_base64 / from_base64 round-trip -----------------------------

    #[test]
    fn ctx_round_trip_simple_path() {
        let original = vec![
            ctx_map_key(Value::String("book".into())),
            ctx_list_index(2),
            ctx_map_rank(-1),
        ];
        let b64 = to_base64(&original).expect("encode");
        let restored = ctx_from_base64(&b64).expect("decode");

        assert_eq!(restored.len(), original.len());
        for (o, r) in original.iter().zip(&restored) {
            assert_eq!(o.id, r.id);
            assert_eq!(o.value, r.value);
            assert!(r.expression.is_none());
        }
    }

    #[test]
    fn ctx_round_trip_with_keys_in() {
        let original = vec![
            ctx_map_key(Value::String("users".into())),
            ctx_map_keys_in(["alice", "bob"]),
        ];
        let b64 = to_base64(&original).expect("encode");
        let restored = ctx_from_base64(&b64).expect("decode");

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[1].id, CtxType::MapKeysIn as u16);
        match &restored[1].value {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Value::String("alice".into()));
            }
            other => panic!("expected Value::List, got {:?}", other),
        }
    }

    #[test]
    fn ctx_round_trip_preserves_expression_bytes() {
        // Expression-form CTX entries must round-trip even though we
        // can't structurally re-decode them — the raw packed bytes are
        // preserved on the restored Expression so re-packing is
        // bit-identical.
        let original = vec![
            ctx_map_key(Value::String("book".into())),
            ctx_all_children(),
        ];
        let b64 = to_base64(&original).expect("encode");
        let restored = ctx_from_base64(&b64).expect("decode");

        // Re-encoding the restored vector must produce the same base64.
        let b64_again = to_base64(&restored).expect("re-encode");
        assert_eq!(b64, b64_again);
    }

    #[test]
    fn ctx_round_trip_preserves_and_filter_high_bit_id() {
        // The Go client commit 32860eb fixed Base64ToCDTContext so that
        // `(id & 0x0f) == ctxTypeExpression` — without it, an id of
        // 0x204 (CTX_AND | CTX_EXP) is misclassified as a value entry.
        // Our Rust decoder uses the wire-level bin-marker check
        // (0xc4/c5/c6 in `ctx_from_bytes`) which is structurally
        // equivalent. This test pins that behavior down so the
        // 0x204 case stays decoded as an expression.
        let original = vec![
            ctx_map_keys_in(["a", "b"]),
            ctx_and_filter(crate::expressions::bool_val(true)),
        ];
        let b64 = to_base64(&original).expect("encode");
        let restored = ctx_from_base64(&b64).expect("decode");

        assert_eq!(restored.len(), 2);
        assert_eq!(restored[1].id, 0x204, "and_filter id must round-trip");
        assert!(
            restored[1].expression.is_some(),
            "and_filter entry must be decoded as an expression, not a value"
        );

        let b64_again = to_base64(&restored).expect("re-encode");
        assert_eq!(b64, b64_again);
    }

    #[test]
    fn ctx_from_bytes_rejects_bad_header() {
        let err = ctx_from_bytes(&[0xff]).unwrap_err();
        assert!(matches!(err, Error::BadResponse(_)));
    }

    #[test]
    fn ctx_from_bytes_rejects_odd_array_length() {
        // fixarray of 3 elements (odd) — must be rejected because every
        // CTX entry needs a (id, value) pair.
        let err = ctx_from_bytes(&[0x93, 0x01, 0xc0, 0x02]).unwrap_err();
        assert!(matches!(err, Error::BadResponse(_)));
    }

    // ---- Path builder ---------------------------------------------------

    #[test]
    fn path_builder_chains_in_order() {
        let path = Path::new().map_key("book").all_children().map_key("price");

        let entries = path.as_slice();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, CtxType::MapKey as u16);
        assert_eq!(entries[1].id, CtxType::Expression as u16);
        assert_eq!(entries[2].id, CtxType::MapKey as u16);
    }

    #[test]
    fn path_builder_compatible_with_select_signature() {
        // `Path` must be acceptable wherever `&[CdtContext]` is expected,
        // via `AsRef`.
        let path = Path::new().map_key("a").list_index(0);
        let slice: &[CdtContext] = path.as_ref();
        assert_eq!(slice.len(), 2);
    }

    #[test]
    fn path_builder_into_vec() {
        let path = Path::new().map_key("a");
        let v: Vec<CdtContext> = path.into();
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn path_builder_extend_composes_fragments() {
        let prefix = Path::new().map_key("user").build();
        let path = Path::new().map_key("root").extend(prefix);
        assert_eq!(path.len(), 2);
    }
}
