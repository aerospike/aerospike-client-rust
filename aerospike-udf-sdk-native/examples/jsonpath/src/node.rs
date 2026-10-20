// Copyright 2026 Aerospike, Inc.
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

//! A record, viewed as a JSON document, expanded only where a query looks.
//!
//! This is the whole reason `jsonpath-rust` was chosen: its [`Queryable`] trait is the seam that lets a query
//! engine walk *this* type instead of `serde_json::Value`, so a record never has to be converted into a JSON
//! document to be queried like one.
//!
//! # What is lazy about it, and what is not
//!
//! **Not the reads.** By the time a UDF runs, the server has already loaded every bin into memory — `rec_get`
//! is a lookup in an in-memory bin array, not I/O. Nothing here defers a disk access, because there is no disk
//! access left to defer.
//!
//! What it defers is **construction**. `$.id` against a record whose other bin holds a ten-thousand element
//! list never walks that list: no `Vec<Node>` is built, no `String` is allocated, and the elements are never
//! converted. That is the saving, and on a wide record it is most of the work.
//!
//! One granularity limit, chosen rather than accidental: [`Queryable::get`] on an object forces **all** of that
//! object's keys, not just the one asked for, because the cache is one `OnceCell<Vec<_>>` per node rather than
//! one cell per key. Per-key laziness would need an insertion-safe map like `elsa::FrozenMap` and would buy a
//! handful of host calls against data that is already resident. The subtree laziness above is where the cost is.
//!
//! # Why `OnceCell` and not an arena
//!
//! `Queryable` hands out references — `get` returns `Option<&Self>`, `as_array` returns `Option<&Vec<Self>>`. A
//! lazy node therefore cannot build a child and return a reference to it, because the child would die at the end
//! of the call. The children have to live somewhere with a stable address for as long as the query runs.
//!
//! `OnceCell::get_or_init(&self) -> &T` is exactly that shape, and it is in `std`. The alternative — an arena
//! plus a lifetime that refers to itself — would have bought nothing here and cost a dependency and an `unsafe`
//! block. **There is no `unsafe` in this file.**

use std::cell::RefCell;
use std::cell::OnceCell;
use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

use aerospike_udf_sdk_native::{Kind, PackedCursor, Record, Value};

use jsonpath_rust::query::queryable::Queryable;

/// Something a query hit that [`Queryable`] gave no way to report.
///
/// **The trait's methods return `Option`, not `Result`.** There is no error channel: a map whose keys are
/// ambiguous cannot refuse to be read from inside `get`. So the ambiguity is recorded here and the UDF checks it
/// *after* the query, which is the difference between failing a call and answering it wrongly.
#[derive(Default)]
pub struct Trouble {
    duplicate_key: RefCell<Option<String>>,
}

impl Trouble {
    /// Crate-visible so the unit tests can pin the contract the UDF relies on: first key wins, taking
    /// clears it.
    pub(crate) fn duplicate(&self, key: &str) {
        let mut slot = self.duplicate_key.borrow_mut();

        if slot.is_none() {
            *slot = Some(key.to_string());
        }
    }

    /// The first ambiguous key a query ran into, if any.
    pub fn taken(&self) -> Option<String> {
        self.duplicate_key.borrow_mut().take()
    }
}

/// One node of the document a query walks.
///
/// The collection variants are boxed to keep this enum small: it goes into `Vec<Node>` by the thousand when a
/// list is expanded, and a `OnceCell<Vec<_>>` inline in every element would triple the size of an integer.
pub enum Node<'r> {
    Nil,
    Bool(bool),
    Int(i64),
    Double(f64),
    /// A string — **and possibly an embedded JSON document**, if a path tries to descend into it. See
    /// [`StrNode`].
    Str(Box<StrNode<'r>>),
    /// A byte blob. Deliberately **not addressable as a string**: there is no JSON equivalent, so a path cannot
    /// reach inside one and `as_str` reports nothing rather than inventing an encoding.
    Blob(Vec<u8>),
    /// An owned array: what `From<Vec<Self>>` builds for a filter literal, and what an embedded JSON array
    /// parses into.
    Array(Vec<Node<'r>>),
    /// An owned object, which only an embedded JSON document produces — a map read from a record is a cursor over
    /// packed bytes, not this.
    Object(Vec<(String, Node<'r>)>),
    /// The record itself: the root object, whose keys are bin names.
    Record(Box<RecordNode<'r>>),
    List(Box<ListNode<'r>>),
    Map(Box<MapNode<'r>>),
}

/// A string, and the document it turns out to be if anyone asks.
///
/// **A string is parsed as JSON only when a path tries to descend into it**, never when it is merely read. So
/// `$.payload` still answers with the string, and `$.payload.b` parses it and keeps going. That is what makes
/// this a strict extension rather than a change to what string bins mean.
///
/// Two guards make it safe to have on by default:
///
/// - **Only text that starts with `{` or `[` is attempted.** One byte rejects almost every ordinary string, and
///   more importantly it means a bin holding `"42"` or `"true"` is *never* silently a number or a boolean —
///   those are valid JSON scalars, and treating them as such would change what a filter means. Only objects and
///   arrays can be descended into anyway, so restricting to them costs nothing.
/// - **Attempted at most once**, whatever the answer. `None` in the cell means "tried, it is not a document".
///
/// A parse that succeeds can never yield another `Str` — input starting with `{` or `[` produces an object or an
/// array or an error — so descending through nested documents terminates. Each level strips one layer of
/// quoting, and a level that is not a document simply stops.
pub struct StrNode<'r> {
    text: String,
    parsed: OnceCell<Option<Box<Node<'r>>>>,
}

impl<'r> StrNode<'r> {
    fn new(text: String) -> Self {
        Self {
            text,
            parsed: OnceCell::new(),
        }
    }

    /// The text, whatever it turns out to mean.
    pub(crate) fn as_str(&self) -> &str {
        &self.text
    }

    /// The document this string holds, if it holds one.
    fn document(&self) -> Option<&Node<'r>> {
        self.parsed
            .get_or_init(|| parse_embedded(&self.text).map(Box::new))
            .as_deref()
    }
}

/// Parse an embedded document, or decide there isn't one.
///
/// `serde_json` is already in the dependency tree — `jsonpath-rust` depends on it unconditionally — so this costs
/// nothing that has not already been paid for.
///
/// # The gate is a pre-filter and nothing more
///
/// It exists to reject almost every string in a couple of byte comparisons, and it is written so that it **never
/// changes the answer** — anything it accepts, the parser decides on; anything it rejects, the parser would have
/// rejected too. Getting that exactly right is the whole reason [`json_whitespace`] exists rather than a call to
/// `str::trim_start`.
pub(crate) fn parse_embedded<'r>(text: &str) -> Option<Node<'r>> {
    // Leading JSON whitespace is skipped for the *check* only. The parser is handed the original text, so nothing
    // here can quietly repair input that is not actually a document.
    let gated = text.trim_start_matches(json_whitespace);

    if !gated.starts_with('{') && !gated.starts_with('[') {
        return None;
    }

    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .map(from_serde)
}

/// The four characters JSON calls whitespace: space, tab, carriage return, line feed (RFC 8259 §2).
///
/// **Deliberately not `char::is_whitespace`**, which is the Unicode set and includes things like U+00A0 NO-BREAK
/// SPACE and U+3000 IDEOGRAPHIC SPACE. Trimming those would let `"\u{00A0}{\"a\":1}"` through the gate and then —
/// if the trimmed text were what got parsed — turn it into a document, even though every other JSON tool in the
/// world rejects it. Worse, it was asymmetric: a *trailing* no-break space still failed, because the parser's own
/// whitespace rules are the ones above.
///
/// So the gate uses JSON's definition and the parser uses JSON's definition, and the two cannot disagree.
fn json_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// A parsed document as nodes. Strings inside stay strings, so a document nested three deep is still only
/// parsed as far as a path actually goes.
fn from_serde<'r>(value: serde_json::Value) -> Node<'r> {
    match value {
        serde_json::Value::Null => Node::Nil,
        serde_json::Value::Bool(b) => Node::Bool(b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Node::Int(i),
            None => Node::Double(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => Node::string(s),
        serde_json::Value::Array(items) => Node::Array(items.into_iter().map(from_serde).collect()),
        serde_json::Value::Object(map) => {
            Node::Object(map.into_iter().map(|(k, v)| (k, from_serde(v))).collect())
        }
    }
}

pub struct RecordNode<'r> {
    rec: &'r Record,
    trouble: Rc<Trouble>,
    bins: OnceCell<Vec<(String, Node<'r>)>>,
}

pub struct ListNode<'r> {
    /// A position in the bin's **packed** bytes, not a deserialised handle.
    ///
    /// This is the whole reason a query that touches nothing is now nearly free. A `List` handle only exists once
    /// the host has walked the packed form and built one `as_val` per node — for the *entire* bin, however deep,
    /// before a path is even consulted. A cursor is sixteen bytes of borrowed pointer and offset, and descending
    /// one is arithmetic.
    cursor: PackedCursor<'r>,
    trouble: Rc<Trouble>,
    items: OnceCell<Vec<Node<'r>>>,
}

pub struct MapNode<'r> {
    cursor: PackedCursor<'r>,
    trouble: Rc<Trouble>,
    entries: OnceCell<Vec<(String, Node<'r>)>>,
}

impl<'r> Node<'r> {
    /// A string node. Boxed because the parse cell would otherwise make every `Node` half again as large, and
    /// `Node` goes into `Vec` by the thousand when a list is expanded.
    pub(crate) fn string(text: String) -> Self {
        Node::Str(Box::new(StrNode::new(text)))
    }

    /// The root of the document: a record, as an object of its bins.
    pub fn root(rec: &'r Record, trouble: &Rc<Trouble>) -> Self {
        Node::Record(Box::new(RecordNode {
            rec,
            trouble: Rc::clone(trouble),
            bins: OnceCell::new(),
        }))
    }

    /// A node from one step of a cursor.
    ///
    /// `child` is `Some` exactly when the value is a list or a map, so the container cases never reach
    /// [`from_value`], whose collection arms are now unreachable — see the note on them.
    fn from_cursor(
        value: Value<'r>,
        child: Option<PackedCursor<'r>>,
        trouble: &Rc<Trouble>,
    ) -> Self {
        match child {
            Some(cursor) if cursor.kind() == Kind::Map => Node::Map(Box::new(MapNode {
                cursor,
                trouble: Rc::clone(trouble),
                entries: OnceCell::new(),
            })),
            Some(cursor) => Node::List(Box::new(ListNode {
                cursor,
                trouble: Rc::clone(trouble),
                items: OnceCell::new(),
            })),
            None => Node::from_value(value, trouble),
        }
    }

    /// A bin or element value, without expanding it.
    ///
    /// The fallback for a bin the cursor declines: a scalar, a string, a blob. Collections never reach here — they
    /// arrive as cursors — so this is now only about how Aerospike's *scalar* types are seen as JSON.
    fn from_value(value: Value<'r>, trouble: &Rc<Trouble>) -> Self {
        match value.kind() {
            Kind::Absent | Kind::Nil | Kind::Other => Node::Nil,
            Kind::Bool => Node::Bool(value.as_bool().unwrap_or(false)),
            Kind::Int => Node::Int(value.as_i64().unwrap_or(0)),
            Kind::Double => Node::Double(value.as_f64().unwrap_or(0.0)),
            // GeoJson is carried as its text, which is what it is. Parsing it into an object would be a
            // different feature and a surprising one - a path would silently reach inside a geometry.
            Kind::Str | Kind::GeoJson => Node::string(value.as_str().unwrap_or_default().to_string()),
            Kind::Blob => Node::Blob(value.as_bytes().unwrap_or_default().to_vec()),
            // **Unreachable in practice, and nil rather than a panic if it ever is.**
            //
            // Every collection now arrives as a cursor: from `rec.cursor` at the root, or as the child cursor a
            // cursor step hands back. `from_value` is only reached for a bin the cursor declined, and it declines
            // only non-collections. A collection getting here would mean a host that reports a list bin the cursor
            // cannot open, which is a contradiction - so this reports nothing rather than materialising a tree the
            // conversion exists to avoid.
            Kind::List | Kind::Map => Node::Nil,
        }
    }

    /// The object entries of whatever this is, or nothing if it is not an object.
    ///
    /// A string is asked whether it holds a document, which is the whole embedded-JSON feature: descending is
    /// what triggers the parse.
    pub(crate) fn entries(&self) -> Option<&Vec<(String, Node<'r>)>> {
        match self {
            Node::Record(r) => Some(r.entries()),
            Node::Map(m) => Some(m.entries()),
            Node::Object(entries) => Some(entries),
            Node::Str(s) => s.document()?.entries(),
            _ => None,
        }
    }

    /// The array elements of whatever this is, or nothing if it is not an array.
    pub(crate) fn elements(&self) -> Option<&Vec<Node<'r>>> {
        match self {
            Node::Array(items) => Some(items),
            Node::List(l) => Some(l.items()),
            Node::Str(s) => s.document()?.elements(),
            _ => None,
        }
    }
}

impl<'r> RecordNode<'r> {
    fn entries(&self) -> &Vec<(String, Node<'r>)> {
        self.bins.get_or_init(|| {
            let mut out = Vec::new();

            // Bin names are unique by construction, so the record root needs no ambiguity check - only maps do.
            for name in self.rec.bin_names() {
                // **A collection bin becomes a cursor, not a tree.** Before this, every bin went through
                // `rec.get`, which deserialises the bin's whole value into one as_val per node - so a query like
                // `$.nosuchbin`, which matches nothing and descends nowhere, still paid for the entire document.
                // Measured at 632 µs on a 9,035-node record.
                match self.rec.cursor(&name) {
                    Ok(Some(cursor)) => {
                        let node = if cursor.kind() == Kind::Map {
                            Node::Map(Box::new(MapNode {
                                cursor,
                                trouble: Rc::clone(&self.trouble),
                                entries: OnceCell::new(),
                            }))
                        } else {
                            Node::List(Box::new(ListNode {
                                cursor,
                                trouble: Rc::clone(&self.trouble),
                                items: OnceCell::new(),
                            }))
                        };

                        out.push((name, node));

                        continue;
                    }
                    // Not a collection, or a shape the cursor declines - a staged value, a masked set. Either way
                    // the scalar path below is correct, and it is the same code as before.
                    Ok(None) | Err(_) => {}
                }

                let value = match self.rec.get(&name) {
                    Ok(v) => v,
                    // A bin that cannot be read is a bin the query cannot match, which is the same thing a
                    // missing bin is. Failing the whole query over one unreadable bin would be worse.
                    Err(_) => continue,
                };

                out.push((name, Node::from_value(value, &self.trouble)));
            }

            out
        })
    }
}

impl<'r> ListNode<'r> {
    fn items(&self) -> &Vec<Node<'r>> {
        self.items.get_or_init(|| {
            let mut out = Vec::with_capacity(self.cursor.len() as usize);
            let trouble = &self.trouble;

            // The cursor is `Copy`, so expanding takes its own walker and leaves this node's position alone - which
            // matters because `items` is a cache and may be read more than once.
            let mut walk = self.cursor;

            while let Some((value, child)) = walk.next() {
                out.push(Node::from_cursor(value, child, trouble));
            }

            out
        })
    }
}

impl<'r> MapNode<'r> {
    fn entries(&self) -> &Vec<(String, Node<'r>)> {
        self.entries.get_or_init(|| {
            let trouble = &self.trouble;

            // Collected first, keys and all, because the ambiguity check needs to see every key before any of
            // them can be trusted. One pass to gather, one to check.
            let mut gathered: Vec<(Option<String>, Node<'r>)> = Vec::new();

            // A map iterates as alternating key and value, which is how msgpack stores one. A trailing key with no
            // value would mean a malformed particle; it is dropped rather than paired with something invented.
            let mut walk = self.cursor;

            while let Some((key, key_child)) = walk.next() {
                let Some((value, child)) = walk.next() else {
                    break;
                };

                // A key that is itself a container has no name a path could use, and `key_name` already declines
                // anything that is not a string or an integer.
                let name = if key_child.is_some() { None } else { key_name(key) };

                gathered.push((name, Node::from_cursor(value, child, trouble)));
            }

            let mut seen: HashSet<&str> = HashSet::with_capacity(gathered.len());
            let mut ambiguous: Option<String> = None;

            for (name, _) in &gathered {
                if let Some(name) = name {
                    if !seen.insert(name.as_str()) {
                        ambiguous = Some(name.clone());
                        break;
                    }
                }
            }

            if let Some(name) = ambiguous {
                trouble.duplicate(&name);
            }

            // Keys that have no string form - a blob key - are dropped rather than named something invented.
            // They are unreachable by a path either way; the difference is that this way nothing is misreported.
            gathered
                .into_iter()
                .filter_map(|(name, node)| name.map(|n| (n, node)))
                .collect()
        })
    }
}

/// A map key as a JSONPath name.
///
/// Aerospike lets a map be keyed by integers and blobs; JSONPath names are strings. So integers are rendered,
/// which is what makes `$.m['5']` reach the entry keyed by the number 5 — **and what makes it ambiguous if the
/// same map also has the string key `"5"`.** That case is detected rather than silently resolved: see
/// [`Trouble`].
///
/// Doubles are rendered with `{:?}`, which always emits a decimal point, so a double key can never collide with
/// an integer one by accident of formatting. The check above does not rely on that — it compares the rendered
/// names whatever produced them — but it means the common case never trips it.
fn key_name(key: Value<'_>) -> Option<String> {
    match key.kind() {
        Kind::Str => key.as_str().map(str::to_string),
        Kind::Int => key.as_i64().map(|i| i.to_string()),
        Kind::Double => key.as_f64().map(|d| format!("{d:?}")),
        Kind::Bool => key.as_bool().map(|b| b.to_string()),
        // A blob key has no string form worth inventing.
        _ => None,
    }
}

//==========================================================
// What `Queryable` asks for.
//

impl Default for Node<'_> {
    fn default() -> Self {
        Node::Nil
    }
}

impl Clone for Node<'_> {
    /// Clones the *expansion state* along with the node, which is correct but not cheap for a big expanded
    /// collection. The query engine clones only literals and filter arguments, so this is off the hot path — if
    /// that ever changes, this is the thing to look at first.
    fn clone(&self) -> Self {
        match self {
            Node::Nil => Node::Nil,
            Node::Bool(b) => Node::Bool(*b),
            Node::Int(i) => Node::Int(*i),
            Node::Double(d) => Node::Double(*d),
            Node::Str(s) => Node::Str(Box::new(StrNode {
                text: s.text.clone(),
                parsed: s.parsed.clone(),
            })),
            Node::Object(entries) => Node::Object(entries.clone()),
            Node::Blob(b) => Node::Blob(b.clone()),
            Node::Array(items) => Node::Array(items.clone()),
            Node::Record(r) => Node::Record(Box::new(RecordNode {
                rec: r.rec,
                trouble: Rc::clone(&r.trouble),
                bins: r.bins.clone(),
            })),
            Node::List(l) => Node::List(Box::new(ListNode {
                cursor: l.cursor,
                trouble: Rc::clone(&l.trouble),
                items: l.items.clone(),
            })),
            Node::Map(m) => Node::Map(Box::new(MapNode {
                cursor: m.cursor,
                trouble: Rc::clone(&m.trouble),
                entries: m.entries.clone(),
            })),
        }
    }
}

impl fmt::Debug for Node<'_> {
    /// **Does not expand anything.** `Debug` is reachable from the library's error paths, and a formatter with
    /// the side effect of walking a ten-thousand element list would be a trap: the cost of printing a value
    /// would depend on whether anyone had printed it before.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Node::Nil => f.write_str("null"),
            Node::Bool(b) => write!(f, "{b}"),
            Node::Int(i) => write!(f, "{i}"),
            Node::Double(d) => write!(f, "{d:?}"),
            // The text, not the document: `Debug` must not have the side effect of parsing, for the same
            // reason it must not expand a collection.
            Node::Str(s) => write!(f, "{:?}", s.text),
            Node::Object(entries) => write!(f, "<object {} entries>", entries.len()),
            Node::Blob(b) => write!(f, "<blob {} bytes>", b.len()),
            Node::Array(items) => write!(f, "<array {} items>", items.len()),
            Node::Record(r) => match r.bins.get() {
                Some(bins) => write!(f, "<record {} bins>", bins.len()),
                None => f.write_str("<record, not expanded>"),
            },
            Node::List(l) => match l.items.get() {
                Some(items) => write!(f, "<list {} items>", items.len()),
                None => write!(f, "<list {}, not expanded>", l.cursor.len()),
            },
            Node::Map(m) => match m.entries.get() {
                Some(entries) => write!(f, "<map {} entries>", entries.len()),
                None => write!(f, "<map {}, not expanded>", m.cursor.len()),
            },
        }
    }
}

impl PartialEq for Node<'_> {
    /// Numbers compare **across** integer and double, which RFC 9535 asks for and `serde_json` does not do: a
    /// filter written `@.qty == 2` should match a bin holding `2.0`, because to everyone except a type system
    /// those are the same number.
    ///
    /// Comparing two collections forces both, which is the one place equality is expensive. Worth it: the
    /// alternative is `[?@.tags == ["a"]]` silently never matching.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Node::Nil, Node::Nil) => true,
            (Node::Bool(a), Node::Bool(b)) => a == b,
            (Node::Str(a), Node::Str(b)) => a.text == b.text,
            (Node::Blob(a), Node::Blob(b)) => a == b,
            (Node::Int(a), Node::Int(b)) => a == b,
            (Node::Double(a), Node::Double(b)) => a == b,
            (Node::Int(a), Node::Double(b)) | (Node::Double(b), Node::Int(a)) => *a as f64 == *b,
            _ => match (self.elements(), other.elements()) {
                (Some(a), Some(b)) => a == b,
                _ => match (self.entries(), other.entries()) {
                    (Some(a), Some(b)) => {
                        a.len() == b.len()
                            && a.iter().all(|(k, v)| {
                                b.iter().any(|(k2, v2)| k == k2 && v == v2)
                            })
                    }
                    _ => false,
                },
            },
        }
    }
}

impl From<&str> for Node<'_> {
    fn from(value: &str) -> Self {
        // Copied, not borrowed: the trait wants `for<'a> From<&'a str>`, so the node cannot hold a reference
        // whose lifetime it does not know.
        Node::string(value.to_string())
    }
}

impl From<String> for Node<'_> {
    fn from(value: String) -> Self {
        Node::string(value)
    }
}

impl From<bool> for Node<'_> {
    fn from(value: bool) -> Self {
        Node::Bool(value)
    }
}

impl From<i64> for Node<'_> {
    fn from(value: i64) -> Self {
        Node::Int(value)
    }
}

impl From<f64> for Node<'_> {
    fn from(value: f64) -> Self {
        Node::Double(value)
    }
}

impl<'r> From<Vec<Node<'r>>> for Node<'r> {
    fn from(value: Vec<Node<'r>>) -> Self {
        Node::Array(value)
    }
}

impl<'r> Queryable for Node<'r> {
    fn get(&self, key: &str) -> Option<&Self> {
        self.entries()?
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, node)| node)
    }

    fn as_array(&self) -> Option<&Vec<Self>> {
        self.elements()
    }

    fn as_object(&self) -> Option<Vec<(&str, &Self)>> {
        Some(
            self.entries()?
                .iter()
                .map(|(name, node)| (name.as_str(), node))
                .collect(),
        )
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Node::Str(s) => Some(&s.text),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Node::Int(i) => Some(*i),
            // A double that is exactly an integer reads as one, so `[?@.n == 3]` works whichever way the bin
            // was written. A fractional double does not, because rounding a filter's input silently is worse
            // than not matching.
            Node::Double(d) if d.fract() == 0.0 => Some(*d as i64),
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Node::Double(d) => Some(*d),
            Node::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Node::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn null() -> Self {
        Node::Nil
    }
}
