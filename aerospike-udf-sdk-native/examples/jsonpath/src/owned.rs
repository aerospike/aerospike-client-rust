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

//! A match, once it has been taken out of the record.
//!
//! [`Node`](crate::node::Node) borrows from the record it is reading, so it cannot outlive one call — which is
//! fine for a record UDF and useless for an aggregation, where matches have to survive until the accumulator is
//! written out. So a match becomes one of these on the way out.
//!
//! There is **one** conversion into the host's value builders, used by both the record functions and
//! `to_partial`. Two writers would have been slightly faster for the record path — it could write straight from
//! a `Node` and skip this copy — and would have been two places for the type mapping to drift apart.

use aerospike_udf_sdk_native::{Ctx, Kind, List, Map, Result, Value};

use crate::node::Node;

/// A JSON value a query matched, owned outright.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Owned {
    #[default]
    Nil,
    Bool(bool),
    Int(i64),
    Double(f64),
    Str(String),
    Blob(Vec<u8>),
    Array(Vec<Owned>),
    /// Entry order is the order the map was walked in, which for an Aerospike map is key order.
    Object(Vec<(String, Owned)>),
}

impl Owned {
    /// Take a match out of the record.
    ///
    /// **This forces whatever it touches**, which is the point: an aggregation cannot hold a lazy view of a
    /// record that is about to be closed. A path that matches a whole list bin therefore copies that list here,
    /// and a path that matches one number copies one number.
    pub fn from_node(node: &Node<'_>) -> Self {
        match node {
            Node::Nil => Owned::Nil,
            Node::Bool(b) => Owned::Bool(*b),
            Node::Int(i) => Owned::Int(*i),
            Node::Double(d) => Owned::Double(*d),
            // The *text*, not the document it may hold. If a path descended into it, the match is the inner
            // value and this arm is never reached for it; if the path selected the string itself, the string is
            // what was asked for.
            Node::Str(s) => Owned::Str(s.as_str().to_string()),
            Node::Blob(b) => Owned::Blob(b.clone()),
            other => {
                if let Some(items) = other.elements() {
                    Owned::Array(items.iter().map(Owned::from_node).collect())
                } else if let Some(entries) = other.entries() {
                    Owned::Object(
                        entries
                            .iter()
                            .map(|(k, v)| (k.clone(), Owned::from_node(v)))
                            .collect(),
                    )
                } else {
                    Owned::Nil
                }
            }
        }
    }

    /// Write a match into the host's list **straight from the node**, with no `Owned` in between.
    ///
    /// # Why this exists alongside `from_node` + `push_into`
    ///
    /// The note at the top of this file says two writers would be slightly faster for the record path and would be two
    /// places for the type mapping to drift apart. That trade was taken deliberately, and then measured: a wildcard
    /// selecting 243 scalars builds 243 `Owned` values purely to copy each one into the host list and drop it.
    ///
    /// So both writers exist now, and the drift is guarded by a test rather than by there being only one of them —
    /// `both_writers_agree` runs every document the suite has through each and asserts the results are identical.
    ///
    /// A container match is a wash: the node has to be expanded either way. The gain is on scalars, which is what a
    /// path ending in a field name selects.
    pub fn push_node_into(node: &Node<'_>, ctx: &Ctx, list: &List<'_>) -> Result<()> {
        match node {
            Node::Nil => list.push(&Value::nil()),
            Node::Bool(b) => list.push(&Value::boolean(*b)),
            Node::Int(i) => list.push_int(*i),
            Node::Double(d) => list.push_f64(*d),
            Node::Str(s) => list.push_str(s.as_str()),
            Node::Blob(b) => list.push(&Value::blob(b)),
            other => {
                if let Some(items) = other.elements() {
                    let inner = ctx.list(items.len() as u32)?;

                    for item in items {
                        Self::push_node_into(item, ctx, &inner)?;
                    }

                    list.push(&inner.as_value())
                } else if let Some(entries) = other.entries() {
                    let inner = ctx.map(entries.len() as u32)?;

                    for (k, v) in entries {
                        Self::put_node_into(v, ctx, &inner, k)?;
                    }

                    list.push(&inner.as_value())
                } else {
                    list.push(&Value::nil())
                }
            }
        }
    }

    /// The map-valued half of [`push_node_into`], keyed by a string as a JSON object is.
    fn put_node_into(
        node: &Node<'_>,
        ctx: &Ctx,
        map: &Map<'_>,
        key: &str,
    ) -> Result<()> {
        match node {
            // Mirrors `put_into` arm for arm - nil, bool and blob go through the generic `put`, and stringifying them
            // here instead was the first version of this function and would have made the two writers disagree.
            Node::Nil => map.put(&Value::string(key), &Value::nil()),
            Node::Bool(b) => map.put(&Value::string(key), &Value::boolean(*b)),
            Node::Int(i) => map.put_str_int(key, *i),
            Node::Double(d) => map.put_str_f64(key, *d),
            Node::Str(s) => map.put_str_str(key, s.as_str()),
            Node::Blob(b) => map.put(&Value::string(key), &Value::blob(b)),
            other => {
                if let Some(items) = other.elements() {
                    let inner = ctx.list(items.len() as u32)?;

                    for item in items {
                        Self::push_node_into(item, ctx, &inner)?;
                    }

                    map.put_str_list(key, &inner)
                } else if let Some(entries) = other.entries() {
                    let inner = ctx.map(entries.len() as u32)?;

                    for (k, v) in entries {
                        Self::put_node_into(v, ctx, &inner, k)?;
                    }

                    map.put_str_map(key, &inner)
                } else {
                    map.put(&Value::string(key), &Value::nil())
                }
            }
        }
    }

    /// Read one back off the wire, for `from_partial`.
    pub fn from_value(value: Value<'_>) -> Self {
        match value.kind() {
            Kind::Absent | Kind::Nil | Kind::Other => Owned::Nil,
            Kind::Bool => Owned::Bool(value.as_bool().unwrap_or(false)),
            Kind::Int => Owned::Int(value.as_i64().unwrap_or(0)),
            Kind::Double => Owned::Double(value.as_f64().unwrap_or(0.0)),
            Kind::Str | Kind::GeoJson => Owned::Str(value.as_str().unwrap_or_default().to_string()),
            Kind::Blob => Owned::Blob(value.as_bytes().unwrap_or_default().to_vec()),
            Kind::List => match value.as_list() {
                Some(list) => {
                    // Collected first: `for_each` cannot carry a `Result` out of the host's callback, and
                    // nothing here is fallible anyway.
                    let mut items = Vec::with_capacity(list.len() as usize);

                    list.for_each(|_, item| {
                        items.push(Owned::from_value(item));

                        true
                    });

                    Owned::Array(items)
                }
                None => Owned::Nil,
            },
            Kind::Map => match value.as_map() {
                Some(map) => {
                    let mut entries = Vec::with_capacity(map.len() as usize);

                    map.for_each(|key, value| {
                        if let Some(name) = key.as_str() {
                            entries.push((name.to_string(), Owned::from_value(value)));
                        }

                        true
                    });

                    Owned::Object(entries)
                }
                None => Owned::Nil,
            },
        }
    }

    /// Append this value to a list the host owns.
    ///
    /// Everything goes through the host's copying setters — `push_str` and friends — so nothing that reaches a
    /// client borrows from this `Owned` or from the record behind it.
    pub fn push_into(&self, ctx: &Ctx, list: &List<'_>) -> Result<()> {
        match self {
            Owned::Nil => list.push(&Value::nil()),
            Owned::Bool(b) => list.push(&Value::boolean(*b)),
            Owned::Int(i) => list.push_int(*i),
            Owned::Double(d) => list.push_f64(*d),
            Owned::Str(s) => list.push_str(s),
            // A blob a path could not reach into can still be a *result*, when the path names the bin itself.
            Owned::Blob(b) => list.push(&Value::blob(b)),
            Owned::Array(items) => {
                let inner = ctx.list(items.len() as u32)?;

                for item in items {
                    item.push_into(ctx, &inner)?;
                }

                list.push_list(&inner)
            }
            Owned::Object(entries) => {
                let inner = ctx.map(entries.len() as u32)?;

                for (name, value) in entries {
                    value.put_into(ctx, &inner, name)?;
                }

                list.push_map(&inner)
            }
        }
    }

    /// Put this value into a map the host owns, under `key`.
    pub fn put_into(&self, ctx: &Ctx, map: &Map<'_>, key: &str) -> Result<()> {
        match self {
            Owned::Nil => map.put(&Value::string(key), &Value::nil()),
            Owned::Bool(b) => map.put(&Value::string(key), &Value::boolean(*b)),
            Owned::Int(i) => map.put_str_int(key, *i),
            Owned::Double(d) => map.put_str_f64(key, *d),
            Owned::Str(s) => map.put_str_str(key, s),
            Owned::Blob(b) => map.put(&Value::string(key), &Value::blob(b)),
            Owned::Array(items) => {
                let inner = ctx.list(items.len() as u32)?;

                for item in items {
                    item.push_into(ctx, &inner)?;
                }

                map.put_str_list(key, &inner)
            }
            Owned::Object(entries) => {
                let inner = ctx.map(entries.len() as u32)?;

                for (name, value) in entries {
                    value.put_into(ctx, &inner, name)?;
                }

                map.put_str_map(key, &inner)
            }
        }
    }
}
