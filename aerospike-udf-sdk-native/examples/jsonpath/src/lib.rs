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

//! Query a record with JSONPath, server-side.
//!
//! ```text
//! $.address.city                          a nested map bin
//! $.tags[*]                               every element of a list bin
//! $.orders[?@.total > 100].id             a filter over a list of maps
//! $..price                                every price at any depth
//! ```
//!
//! A record is the root object and its bins are the root's keys, so `$.address.city` means bin `address`, key
//! `city`. Nothing is converted to JSON text and nothing is converted to `serde_json::Value`: the query engine
//! walks the record itself through [`node::Node`], which expands only where the path looks.
//!
//! # What this example is for
//!
//! Two things the dependency-free `stats` example cannot show.
//!
//! First, that a native UDF can carry **a real library** — this one pulls `jsonpath-rust`, and with it
//! `serde_json`, `regex` and `pest`, into the server's address space. That is a cost an operator is choosing when
//! they load this, and it is stated in `Cargo.toml` rather than buried.
//!
//! Second, that **`emit_record` makes server-side filtering worth doing.** `jsonpath_filter` runs a path over
//! every record and returns the matches themselves, with no accumulator and no partials — so a query that would
//! have shipped a million records to be filtered by a client ships only the ones that matched.
//!
//! # The one sharp edge
//!
//! Aerospike map keys may be integers; JSONPath names are strings. So the key `5` is addressed as `$.m['5']` —
//! **and if the same map also holds the string key `"5"`, that is ambiguous.** This module detects it and fails
//! the call rather than picking one, because a query that quietly returns the wrong entry is worse than one that
//! refuses. See [`node::Trouble`] for why the check cannot live inside the trait.

pub mod node;
pub mod owned;

#[cfg(test)]
mod tests;

use std::ffi::CStr;
use std::rc::Rc;

use aerospike_udf_sdk_native::{
    module, udfs, Aggregation, Args, AsPartial, Ctx, Emit, Error, Fold, Record, Result, Value,
};

use jsonpath_rust::parser::model::JpQuery;
use jsonpath_rust::parser::parse_json_path;
use jsonpath_rust::query::js_path_process;

use node::{Node, Trouble};
use owned::Owned;

/// Compile a path, turning the library's parse error into a UDF error a client can read.
fn compile(path: &str) -> Result<JpQuery> {
    parse_json_path(path).map_err(|e| Error::msg(format!("bad JSONPath {path:?}: {e}")))
}

/// Run a compiled path over a record and take the matches out of it.
///
/// The `Trouble` check at the end is the whole reason that type exists: [`Queryable`] cannot fail, so an
/// ambiguous map key is recorded during expansion and turned into an error here — after the query, before the
/// answer is used.
///
/// [`Queryable`]: jsonpath_rust::query::queryable::Queryable
fn matches(rec: &Record, query: &JpQuery) -> Result<Vec<Owned>> {
    let trouble = Rc::new(Trouble::default());
    let root = Node::root(rec, &trouble);

    let found = js_path_process(query, &root)
        .map_err(|e| Error::msg(format!("JSONPath failed: {e}")))?;

    // Taken out before the results are trusted. A record whose map keys are ambiguous produces an error even if
    // the path never touched the ambiguous map - the expansion did, or the flag would not be set.
    if let Some(key) = trouble.taken() {
        return Err(Error::msg(format!(
            "map key {key:?} is ambiguous: the map holds both that string key and a number that renders \
             the same, so a JSONPath name cannot tell them apart"
        )));
    }

    Ok(found.iter().map(|p| Owned::from_node(p.val)).collect())
}

/// Whether a path matches at all, without building anything.
fn matches_any(rec: &Record, query: &JpQuery) -> Result<bool> {
    Ok(count_matches(rec, query)? != 0)
}

/// How many locations a path matches, without building any of them.
fn count_matches(rec: &Record, query: &JpQuery) -> Result<usize> {
    let trouble = Rc::new(Trouble::default());
    let root = Node::root(rec, &trouble);

    let found = js_path_process(query, &root)
        .map_err(|e| Error::msg(format!("JSONPath failed: {e}")))?;

    if let Some(key) = trouble.taken() {
        return Err(Error::msg(format!("map key {key:?} is ambiguous")));
    }

    Ok(found.len())
}

/// The **normalized path** to each location a query matched.
///
/// A normalized path is a *concrete* location — `$['orders'][2]['total']` — with no wildcards, filters or
/// descent left in it. RFC 9535 §2.7 specifies the form, and `jsonpath-rust` produces it.
///
/// Worth having on its own: it answers "what did my path actually resolve to on this record", which is the
/// question you ask when a filter matched more or fewer things than expected. It is also the input a write would
/// need — a concrete path is what translates into an Aerospike context — so this is the read-only half of that,
/// available now and useful without it.
fn matched_paths(rec: &Record, query: &JpQuery) -> Result<Vec<String>> {
    let trouble = Rc::new(Trouble::default());
    let root = Node::root(rec, &trouble);

    let found = js_path_process(query, &root)
        .map_err(|e| Error::msg(format!("JSONPath failed: {e}")))?;

    if let Some(key) = trouble.taken() {
        return Err(Error::msg(format!("map key {key:?} is ambiguous")));
    }

    Ok(found.iter().map(|p| p.path.clone()).collect())
}

udfs! {
    /// Every value `path` matches in this record, as a list.
    ///
    /// An empty list means the path matched nothing, which is different from a path that is invalid — that is an
    /// error, because a typo in a path should not look like an empty result.
    fn jsonpath<'a>(rec: &'a Record, path: &str) -> Result<Value<'a>> {
        let found = matches(rec, &compile(path)?)?;

        let out = rec.ctx().list(found.len() as u32)?;

        for value in &found {
            value.push_into(rec.ctx(), &out)?;
        }

        Ok(out.as_value())
    }

    /// Every value `path` matches, **written straight from the record** with no owned copy in between.
    ///
    /// Identical results to [`jsonpath`] — that is asserted, not assumed — and cheaper, because a match no longer
    /// becomes an `Owned` on its way to the host's list. For a path ending in a field name, which selects scalars, that
    /// removes one allocation and one copy per match.
    ///
    /// [`jsonpath`] remains the one to reach for when the same code also feeds an aggregation: a partial has to outlive
    /// the call, and only an `Owned` can.
    fn jsonpath_values<'a>(rec: &'a Record, path: &str) -> Result<Value<'a>> {
        let query = compile(path)?;
        let trouble = Rc::new(Trouble::default());
        let root = Node::root(rec, &trouble);

        let found = js_path_process(&query, &root)
            .map_err(|e| Error::msg(format!("JSONPath failed: {e}")))?;

        // Checked before the results are trusted, exactly as `matches` does - the expansion sets the flag even when the
        // path never touched the ambiguous map.
        if let Some(key) = trouble.taken() {
            return Err(Error::msg(format!(
                "map key {key:?} is ambiguous: the map holds both that string key and a number that renders \
                 the same, so a JSONPath name cannot tell them apart"
            )));
        }

        let out = rec.ctx().list(found.len() as u32)?;

        for matched in &found {
            Owned::push_node_into(matched.val, rec.ctx(), &out)?;
        }

        Ok(out.as_value())
    }

    /// The first value `path` matches, or nil.
    ///
    /// Built as a one-element list and read back out, which looks roundabout and is the cheapest correct thing:
    /// a match that is a string has to be copied into memory the *host* owns before it can be a result, and
    /// pushing it into a host list is how that happens. Returning `&str` from here would hand the client a
    /// pointer into a `String` this function is about to drop.
    fn jsonpath_one<'a>(rec: &'a Record, path: &str) -> Result<Value<'a>> {
        let found = matches(rec, &compile(path)?)?;

        let Some(first) = found.first() else {
            return Ok(Value::nil());
        };

        let holder = rec.ctx().list(1)?;

        first.push_into(rec.ctx(), &holder)?;

        Ok(holder.get(0))
    }

    /// Whether `path` matches anything in this record.
    ///
    /// The cheap one: nothing is copied out of the record and no result value is built.
    fn jsonpath_exists(rec: &Record, path: &str) -> Result<bool> {
        matches_any(rec, &compile(path)?)
    }

    /// How many locations `path` matches.
    ///
    /// Cheaper than `jsonpath` when the count is all you want: the matches are located but never copied out of
    /// the record and no result list is built. `jsonpath-rust` dropped `length()` in 1.0, so a path cannot ask
    /// this of itself.
    fn jsonpath_count(rec: &Record, path: &str) -> Result<i64> {
        Ok(count_matches(rec, &compile(path)?)? as i64)
    }

    /// The **normalized path** to each match, as a list of strings.
    ///
    /// `$..total` on a record answers something like `["$['orders'][0]['total']", "$['orders'][1]['total']"]` —
    /// concrete locations with no wildcards or filters left in them.
    ///
    /// This is the function to reach for when a path matched a surprising number of things, because it says
    /// *where*. It is also, not coincidentally, the shape a write needs: a concrete path is what maps onto an
    /// Aerospike context.
    fn jsonpath_paths<'a>(rec: &'a Record, path: &str) -> Result<Value<'a>> {
        let paths = matched_paths(rec, &compile(path)?)?;

        let out = rec.ctx().list(paths.len() as u32)?;

        for p in &paths {
            // `push_str` copies into memory the host owns, so nothing here borrows from `paths`.
            out.push_str(p)?;
        }

        Ok(out.as_value())
    }
}

//==========================================================
// Aggregations.
//

/// Return every record a path matches. **The filter shape.**
///
/// No accumulator and no partials: a matching record goes straight to the response through `emit_record`, which
/// costs nothing beyond the query itself. This is the case that makes server-side JSONPath worth having — the
/// alternative is shipping every record to a client and filtering it there.
///
/// # Written by hand, not by `partial!`
///
/// A compiled `JpQuery` is not a `PartialField` and never will be — it is a parse tree, not a value. So the
/// state is declared plainly and `AsPartial` is written out below. That is exactly the escape hatch
/// `partial! { @no_impl … }` exists for; using it here rather than fighting the macro is the point.
pub struct JsonPathFilter {
    path: String,
    query: Option<JpQuery>,
}

impl Default for JsonPathFilter {
    fn default() -> Self {
        Self {
            path: String::new(),
            query: None,
        }
    }
}

impl AsPartial for JsonPathFilter {
    /// Nothing is accumulated, so there is nothing to serialize. A stream carrying one nil per query thread is
    /// what a client sees, and a client reading records ignores it.
    fn to_partial<'c>(&self, _ctx: &'c Ctx) -> Result<Value<'c>> {
        Ok(Value::nil())
    }

    fn from_partial(&mut self, _partial: Value<'_>) -> Result<()> {
        Ok(())
    }
}

impl Aggregation for JsonPathFilter {
    const NAME: &'static CStr = c"jsonpath_filter";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.path = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the JSONPath"))?
            .to_string();

        // **Compiled once per accumulator, which is once per query thread** - not once per record. A 200,000
        // record query parses one path. `configure` also runs again after a dump, which is why the string is
        // kept alongside the tree.
        self.query = Some(compile(&self.path)?);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, out: &Emit) -> Result<Fold> {
        let query = self
            .query
            .as_ref()
            .ok_or_else(|| Error::msg("jsonpath_filter was not configured"))?;

        if matches_any(rec, query)? {
            // Straight to the response: the server already has this record laid out.
            out.record(rec)?;
        }

        // Never full: nothing is held.
        Ok(Fold::Continue)
    }

    fn merge(&mut self, _other: &Self) {}

    fn finalize<'c>(&mut self, _ctx: &'c Ctx) -> Result<Value<'c>> {
        Ok(Value::nil())
    }
}

/// Pull the values a path matches out of every record. **The collecting shape.**
///
/// Output grows with the input, so it dumps every `chunk` matches and the client reads the chunks as they
/// arrive. This is what you want when the answer is a column rather than a set of records — every `price` in the
/// namespace, say.
pub struct JsonPathExtract {
    path: String,
    chunk: i64,
    query: Option<JpQuery>,
    pub values: Vec<Owned>,
}

impl Default for JsonPathExtract {
    fn default() -> Self {
        Self {
            path: String::new(),
            chunk: 1000,
            query: None,
            values: Vec::new(),
        }
    }
}

impl AsPartial for JsonPathExtract {
    /// Only `values` crosses the wire. The path and the chunk size come back from `configure` on the other side,
    /// which is why they are arguments rather than state — a partial carries what was *accumulated*, not what
    /// was configured.
    fn to_partial<'c>(&self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let map = ctx.map(1)?;
        let list = ctx.list(self.values.len() as u32)?;

        for value in &self.values {
            value.push_into(ctx, &list)?;
        }

        map.put_str_list("values", &list)?;

        Ok(map.as_value())
    }

    fn from_partial(&mut self, partial: Value<'_>) -> Result<()> {
        let map = partial
            .as_map()
            .ok_or_else(|| Error::msg("a jsonpath_extract partial must be a map"))?;

        self.values.clear();

        let Some(list) = map.get_str("values").as_list() else {
            return Ok(());
        };

        // Collected first, because `for_each` answers only whether to keep going.
        let mut items = Vec::with_capacity(list.len() as usize);

        list.for_each(|_, item| {
            items.push(item);

            true
        });

        for item in items {
            self.values.push(Owned::from_value(item));
        }

        Ok(())
    }
}

impl Aggregation for JsonPathExtract {
    const NAME: &'static CStr = c"jsonpath_extract";

    fn configure(&mut self, args: Args<'_>) -> Result<()> {
        self.path = args
            .get(0)
            .as_str()
            .ok_or_else(|| Error::msg("the first argument must be the JSONPath"))?
            .to_string();

        self.chunk = args.get(1).as_i64().unwrap_or(1000).max(1);
        self.query = Some(compile(&self.path)?);

        Ok(())
    }

    fn accumulate(&mut self, rec: &Record, _out: &Emit) -> Result<Fold> {
        let query = self
            .query
            .as_ref()
            .ok_or_else(|| Error::msg("jsonpath_extract was not configured"))?;

        self.values.extend(matches(rec, query)?);

        Ok(if self.values.len() as i64 >= self.chunk {
            Fold::Full
        } else {
            Fold::Continue
        })
    }

    fn heap_used(&self) -> usize {
        // A rough figure on purpose: what an operator's `agg-memory-limit` needs is the shape of the growth, and
        // walking a nested value per record to size it exactly would cost more than the limit saves.
        self.values.len() * 32
    }

    fn merge(&mut self, other: &Self) {
        self.values.extend(other.values.iter().cloned());
    }

    fn finalize<'c>(&mut self, ctx: &'c Ctx) -> Result<Value<'c>> {
        let out = ctx.list(self.values.len() as u32)?;

        for value in &self.values {
            value.push_into(ctx, &out)?;
        }

        Ok(out.as_value())
    }
}

module! {
    name: "jsonpath",
    version: "1.0.0",
    functions: [
        jsonpath, jsonpath_values, jsonpath_one, jsonpath_exists, jsonpath_count,
        jsonpath_paths,
    ],
    aggregations: [JsonPathFilter, JsonPathExtract],
}
