// Copyright 2015-2026 Aerospike, Inc.
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

//! Conversions between [`Value`] and Lua values, plus the `list` and `map`
//! userdata libraries exposed to aggregation UDFs.
//!
//! Mirrors the Go client's `internal/lua` package: only the globals a
//! client-side stream UDF actually needs are registered (`list`, `map`,
//! `stream`, `aerospike`).

use std::cell::RefCell;
use std::collections::HashMap;

use mlua::{Lua, MetaMethod, MultiValue, Table, UserData, UserDataMethods, Value as LuaValue};

use crate::value::FloatValue;
use crate::Value;

use super::bytes::LuaBytes;
use crate::commands::ParticleType;

/// Userdata wrapper for an Aerospike list value inside the Lua interpreter.
pub struct LuaList(pub(crate) Vec<Value>);

/// Userdata wrapper for an Aerospike map value inside the Lua interpreter.
pub struct LuaMap(pub(crate) HashMap<Value, Value>);

/// Userdata wrapper for a `GeoJSON` value inside the Lua interpreter.
/// Like the Java client's `LuaGeoJSON`: an opaque value that stringifies
/// to its `GeoJSON` text and round-trips back to [`Value::GeoJSON`].
pub struct LuaGeoJSON(pub(crate) String);

impl UserData for LuaGeoJSON {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| Ok(this.0.clone()));
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| Ok(this.0.len() as i64));
    }
}

/// Convert an Aerospike [`Value`] into a Lua value. Lists and maps become
/// `LuaList`/`LuaMap` userdata so the `list`/`map` libraries can operate on
/// them; blobs and `GeoJSON` become Lua strings (the Go client does the same).
pub fn value_to_lua(lua: &Lua, value: Value) -> mlua::Result<LuaValue> {
    Ok(match value {
        Value::Nil => LuaValue::Nil,
        Value::Bool(b) => LuaValue::Boolean(b),
        Value::Int(i) => LuaValue::Integer(i),
        Value::Float(f) => LuaValue::Number(match f {
            FloatValue::F32(bits) => f64::from(f32::from_bits(bits)),
            FloatValue::F64(bits) => f64::from_bits(bits),
        }),
        Value::String(s) => LuaValue::String(lua.create_string(&s)?),
        Value::GeoJSON(s) => LuaValue::UserData(lua.create_userdata(LuaGeoJSON(s))?),
        Value::Blob(b) => {
            LuaValue::UserData(lua.create_userdata(LuaBytes::new(b, ParticleType::BLOB))?)
        }
        Value::HLL(b) => {
            LuaValue::UserData(lua.create_userdata(LuaBytes::new(b, ParticleType::HLL))?)
        }
        Value::List(items) | Value::MultiResult(items) => {
            LuaValue::UserData(lua.create_userdata(LuaList(items))?)
        }
        Value::HashMap(m) => LuaValue::UserData(lua.create_userdata(LuaMap(m))?),
        Value::OrderedMap(m) => {
            LuaValue::UserData(lua.create_userdata(LuaMap(m.into_iter().collect()))?)
        }
        Value::SortedMap(m) => {
            LuaValue::UserData(lua.create_userdata(LuaMap(m.into_iter().collect()))?)
        }
        Value::KeyValueList(pairs) => {
            LuaValue::UserData(lua.create_userdata(LuaMap(pairs.into_iter().collect()))?)
        }
        Value::Infinity | Value::Wildcard => {
            return Err(mlua::Error::runtime(
                "Infinity/Wildcard values cannot be passed to Lua",
            ))
        }
    })
}

/// Convert a Lua value back into an Aerospike [`Value`]. Plain Lua tables are
/// interpreted as lists when their keys form the sequence `1..=n`, otherwise
/// as maps.
pub fn lua_to_value(value: &LuaValue) -> mlua::Result<Value> {
    Ok(match value {
        LuaValue::Nil => Value::Nil,
        LuaValue::Boolean(b) => Value::Bool(*b),
        LuaValue::Integer(i) => Value::Int(*i),
        LuaValue::Number(n) => Value::Float(FloatValue::from(*n)),
        LuaValue::String(s) => s.to_str().map_or_else(
            |_| Value::Blob(s.as_bytes().to_vec()),
            |utf8| Value::String(utf8.to_owned()),
        ),
        LuaValue::Table(t) => table_to_value(t)?,
        LuaValue::UserData(ud) => {
            if let Ok(list) = ud.borrow::<LuaList>() {
                Value::List(list.0.clone())
            } else if let Ok(map) = ud.borrow::<LuaMap>() {
                Value::HashMap(map.0.clone())
            } else if let Ok(bytes) = ud.borrow::<LuaBytes>() {
                if bytes.particle_type == ParticleType::HLL as u8 {
                    Value::HLL(bytes.bytes.clone())
                } else {
                    Value::Blob(bytes.bytes.clone())
                }
            } else if let Ok(geo) = ud.borrow::<LuaGeoJSON>() {
                Value::GeoJSON(geo.0.clone())
            } else {
                return Err(mlua::Error::runtime(
                    "unsupported userdata in stream result",
                ));
            }
        }
        other => {
            return Err(mlua::Error::runtime(format!(
                "unsupported Lua type in stream result: {}",
                other.type_name()
            )))
        }
    })
}

fn table_to_value(table: &Table) -> mlua::Result<Value> {
    let len = table.raw_len();
    let mut entries: Vec<(LuaValue, LuaValue)> = Vec::new();
    for pair in table.pairs::<LuaValue, LuaValue>() {
        entries.push(pair?);
    }

    let is_sequence = !entries.is_empty()
        && entries.len() == len
        && entries
            .iter()
            .all(|(k, _)| matches!(k, LuaValue::Integer(i) if *i >= 1 && *i <= len as i64));

    if is_sequence {
        let mut items = vec![Value::Nil; len];
        for (k, v) in &entries {
            if let LuaValue::Integer(i) = k {
                items[(*i - 1) as usize] = lua_to_value(v)?;
            }
        }
        Ok(Value::List(items))
    } else {
        let mut map = HashMap::with_capacity(entries.len());
        for (k, v) in &entries {
            map.insert(lua_to_value(k)?, lua_to_value(v)?);
        }
        Ok(Value::HashMap(map))
    }
}

fn display_value(v: &Value) -> String {
    format!("{v}")
}

impl UserData for LuaList {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // 1-based element access, like the server's Lua list type.
        methods.add_meta_method(MetaMethod::Index, |lua, this, index: LuaValue| {
            if let LuaValue::Integer(i) = index {
                if i >= 1 && (i as usize) <= this.0.len() {
                    return value_to_lua(lua, this.0[(i - 1) as usize].clone());
                }
            }
            Ok(LuaValue::Nil)
        });
        methods.add_meta_method_mut(
            MetaMethod::NewIndex,
            |_, this, (index, value): (i64, LuaValue)| {
                let v = lua_to_value(&value)?;
                let len = this.0.len() as i64;
                if index >= 1 && index <= len {
                    this.0[(index - 1) as usize] = v;
                    Ok(())
                } else if index == len + 1 {
                    this.0.push(v);
                    Ok(())
                } else {
                    Err(mlua::Error::runtime(format!(
                        "list index {index} out of bounds (len {len})"
                    )))
                }
            },
        );
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| Ok(this.0.len() as i64));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            let items: Vec<String> = this.0.iter().map(display_value).collect();
            Ok(format!("[{}]", items.join(", ")))
        });
    }
}

impl UserData for LuaMap {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |lua, this, key: LuaValue| {
            let key = lua_to_value(&key)?;
            this.0
                .get(&key)
                .map_or_else(|| Ok(LuaValue::Nil), |v| value_to_lua(lua, v.clone()))
        });
        methods.add_meta_method_mut(
            MetaMethod::NewIndex,
            |_, this, (key, value): (LuaValue, LuaValue)| {
                let key = lua_to_value(&key)?;
                if value == LuaValue::Nil {
                    this.0.remove(&key);
                } else {
                    this.0.insert(key, lua_to_value(&value)?);
                }
                Ok(())
            },
        );
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| Ok(this.0.len() as i64));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| {
            let items: Vec<String> = this
                .0
                .iter()
                .map(|(k, v)| format!("{}={}", display_value(k), display_value(v)))
                .collect();
            Ok(format!("{{{}}}", items.join(", ")))
        });
    }
}

/// Build a `LuaList` from an optional Lua table initializer (sequence part).
fn list_from_initializer(init: Option<Table>) -> mlua::Result<LuaList> {
    let mut items = Vec::new();
    if let Some(t) = init {
        for v in t.sequence_values::<LuaValue>() {
            items.push(lua_to_value(&v?)?);
        }
    }
    Ok(LuaList(items))
}

/// Build a `LuaMap` from an optional Lua table initializer (key/value pairs).
fn map_from_initializer(init: Option<Table>) -> mlua::Result<LuaMap> {
    let mut map = HashMap::new();
    if let Some(t) = init {
        for pair in t.pairs::<LuaValue, LuaValue>() {
            let (k, v) = pair?;
            map.insert(lua_to_value(&k)?, lua_to_value(&v)?);
        }
    }
    Ok(LuaMap(map))
}

/// Register the `list` and `map` global libraries.
#[allow(clippy::too_many_lines)]
pub fn register(lua: &Lua) -> mlua::Result<()> {
    register_list(lua)?;
    register_map(lua)
}

fn register_list(lua: &Lua) -> mlua::Result<()> {
    let list = lua.create_table()?;

    // `list()` / `list{1, 2, 3}` constructor.
    let mt = lua.create_table()?;
    mt.set(
        "__call",
        lua.create_function(|_, (_this, init): (Table, Option<Table>)| {
            list_from_initializer(init)
        })?,
    )?;
    list.set_metatable(Some(mt))?;

    list.set(
        "create",
        lua.create_function(|_, capacity: Option<usize>| {
            Ok(LuaList(Vec::with_capacity(capacity.unwrap_or(0))))
        })?,
    )?;
    list.set(
        "size",
        lua.create_function(|_, ud: mlua::AnyUserData| Ok(ud.borrow::<LuaList>()?.0.len() as i64))?,
    )?;
    list.set(
        "is_list",
        lua.create_function(|_, v: LuaValue| {
            Ok(matches!(&v, LuaValue::UserData(ud) if ud.is::<LuaList>()))
        })?,
    )?;
    list.set(
        "append",
        lua.create_function(|_, (ud, v): (mlua::AnyUserData, LuaValue)| {
            ud.borrow_mut::<LuaList>()?.0.push(lua_to_value(&v)?);
            Ok(())
        })?,
    )?;
    list.set(
        "prepend",
        lua.create_function(|_, (ud, v): (mlua::AnyUserData, LuaValue)| {
            ud.borrow_mut::<LuaList>()?.0.insert(0, lua_to_value(&v)?);
            Ok(())
        })?,
    )?;
    list.set(
        "insert",
        lua.create_function(|_, (ud, index, v): (mlua::AnyUserData, i64, LuaValue)| {
            let mut l = ud.borrow_mut::<LuaList>()?;
            let len = l.0.len() as i64;
            if index >= 1 && index <= len + 1 {
                l.0.insert((index - 1) as usize, lua_to_value(&v)?);
                Ok(())
            } else {
                Err(mlua::Error::runtime(format!(
                    "list index {index} out of bounds (len {len})"
                )))
            }
        })?,
    )?;
    list.set(
        "remove",
        lua.create_function(|_, (ud, index): (mlua::AnyUserData, i64)| {
            let mut l = ud.borrow_mut::<LuaList>()?;
            let len = l.0.len() as i64;
            if index >= 1 && index <= len {
                l.0.remove((index - 1) as usize);
                Ok(())
            } else {
                Err(mlua::Error::runtime(format!(
                    "list index {index} out of bounds (len {len})"
                )))
            }
        })?,
    )?;
    // First `n` elements.
    list.set(
        "take",
        lua.create_function(|_, (ud, n): (mlua::AnyUserData, usize)| {
            let l = ud.borrow::<LuaList>()?;
            Ok(LuaList(l.0.iter().take(n).cloned().collect()))
        })?,
    )?;
    // All but the first `n` elements.
    list.set(
        "drop",
        lua.create_function(|_, (ud, n): (mlua::AnyUserData, usize)| {
            let l = ud.borrow::<LuaList>()?;
            Ok(LuaList(l.0.iter().skip(n).cloned().collect()))
        })?,
    )?;
    // Remove all elements at and beyond the (1-based) index.
    list.set(
        "trim",
        lua.create_function(|_, (ud, index): (mlua::AnyUserData, i64)| {
            let mut l = ud.borrow_mut::<LuaList>()?;
            let keep = (index - 1).max(0) as usize;
            l.0.truncate(keep);
            Ok(())
        })?,
    )?;
    list.set(
        "clone",
        lua.create_function(|_, ud: mlua::AnyUserData| {
            Ok(LuaList(ud.borrow::<LuaList>()?.0.clone()))
        })?,
    )?;
    // Append the contents of `l2` onto `l1` (in place).
    list.set(
        "concat",
        lua.create_function(|_, (ud1, ud2): (mlua::AnyUserData, mlua::AnyUserData)| {
            let other = ud2.borrow::<LuaList>()?.0.clone();
            ud1.borrow_mut::<LuaList>()?.0.extend(other);
            Ok(())
        })?,
    )?;
    // New list with the contents of both.
    list.set(
        "merge",
        lua.create_function(|_, (ud1, ud2): (mlua::AnyUserData, mlua::AnyUserData)| {
            let mut items = ud1.borrow::<LuaList>()?.0.clone();
            items.extend(ud2.borrow::<LuaList>()?.0.iter().cloned());
            Ok(LuaList(items))
        })?,
    )?;
    list.set(
        "iterator",
        lua.create_function(|lua, ud: mlua::AnyUserData| {
            let items = ud.borrow::<LuaList>()?.0.clone();
            let iter = RefCell::new(items.into_iter());
            lua.create_function(move |lua, ()| {
                iter.borrow_mut()
                    .next()
                    .map_or_else(|| Ok(LuaValue::Nil), |v| value_to_lua(lua, v))
            })
        })?,
    )?;

    lua.globals().set("list", list)
}

fn register_map(lua: &Lua) -> mlua::Result<()> {
    let map = lua.create_table()?;

    // `map()` / `map{k = v}` constructor.
    let mt = lua.create_table()?;
    mt.set(
        "__call",
        lua.create_function(|_, (_this, init): (Table, Option<Table>)| map_from_initializer(init))?,
    )?;
    map.set_metatable(Some(mt))?;

    map.set(
        "create",
        lua.create_function(|_, capacity: Option<usize>| {
            Ok(LuaMap(HashMap::with_capacity(capacity.unwrap_or(0))))
        })?,
    )?;
    map.set(
        "size",
        lua.create_function(|_, ud: mlua::AnyUserData| Ok(ud.borrow::<LuaMap>()?.0.len() as i64))?,
    )?;
    map.set(
        "is_map",
        lua.create_function(|_, v: LuaValue| {
            Ok(matches!(&v, LuaValue::UserData(ud) if ud.is::<LuaMap>()))
        })?,
    )?;
    map.set(
        "remove",
        lua.create_function(|_, (ud, key): (mlua::AnyUserData, LuaValue)| {
            ud.borrow_mut::<LuaMap>()?.0.remove(&lua_to_value(&key)?);
            Ok(())
        })?,
    )?;
    map.set(
        "clone",
        lua.create_function(|_, ud: mlua::AnyUserData| {
            Ok(LuaMap(ud.borrow::<LuaMap>()?.0.clone()))
        })?,
    )?;
    // `for k, v in map.pairs(m) do ... end`
    map.set(
        "pairs",
        lua.create_function(|lua, ud: mlua::AnyUserData| {
            let iter = RefCell::new(ud.borrow::<LuaMap>()?.0.clone().into_iter());
            lua.create_function(move |lua, ()| match iter.borrow_mut().next() {
                Some((k, v)) => Ok(MultiValue::from_vec(vec![
                    value_to_lua(lua, k)?,
                    value_to_lua(lua, v)?,
                ])),
                None => Ok(MultiValue::new()),
            })
        })?,
    )?;
    map.set(
        "keys",
        lua.create_function(|lua, ud: mlua::AnyUserData| {
            let iter = RefCell::new(ud.borrow::<LuaMap>()?.0.clone().into_keys());
            lua.create_function(move |lua, ()| {
                iter.borrow_mut()
                    .next()
                    .map_or_else(|| Ok(LuaValue::Nil), |k| value_to_lua(lua, k))
            })
        })?,
    )?;
    map.set(
        "values",
        lua.create_function(|lua, ud: mlua::AnyUserData| {
            let iter = RefCell::new(ud.borrow::<LuaMap>()?.0.clone().into_values());
            lua.create_function(move |lua, ()| {
                iter.borrow_mut()
                    .next()
                    .map_or_else(|| Ok(LuaValue::Nil), |v| value_to_lua(lua, v))
            })
        })?,
    )?;
    // `map.merge(m1, m2 [, f])`: union of both maps; when a key exists in
    // both, the merge function `f(v1, v2)` decides the value (m2 wins when
    // no function is given).
    map.set(
        "merge",
        lua.create_function(
            |lua, (ud1, ud2, f): (mlua::AnyUserData, mlua::AnyUserData, Option<mlua::Function>)| {
                let mut merged = ud1.borrow::<LuaMap>()?.0.clone();
                let other = ud2.borrow::<LuaMap>()?.0.clone();
                for (k, v2) in other {
                    let value = match (merged.get(&k), &f) {
                        (Some(v1), Some(f)) => {
                            let result: LuaValue = f.call((
                                value_to_lua(lua, v1.clone())?,
                                value_to_lua(lua, v2.clone())?,
                            ))?;
                            lua_to_value(&result)?
                        }
                        _ => v2,
                    };
                    merged.insert(k, value);
                }
                Ok(LuaMap(merged))
            },
        )?,
    )?;
    // Keys whose values differ between the two maps (value taken from m1
    // when present, else from m2).
    map.set(
        "diff",
        lua.create_function(|_, (ud1, ud2): (mlua::AnyUserData, mlua::AnyUserData)| {
            let m1 = &ud1.borrow::<LuaMap>()?.0;
            let m2 = &ud2.borrow::<LuaMap>()?.0;
            let mut out = HashMap::new();
            for (k, v) in m1 {
                if m2.get(k) != Some(v) {
                    out.insert(k.clone(), v.clone());
                }
            }
            for (k, v) in m2 {
                if !m1.contains_key(k) {
                    out.insert(k.clone(), v.clone());
                }
            }
            Ok(LuaMap(out))
        })?,
    )?;

    lua.globals().set("map", map)
}
