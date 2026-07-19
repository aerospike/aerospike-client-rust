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

//! The `stream` and `aerospike` Lua globals.
//!
//! A stream is a Lua userdata wrapping one half of a channel: the input
//! stream (`istream`) receives the per-record partial results parsed from
//! the server nodes, the output stream (`ostream`) delivers the final
//! aggregated values back to the [`crate::query::ResultSet`]. `stream.read`
//! awaits the channel and returns `nil` once it is closed and drained,
//! which the `stream_iterator` in `stream_ops.lua` interprets as
//! end-of-stream — the same contract the Java and Go clients use.
//!
//! `read` and `write` are registered through mlua's async API: when Lua
//! calls them, the interpreter coroutine suspends and the whole pipeline
//! future yields back to the executor, so an aggregation waiting on
//! server data or on a slow consumer costs no OS thread.

use async_channel::{Receiver, Sender};
use mlua::{Lua, MetaMethod, UserData, UserDataMethods, Value as LuaValue};

use super::values::{lua_to_value, value_to_lua};
use crate::Value;

/// Input stream: the Lua side reads values fed by the query record pump.
pub struct LuaInputStream(pub(crate) Receiver<Value>);

/// Output stream: the Lua side writes final values destined for the
/// `ResultSet`.
pub struct LuaOutputStream(pub(crate) Sender<Value>);

impl UserData for LuaInputStream {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("LuaInputStream"));
    }
}

impl UserData for LuaOutputStream {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::ToString, |_, _, ()| Ok("LuaOutputStream"));
    }
}

/// Register the `stream` and `aerospike` globals.
pub fn register(lua: &Lua) -> mlua::Result<()> {
    let stream = lua.create_table()?;

    stream.set(
        "read",
        lua.create_async_function(|lua, ud: mlua::AnyUserData| async move {
            // Clone the channel handle so no userdata borrow is held
            // across the await point.
            let input = ud.borrow::<LuaInputStream>()?.0.clone();
            // A closed and drained channel means end of stream -> nil.
            input
                .recv()
                .await
                .map_or_else(|_| Ok(LuaValue::Nil), |v| value_to_lua(&lua, v))
        })?,
    )?;
    stream.set(
        "write",
        lua.create_async_function(|_, (ud, v): (mlua::AnyUserData, LuaValue)| async move {
            let output = ud.borrow::<LuaOutputStream>()?.0.clone();
            let value = lua_to_value(&v)?;
            output.send(value).await.map_err(|_| {
                // The ResultSet consumer went away; abort the pipeline.
                mlua::Error::runtime("output stream closed")
            })
        })?,
    )?;
    stream.set(
        "readable",
        lua.create_function(|_, ud: mlua::AnyUserData| Ok(ud.is::<LuaInputStream>()))?,
    )?;
    stream.set(
        "writeable",
        lua.create_function(|_, ud: mlua::AnyUserData| Ok(ud.is::<LuaOutputStream>()))?,
    )?;

    lua.globals().set("stream", stream)?;

    // `aerospike:log(level, message)` — used by the trace/debug/info/warn
    // helpers in aerospike.lua. Levels follow the server convention:
    // 1=warn, 2=info, 3=debug, 4=trace.
    let aerospike = lua.create_table()?;
    aerospike.set(
        "log",
        lua.create_function(|_, (_this, level, message): (mlua::Table, i64, String)| {
            match level {
                1 => log::warn!(target: "aerospike_lua", "{message}"),
                2 => log::info!(target: "aerospike_lua", "{message}"),
                3 => log::debug!(target: "aerospike_lua", "{message}"),
                _ => log::trace!(target: "aerospike_lua", "{message}"),
            }
            Ok(())
        })?,
    )?;
    lua.globals().set("aerospike", aerospike)
}
