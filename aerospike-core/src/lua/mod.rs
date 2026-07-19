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

//! Client-side Lua runtime for stream UDF aggregation (`lua` feature).
//!
//! Aerospike aggregation queries split a stream UDF's operation chain in
//! two: the server nodes run the first part (`map`/`filter`/`aggregate`)
//! and return one partial result per node stream, and the client runs the
//! remainder (from the first `reduce` onward) to combine those partials
//! into the final result. That client-side portion executes in an embedded
//! Lua 5.4 interpreter (via [mlua](https://docs.rs/mlua)), driven by
//! [`Client::query_aggregate`](crate::Client::query_aggregate).
//!
//! For that to work, the client needs the *same* UDF source that was
//! registered on the server. Point the client at your local `.lua` files
//! with [`set_lua_path`] (analogous to `LuaConfig.SourceDirectory` in the
//! Java client and `SetLuaPath` in the Go client), or register the source
//! in memory with [`register_package`]:
//!
//! ```rust,no_run
//! aerospike::lua::set_lua_path("udf/");
//! // or, without touching the filesystem:
//! aerospike::lua::register_package(
//!     "example",
//!     r#"function sum(s, name)
//!            local function m(rec) return rec[name] end
//!            local function r(a, b) return a + b end
//!            return s : map(m) : reduce(r)
//!        end"#,
//! );
//! ```

mod bytes;
mod stream;
mod values;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, RwLock};

use async_channel::{Receiver, Sender};
use mlua::Lua;

pub(crate) use stream::{LuaInputStream, LuaOutputStream};

use crate::errors::{Error, Result};
use crate::Value;

/// The system packages bootstrapped into every interpreter instance.
const STREAM_OPS_SOURCE: &str = include_str!("resources/stream_ops.lua");
const AEROSPIKE_SOURCE: &str = include_str!("resources/aerospike.lua");

/// Directory containing the client-side copies of the UDF packages.
static SOURCE_DIR: LazyLock<RwLock<PathBuf>> =
    LazyLock::new(|| RwLock::new(PathBuf::from("udf")));

/// In-memory UDF package sources registered via [`register_package`].
/// Looked up before the filesystem.
static PACKAGES: LazyLock<RwLock<HashMap<String, String>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Set the directory where the client looks for `<package_name>.lua` files
/// during aggregation queries.
///
/// Defaults to `"udf"`. Process-wide, like the Java client's
/// `LuaConfig.SourceDirectory` and the Go client's `SetLuaPath`.
///
/// # Panics
/// Panics if the internal configuration lock is poisoned.
pub fn set_lua_path<P: AsRef<Path>>(dir: P) {
    *SOURCE_DIR.write().expect("lua path lock poisoned") = dir.as_ref().to_path_buf();
}

/// The directory where the client looks for UDF packages.
///
/// # Panics
/// Panics if the internal configuration lock is poisoned.
#[must_use]
pub fn lua_path() -> PathBuf {
    SOURCE_DIR.read().expect("lua path lock poisoned").clone()
}

/// Register a UDF package source in memory under `package_name`.
///
/// Takes precedence over a `<package_name>.lua` file in the [`lua_path`]
/// directory. Useful when the UDF source is embedded in the application
/// rather than shipped as a file.
///
/// # Panics
/// Panics if the internal configuration lock is poisoned.
pub fn register_package(package_name: &str, source: &str) {
    PACKAGES
        .write()
        .expect("lua package lock poisoned")
        .insert(package_name.to_owned(), source.to_owned());
}

/// Remove a package registered with [`register_package`].
///
/// # Panics
/// Panics if the internal configuration lock is poisoned.
pub fn unregister_package(package_name: &str) {
    PACKAGES
        .write()
        .expect("lua package lock poisoned")
        .remove(package_name);
}

fn lua_error(context: &str, err: &mlua::Error) -> Error {
    Error::client_error(format!("{context}: {err}"))
}

/// Create a fresh interpreter with the Aerospike globals (`list`, `map`,
/// `bytes`, `stream`, `aerospike`) registered and the system packages
/// loaded.
fn new_instance() -> Result<Lua> {
    let lua = Lua::new();
    values::register(&lua).map_err(|e| lua_error("failed to register lua globals", &e))?;
    bytes::register(&lua).map_err(|e| lua_error("failed to register lua globals", &e))?;
    stream::register(&lua).map_err(|e| lua_error("failed to register lua globals", &e))?;
    lua.load(STREAM_OPS_SOURCE)
        .set_name("@stream_ops.lua")
        .exec()
        .map_err(|e| lua_error("failed to load stream_ops.lua", &e))?;
    lua.load(AEROSPIKE_SOURCE)
        .set_name("@aerospike.lua")
        .exec()
        .map_err(|e| lua_error("failed to load aerospike.lua", &e))?;
    Ok(lua)
}

/// Load the user's UDF package into the interpreter: from the in-memory
/// registry if present, otherwise from `<lua_path>/<package_name>.lua`.
async fn load_user_package(lua: &Lua, package_name: &str) -> Result<()> {
    let registered = PACKAGES
        .read()
        .expect("lua package lock poisoned")
        .get(package_name)
        .cloned();

    let source = if let Some(source) = registered {
        source
    } else {
        let path = lua_path().join(format!("{package_name}.lua"));
        aerospike_rt::fs::read_to_string(&path).await.map_err(|e| {
            Error::invalid_argument(format!(
                "cannot read client-side UDF package '{}' from {}: {e}; \
                 set the source directory with aerospike::lua::set_lua_path() \
                 or register the source with aerospike::lua::register_package()",
                package_name,
                path.display()
            ))
        })?
    };

    lua.load(&source)
        .set_name(format!("@{package_name}.lua"))
        .exec()
        .map_err(|e| lua_error(&format!("failed to load UDF package '{package_name}'"), &e))
}

/// Scope value passed to `apply_stream`: run the client-side portion of the
/// stream operations (see `StreamOps_select` in `stream_ops.lua`).
const SCOPE_CLIENT: i64 = 2;

/// Run the client half of an aggregation: read per-node partial results
/// from `input`, apply the client-scope stream operations of
/// `package_name.function_name`, and write final values to `output`.
///
/// Fully async: `stream.read`/`stream.write` inside the interpreter are
/// await points (mlua async API), so this runs as an ordinary task on the
/// client's executor — no dedicated OS thread per aggregation. The output
/// sender is dropped when this returns, closing the result stream.
pub(crate) async fn run_aggregate_pipeline(
    package_name: &str,
    function_name: &str,
    function_args: Vec<Value>,
    input: Receiver<Value>,
    output: Sender<Value>,
) -> Result<()> {
    let lua = new_instance()?;
    load_user_package(&lua, package_name).await?;

    let function: mlua::Function = lua.globals().get(function_name).map_err(|_| {
        Error::invalid_argument(format!(
            "aggregation function '{function_name}' not found in package '{package_name}'"
        ))
    })?;
    let apply_stream: mlua::Function = lua
        .globals()
        .get("apply_stream")
        .map_err(|e| lua_error("apply_stream missing", &e))?;

    let istream = lua
        .create_userdata(LuaInputStream(input))
        .map_err(|e| lua_error("failed to create input stream", &e))?;
    let ostream = lua
        .create_userdata(LuaOutputStream(output))
        .map_err(|e| lua_error("failed to create output stream", &e))?;

    let mut args: Vec<mlua::Value> = vec![
        mlua::Value::Function(function),
        mlua::Value::Integer(SCOPE_CLIENT),
        mlua::Value::UserData(istream),
        mlua::Value::UserData(ostream),
    ];
    for arg in function_args {
        args.push(
            values::value_to_lua(&lua, arg)
                .map_err(|e| lua_error("failed to convert UDF argument", &e))?,
        );
    }

    apply_stream
        .call_async::<()>(mlua::MultiValue::from_vec(args))
        .await
        .map_err(|e| lua_error("stream UDF aggregation failed", &e))?;
    Ok(())
}

#[cfg(test)]
mod tests;
