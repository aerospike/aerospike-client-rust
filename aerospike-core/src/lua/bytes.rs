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

//! The `bytes` Lua global — the byte-array userdata type of the Aerospike
//! UDF API, ported from the Java client's `LuaBytes`/`LuaBytesLib`.
//!
//! Offsets are 1-based on the Lua side (converted to 0-based internally),
//! reads out of range return `0`/empty values, and every mutator returns a
//! boolean success flag instead of raising — matching the Java client and
//! the server's `bytes` module. Buffers grow automatically on writes past
//! the end (zero-filled).
//!
//! Two deliberate deviations from the Java implementation (both look like
//! bugs there): `get_byte` returns the byte unsigned (0-255) instead of
//! sign-extended, and `append_byte`/`append_int16*` append what their
//! names say (Java's opcode table crossed them).

use mlua::{Lua, MetaMethod, MultiValue, Table, UserData, UserDataMethods, Value as LuaValue};

use crate::commands::ParticleType;

/// Userdata wrapper for an Aerospike byte array inside the Lua interpreter.
pub struct LuaBytes {
    pub(crate) bytes: Vec<u8>,
    /// Wire particle type carried alongside the payload
    /// ([`ParticleType`] as an integer; `BLOB` by default, `HLL` when the
    /// value came from an HLL bin).
    pub(crate) particle_type: u8,
}

impl LuaBytes {
    pub(crate) const fn new(bytes: Vec<u8>, particle_type: ParticleType) -> Self {
        LuaBytes {
            bytes,
            particle_type: particle_type as u8,
        }
    }

    fn hex(&self) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(self.bytes.len() * 2);
        for b in &self.bytes {
            let _ = write!(out, "{b:02X}");
        }
        out
    }

    /// Grow (zero-filled) so that `end_offset` bytes are addressable.
    fn ensure(&mut self, end_offset: usize) {
        if end_offset > self.bytes.len() {
            self.bytes.resize(end_offset, 0);
        }
    }

    fn get_byte(&self, offset: usize) -> u8 {
        self.bytes.get(offset).copied().unwrap_or(0)
    }

    fn set_byte(&mut self, offset: usize, value: u8) {
        self.ensure(offset + 1);
        self.bytes[offset] = value;
    }

    fn read<const N: usize>(&self, offset: usize) -> Option<[u8; N]> {
        self.bytes
            .get(offset..offset + N)
            .map(|s| s.try_into().expect("slice length"))
    }

    fn write(&mut self, offset: usize, data: &[u8]) {
        self.ensure(offset + data.len());
        self.bytes[offset..offset + data.len()].copy_from_slice(data);
    }

    /// Decode a 7-bit variable-length unsigned int (high bit = continue).
    /// Returns `(value, bytes_consumed)`; `(0, 0)` when out of range or
    /// truncated.
    fn get_var_int(&self, offset: usize) -> (i64, i64) {
        let mut value: u32 = 0;
        let mut shift = 0u32;
        let mut i = offset;
        loop {
            let Some(&b) = self.bytes.get(i) else {
                return (0, 0);
            };
            i += 1;
            value |= u32::from(b & 0x7F) << shift;
            shift += 7;
            if b & 0x80 == 0 {
                break;
            }
        }
        (i64::from(value), (i - offset) as i64)
    }

    /// Encode `value` in the 7-bit variable-length format at `offset`.
    /// Returns the number of bytes written.
    fn set_var_int(&mut self, offset: usize, value: i64) -> i64 {
        #[allow(clippy::cast_sign_loss)]
        let mut v = value as u32;
        let mut encoded = Vec::with_capacity(5);
        #[allow(clippy::cast_possible_truncation)]
        while v >= 0x80 {
            encoded.push((v as u8) | 0x80);
            v >>= 7;
        }
        #[allow(clippy::cast_possible_truncation)]
        encoded.push(v as u8);
        self.write(offset, &encoded);
        encoded.len() as i64
    }
}

impl UserData for LuaBytes {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // 1-based byte access, like the server's Lua bytes type.
        methods.add_meta_method(MetaMethod::Index, |_, this, index: i64| {
            if index >= 1 {
                Ok(i64::from(this.get_byte((index - 1) as usize)))
            } else {
                Ok(0)
            }
        });
        methods.add_meta_method_mut(MetaMethod::NewIndex, |_, this, (index, value): (i64, i64)| {
            if index >= 1 {
                #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                this.set_byte((index - 1) as usize, value as u8);
            }
            Ok(())
        });
        methods.add_meta_method(MetaMethod::Len, |_, this, ()| Ok(this.bytes.len() as i64));
        methods.add_meta_method(MetaMethod::ToString, |_, this, ()| Ok(this.hex()));
    }
}

/// Convert a 1-based Lua offset to 0-based, rejecting values < 1.
fn offset0(lua_offset: i64) -> Option<usize> {
    usize::try_from(lua_offset - 1).ok()
}

/// Register the `bytes` global library.
#[allow(clippy::too_many_lines)]
pub fn register(lua: &Lua) -> mlua::Result<()> {
    let bytes = lua.create_table()?;

    // `bytes()` / `bytes(capacity)` constructor. The capacity is a
    // pre-allocation hint only: the new byte array has size 0.
    let mt = lua.create_table()?;
    mt.set(
        "__call",
        lua.create_function(|_, (_this, capacity): (Table, Option<usize>)| {
            Ok(LuaBytes {
                bytes: Vec::with_capacity(capacity.unwrap_or(0)),
                particle_type: ParticleType::BLOB as u8,
            })
        })?,
    )?;
    bytes.set_metatable(Some(mt))?;

    let get = |lua: &Lua,
               name: &str,
               f: fn(&LuaBytes, usize) -> i64|
     -> mlua::Result<(String, mlua::Function)> {
        let name = name.to_owned();
        let func = lua.create_function(move |_, (ud, offset): (mlua::AnyUserData, i64)| {
            let b = ud.borrow::<LuaBytes>()?;
            Ok(offset0(offset).map_or(0, |o| f(&b, o)))
        })?;
        Ok((name, func))
    };

    bytes.set(
        "size",
        lua.create_function(|_, ud: mlua::AnyUserData| {
            Ok(ud.borrow::<LuaBytes>()?.bytes.len() as i64)
        })?,
    )?;
    bytes.set(
        "get_type",
        lua.create_function(|_, ud: mlua::AnyUserData| {
            Ok(i64::from(ud.borrow::<LuaBytes>()?.particle_type))
        })?,
    )?;
    bytes.set(
        "set_type",
        lua.create_function(|_, (ud, t): (mlua::AnyUserData, i64)| {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            {
                ud.borrow_mut::<LuaBytes>()?.particle_type = t as u8;
            }
            Ok(true)
        })?,
    )?;
    bytes.set(
        "is_bytes",
        lua.create_function(|_, v: LuaValue| {
            Ok(matches!(&v, LuaValue::UserData(ud) if ud.is::<LuaBytes>()))
        })?,
    )?;

    // ---- getters ----
    let (n, f) = get(lua, "get_byte", |b, o| i64::from(b.get_byte(o)))?;
    bytes.set(n, f)?;
    let (n, f) = get(lua, "get_int16", |b, o| {
        b.read::<2>(o).map_or(0, |a| i64::from(i16::from_be_bytes(a)))
    })?;
    bytes.set(n, f.clone())?;
    bytes.set("get_int16_be", f)?;
    let (n, f) = get(lua, "get_int16_le", |b, o| {
        b.read::<2>(o).map_or(0, |a| i64::from(i16::from_le_bytes(a)))
    })?;
    bytes.set(n, f)?;
    let (n, f) = get(lua, "get_int32", |b, o| {
        b.read::<4>(o).map_or(0, |a| i64::from(i32::from_be_bytes(a)))
    })?;
    bytes.set(n, f.clone())?;
    bytes.set("get_int32_be", f)?;
    let (n, f) = get(lua, "get_int32_le", |b, o| {
        b.read::<4>(o).map_or(0, |a| i64::from(i32::from_le_bytes(a)))
    })?;
    bytes.set(n, f)?;
    let (n, f) = get(lua, "get_int64", |b, o| {
        b.read::<8>(o).map_or(0, i64::from_be_bytes)
    })?;
    bytes.set(n, f.clone())?;
    bytes.set("get_int64_be", f)?;
    let (n, f) = get(lua, "get_int64_le", |b, o| {
        b.read::<8>(o).map_or(0, i64::from_le_bytes)
    })?;
    bytes.set(n, f)?;

    bytes.set(
        "get_string",
        lua.create_function(
            |_, (ud, offset, len): (mlua::AnyUserData, i64, usize)| {
                let b = ud.borrow::<LuaBytes>()?;
                let Some(start) = offset0(offset).filter(|o| *o < b.bytes.len()) else {
                    return Ok(String::new());
                };
                let end = (start + len).min(b.bytes.len());
                Ok(String::from_utf8_lossy(&b.bytes[start..end]).into_owned())
            },
        )?,
    )?;
    bytes.set(
        "get_bytes",
        lua.create_function(
            |_, (ud, offset, len): (mlua::AnyUserData, i64, usize)| {
                let b = ud.borrow::<LuaBytes>()?;
                let payload = offset0(offset)
                    .filter(|o| *o < b.bytes.len())
                    .map_or_else(Vec::new, |start| {
                        let end = (start + len).min(b.bytes.len());
                        b.bytes[start..end].to_vec()
                    });
                Ok(LuaBytes::new(payload, ParticleType::BLOB))
            },
        )?,
    )?;
    bytes.set(
        "get_var_int",
        lua.create_function(|_, (ud, offset): (mlua::AnyUserData, i64)| {
            let b = ud.borrow::<LuaBytes>()?;
            let (value, size) = offset0(offset).map_or((0, 0), |o| b.get_var_int(o));
            Ok(MultiValue::from_vec(vec![
                LuaValue::Integer(value),
                LuaValue::Integer(size),
            ]))
        })?,
    )?;

    // ---- setters: return true on success, false on bad arguments ----
    macro_rules! setter {
        ($name:literal, $width:literal, $conv:expr) => {{
            let func =
                lua.create_function(|_, (ud, offset, value): (mlua::AnyUserData, i64, i64)| {
                    let Some(o) = offset0(offset) else {
                        return Ok(false);
                    };
                    let data: [u8; $width] = $conv(value);
                    ud.borrow_mut::<LuaBytes>()?.write(o, &data);
                    Ok(true)
                })?;
            bytes.set($name, func.clone())?;
            func
        }};
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        bytes.set(
            "set_byte",
            lua.create_function(|_, (ud, offset, value): (mlua::AnyUserData, i64, i64)| {
                let Some(o) = offset0(offset) else {
                    return Ok(false);
                };
                ud.borrow_mut::<LuaBytes>()?.set_byte(o, value as u8);
                Ok(true)
            })?,
        )?;

        let f = setter!("set_int16", 2, |v: i64| (v as i16).to_be_bytes());
        bytes.set("set_int16_be", f)?;
        setter!("set_int16_le", 2, |v: i64| (v as i16).to_le_bytes());
        let f = setter!("set_int32", 4, |v: i64| (v as i32).to_be_bytes());
        bytes.set("set_int32_be", f)?;
        setter!("set_int32_le", 4, |v: i64| (v as i32).to_le_bytes());
        let f = setter!("set_int64", 8, |v: i64| v.to_be_bytes());
        bytes.set("set_int64_be", f)?;
        setter!("set_int64_le", 8, |v: i64| v.to_le_bytes());
    }

    bytes.set(
        "set_size",
        lua.create_function(|_, (ud, size): (mlua::AnyUserData, usize)| {
            let mut b = ud.borrow_mut::<LuaBytes>()?;
            if size < b.bytes.len() {
                b.bytes.truncate(size);
            } else {
                let additional = size - b.bytes.len();
                b.bytes.reserve(additional);
            }
            Ok(true)
        })?,
    )?;
    bytes.set(
        "set_string",
        lua.create_function(
            |_, (ud, offset, value): (mlua::AnyUserData, i64, mlua::LuaString)| {
                let Some(o) = offset0(offset) else {
                    return Ok(false);
                };
                ud.borrow_mut::<LuaBytes>()?.write(o, &value.as_bytes());
                Ok(true)
            },
        )?,
    )?;
    bytes.set(
        "set_bytes",
        lua.create_function(
            |_,
             (ud, offset, src, len): (
                mlua::AnyUserData,
                i64,
                mlua::AnyUserData,
                Option<usize>,
            )| {
                let Some(o) = offset0(offset) else {
                    return Ok(false);
                };
                let source = src.borrow::<LuaBytes>()?;
                let len = match len {
                    Some(l) if l > 0 && l <= source.bytes.len() => l,
                    _ => source.bytes.len(),
                };
                let data = source.bytes[..len].to_vec();
                drop(source); // src and ud may be the same userdata
                ud.borrow_mut::<LuaBytes>()?.write(o, &data);
                Ok(true)
            },
        )?,
    )?;
    bytes.set(
        "set_var_int",
        lua.create_function(|_, (ud, offset, value): (mlua::AnyUserData, i64, i64)| {
            let Some(o) = offset0(offset) else {
                return Ok(0);
            };
            Ok(ud.borrow_mut::<LuaBytes>()?.set_var_int(o, value))
        })?,
    )?;

    // ---- appenders ----
    macro_rules! appender {
        ($name:literal, $conv:expr) => {{
            let func = lua.create_function(|_, (ud, value): (mlua::AnyUserData, i64)| {
                let mut b = ud.borrow_mut::<LuaBytes>()?;
                let end = b.bytes.len();
                let data = $conv(value);
                b.write(end, &data);
                Ok(true)
            })?;
            bytes.set($name, func.clone())?;
            func
        }};
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    {
        appender!("append_byte", |v: i64| [v as u8]);
        let f = appender!("append_int16", |v: i64| (v as i16).to_be_bytes());
        bytes.set("append_int16_be", f)?;
        appender!("append_int16_le", |v: i64| (v as i16).to_le_bytes());
        let f = appender!("append_int32", |v: i64| (v as i32).to_be_bytes());
        bytes.set("append_int32_be", f)?;
        appender!("append_int32_le", |v: i64| (v as i32).to_le_bytes());
        let f = appender!("append_int64", |v: i64| v.to_be_bytes());
        bytes.set("append_int64_be", f)?;
        appender!("append_int64_le", |v: i64| v.to_le_bytes());
    }

    bytes.set(
        "append_string",
        lua.create_function(|_, (ud, value): (mlua::AnyUserData, mlua::LuaString)| {
            let mut b = ud.borrow_mut::<LuaBytes>()?;
            let end = b.bytes.len();
            b.write(end, &value.as_bytes());
            Ok(true)
        })?,
    )?;
    bytes.set(
        "append_bytes",
        lua.create_function(
            |_, (ud, src, len): (mlua::AnyUserData, mlua::AnyUserData, Option<usize>)| {
                let source = src.borrow::<LuaBytes>()?;
                let len = match len {
                    Some(l) if l > 0 && l <= source.bytes.len() => l,
                    _ => source.bytes.len(),
                };
                let data = source.bytes[..len].to_vec();
                drop(source);
                let mut b = ud.borrow_mut::<LuaBytes>()?;
                let end = b.bytes.len();
                b.write(end, &data);
                Ok(true)
            },
        )?,
    )?;
    bytes.set(
        "append_var_int",
        lua.create_function(|_, (ud, value): (mlua::AnyUserData, i64)| {
            let mut b = ud.borrow_mut::<LuaBytes>()?;
            let end = b.bytes.len();
            Ok(b.set_var_int(end, value))
        })?,
    )?;

    lua.globals().set("bytes", bytes)
}
