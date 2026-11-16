# Tutorial: working with a record

This tutorial builds a small module one function at a time. Each step is a task you will meet in a real
UDF, shown in all five languages so you can compare them. Read it before the language chapters: they are
the reference, this is the tour.

The record is a bank account:

| Bin | Kind | Holds |
| --- | --- | --- |
| `visits` | integer | how often the account was opened |
| `balance` | integer | the amount in the account |
| `owner` | string | the account holder |
| `history` | list of integers | the payments made |
| `limits` | map, string to integer | a spending limit per kind of payment |
| `note` | string | free text |

Three rules hold in every language, and every step leans on them:

1. **A read borrows.** A value read from the record stays in the record. It is valid until the call ends
   or the bin it came from is written.
2. **A write is staged.** Setting a bin changes nothing on the server until the function commits.
3. **A failure is a message.** A function that fails hands the client an error with the text it chose.

## Setting up

Go, C++ and Java name a bin once and read and write through that handle. Rust and C pass the name with
each call. Rust functions sit together inside one `export! { ... }` block. C has no commit that creates a
missing record, so the tutorial carries a three-line helper.

{{snippet setup}}

## Step 1: count a visit

The task: read an integer that may not exist yet, add to it, write it back, and return the new value.
This is the read-modify-write most UDFs are.

The read needs a default, because a new account has no `visits` bin. The write is staged, so the function
must commit. The commit creates the record when the key has none: Go, C++ and Java do that in one call,
Rust and C choose between create and update.

The defaults differ in one detail. In Go and C++ a bin of another kind also answers the default. In Java
it throws, and the Rust code here fails with `WrongType`.

{{snippet visit}}

A client calls it as `module.visit(1)`. The argument arrives already checked against the function's
signature, except in C, which decodes its arguments by hand and declares the signature as text.

## Step 2: withdraw, or refuse

The task: a bin that must exist and must be an integer, and a business rule that fails the call.

The strict read has no default: an absent bin or a bin of another kind ends the call with an error that
names the problem. The rule is ordinary code. Failing is returning an error in Rust, Go and C++, a status
in C, and throwing in Java. Nothing was committed, so the record is untouched.

{{snippet withdraw}}

## Step 3: rename the owner

The task: take a string argument, compare it with a string bin, and write a new string.

A string's bytes stay in the record until the function asks for them. Go and Java copy them into a
string of their own. Rust and C copy them into a buffer the function supplies, and C++ into a `Text<N>`
that holds up to `N` bytes inline. A string that does not fit is an error, not a truncation.

{{snippet rename}}

## Step 4: add up a list

The task: walk a list bin and sum its elements.

The list is not decoded ahead of the loop. The SDK fetches its elements in batches of 128 and hands each
one over as a value, so a long list costs a few calls into the server, not one per element. Rust, Go, C++
and Java give an iterator. C drives the cursor itself.

{{snippet total}}

## Step 5: look up a key in a map

The task: read one entry of a map bin without walking the map.

The lookup is one call into the server, which searches the map and returns the entry's value. A missing
key is an ordinary outcome, not a failure: `Error::Absent` in Rust, `false` in Go, an empty `Optional` in
C++, `AS_UDF_NOT_FOUND` in C and a status other than `Udf.OK` in Java.

{{snippet limit_for}}

## Step 6: walk a map

The task: visit every entry of a map bin.

A map walks like a list, one entry at a time. Rust, Go, C++ and Java hand over the key and the value
together. A C cursor yields them in turn: key, value, key, value.

{{snippet limits_total}}

## Step 7: append to a list

The task: build a new list from a stored one plus one element, and store it.

A function cannot change a stored list in place. It builds a new one and writes that to the bin. The
builder lives on the server, so the stored elements are copied there and their contents never pass
through the module.

Take what you need from the stored list before you write the bin. After the write, the value read
earlier no longer points at anything.

{{snippet log_payment}}

## Step 8: return a map

The task: answer with a collection, mixing stored values and record metadata.

A map is built the same way as a list, and returning it hands it to the client. A value read from the
record goes into the map as it is, whatever its kind. The metadata calls take no arguments: generation,
bin count and time to live here, and also the record's size, last update time, set name, key and digest.

{{snippet summary}}

## Step 9: extend the record's life

The task: set the record's time to live.

The TTL is in seconds and belongs to the write: it takes effect when the function commits. Zero asks for
the namespace default and -1 for no expiry, subject to the namespace's rules.

{{snippet renew}}

## Step 10: drop a bin

The task: remove a bin from the record.

Writing nil to a bin drops it, and the write commits like any other. The SDKs also have a call that
removes a bin by name. The server on this branch refuses it, so use the nil write.

{{snippet clear_note}}

## Step 11: delete the record

The task: delete the record the function runs on.

The delete is its own write. It needs no commit.

{{snippet close}}

## Quick reference

| Task | Rust | Go | C | C++ | Java |
| --- | --- | --- | --- | --- | --- |
| Read a bin of any kind | `as_udf::bin(name)` | `bin.Get()` | `as_udf_bin(name, &v)` | `BIN.get()` | `bin.get()` |
| Integer with a default | `match` on `Error::Absent` | `bin.IntOr(d)` | test `v.kind` | `BIN.int_or(d)` | `bin.getLong(d)` |
| Required, typed | `.int()?` `.double()?` `.bool()?` `.text()?` | `Int()` `Float64()` `Bool()` `Str()` | test `v.kind`, read `v.payload` | `as_int()` `as_double()` `as_bool()` `as_text<N>()` | `getLong()` `getDouble()` `getBoolean()` `getString()` |
| Write a bin | `as_udf::set(name, v)` | `SetInt` `SetStr` `SetList` ... | `as_udf_set_int` `as_udf_set_str` ... | `BIN.set(v)` | `bin.set(v)` |
| Commit | `update()` or `create()` | `udf.Commit()` | `as_udf_rec_update()` or `as_udf_rec_create()` | `udf::commit()` | `Udf.commit()` |
| Fail the call | `Err(Error::Custom(msg))` | return an `error` | `return as_udf_failf(msg)` | `return udf::error(status, msg)` | `throw new UdfException(msg)` |
| Walk a list | `.list()?.iter()` | `range bin.List()` | cursor | `for (udf::Value v : BIN.list())` | `for (Value v : bin.asList())` |
| Walk a map | `.map()?.entries()` | `range bin.Map()` | cursor, key then value | `for (udf::Entry e : BIN.map())` | `for (Cursor.Entry e : bin.asMap())` |
| Look up a key | `map.get(key)` | `value.Lookup(key)` | `as_udf_coll_get(...)` | `value.get(key)` | `Udf.get(map, key, out)` |
| Build a list | `as_udf::list(n)`, `push`, `extend` | `udf.NewList(n)`, `Add`, `AddAll` | `as_udf_list_new`, `as_udf_list_push_*` | `udf::List`, `add`, `add_all` | `UdfList.create(n)`, `add`, `addAll` |
| Build a map | `as_udf::map(n)`, `put` | `udf.NewMap(n)`, `Put` | `as_udf_map_new`, `as_udf_map_put_*` | `udf::Map`, `put` | `UdfMap.create(n)`, `put` |
| Set the TTL | `set_time_to_live(s)` | `udf.SetTTL(s)` | `as_udf_set_ttl(s)` | `udf::set_ttl(s)` | `Udf.setTtl(s)` |
| Drop a bin | `set(name, Value::Nil)` | `bin.SetNull()` | `as_udf_set_nil(name, len)` | `BIN.set_null()` | `bin.setNull()` |
| Delete the record | `remove_record()` | `udf.Delete()` | `as_udf_rec_remove()` | `udf::remove_record()` | `Udf.delete()` |

The code on this page is compiled: it comes from the sources under `book/tutorial/`, one file per
language. The language chapters that follow cover building a module and list every call in full.
