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
2. **A write is staged.** Setting a bin changes nothing on the server until the function saves the record.
3. **A failure is a message.** A function that fails hands the client an error with the text it chose.

## Setting up

A function that works on its record takes the record as its first parameter, before the arguments a
client passes. It is not one of them: the client never sends it. Everything the function does to the
record is a method on it, and a bin is named in each call. C has no objects, so there the record's
functions share a prefix, `as_udf_rec_`, and the record is implied. Rust functions sit together inside one
`export! { ... }` block.

Rust and C++ say in the parameter's type whether a function writes: `&mut Record` or `udf::Record&` for
one that does, `&Record` or `const udf::Record&` for one that only reads. Go and Java have one `Record`
type.

{{snippet setup}}

## Step 1: count a visit

The task: read an integer that may not exist yet, add to it, write it back, and return the new value.
This is the read-modify-write most UDFs are.

The read needs a default, because a new account has no `visits` bin. The write is staged, so the function
must save. The save creates the record when the key has none, in every language.

The defaults differ in one detail. In Go and C++ a bin of another kind also answers the default. In Java
it throws, and the Rust code here fails with `WrongType`.

{{snippet visit}}

A client calls it as `module.visit(1)`. The argument arrives already checked against the function's
signature, except in C, which decodes its arguments by hand and declares the signature as text.

## Step 2: withdraw, or refuse

The task: a bin that must exist and must be an integer, and a business rule that fails the call.

The strict read has no default: an absent bin or a bin of another kind ends the call with an error that
names the problem. The rule is ordinary code. Failing is returning an error in Rust, Go and C++, a status
in C, and throwing in Java. Nothing was saved, so the record is untouched.

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
record goes into the map as it is, whatever its kind. The metadata is on the record: generation, bin
count and time to live here, and also its size, last update time, set name, key and digest.

{{snippet summary}}

## Step 9: extend the record's life

The task: set the record's time to live.

The TTL is in seconds and belongs to the write: it takes effect when the function saves. Zero asks for
the namespace default and -1 for no expiry, subject to the namespace's rules.

{{snippet renew}}

## Step 10: drop a bin

The task: remove a bin from the record.

Removing a bin is staged like a write, and takes effect with the save. Writing nil to a bin does the
same thing, since a bin cannot hold nil.

{{snippet clear_note}}

## Step 11: delete the record

The task: delete the record the function runs on.

The delete is its own write. It needs no save, and whatever was staged goes with the record. The server
carries it out when the call returns, so inside the call the record still answers that it exists.

{{snippet close}}

## Quick reference

| Task | Rust | Go | C | C++ | Java |
| --- | --- | --- | --- | --- | --- |
| Take the record | `rec: &mut Record` or `&Record`, first | `rec *udf.Record`, first | implied | `udf::Record& rec` or `const udf::Record&`, first | `Record rec`, first |
| Read a bin of any kind | `rec.bin(name)` | `rec.Get(name)` | `as_udf_rec_bin(name, &v)` | `rec.get(name)` | `rec.get(name)` |
| Integer with a default | `rec.int(name)?.unwrap_or(d)` | `rec.Int(name)`, test the bool | test `v.kind` | `rec.int_or(name, d)` | `rec.getLong(name, d)` |
| Required, typed | `rec.bin(name)?` then `.int()?` `.double()?` `.bool()?` `.text()?` | `rec.Bin(name)` then `Int()` `Float64()` `Bool()` `Str()` | test `v.kind`, read `v.payload` | `rec.bin(name)` then `as_int()` `as_double()` `as_bool()` `as_text<N>()` | `rec.getLong(name)` `getDouble` `getBoolean` `getString` |
| Write a bin | `rec.set(name, v)` | `rec.Set(name, v)` | `as_udf_rec_set_int` `as_udf_rec_set_str` ... | `rec.set(name, v)` | `rec.set(name, v)` |
| Save | `rec.save()` | `rec.Save()` | `as_udf_rec_save()` | `rec.save()` | `rec.save()` |
| Fail the call | `Err(Error::Custom(msg))` | return an `error` | `return as_udf_failf(msg)` | `return udf::error(status, msg)` | `throw new UdfException(msg)` |
| Walk a list | `.list()?.iter()` | `range rec.List(name)` | cursor | `for (udf::Value v : rec.list(name))` | `for (Value v : rec.getList(name))` |
| Walk a map | `.map()?.entries()` | `range rec.Map(name)` | cursor, key then value | `for (udf::Entry e : rec.map(name))` | `for (MapValue.Entry e : rec.getMap(name))` |
| Look up a key | `map.get(key)` | `value.Lookup(key)` | `as_udf_coll_get(...)` | `value.get(key)` | `map.get(key)` |
| One element of a list | `list.at(i)` | `value.Index(i)` | `as_udf_list_at(&list, i)` | `value.at(i)` | `list.get(i)` |
| Build a list | `as_udf::list(n)`, `push`, `extend` | `udf.NewList(n)`, `Add`, `AddAll` | `as_udf_list_new`, `as_udf_list_push_*` | `udf::List`, `add`, `add_all` | `Udf.list()`, `add`, `addAll` |
| Build a map | `as_udf::map(n)`, `insert` | `udf.NewMap(n)`, `Put` | `as_udf_map_new`, `as_udf_map_put_*` | `udf::Map`, `put` | `Udf.map()`, `put` |
| Set the TTL | `rec.set_ttl(s)` | `rec.SetTTL(s)` | `as_udf_rec_set_ttl(s)` | `rec.set_ttl(s)` | `rec.setTtl(s)` |
| Remove a bin | `rec.remove(name)` | `rec.Remove(name)` | `as_udf_rec_remove(name, len)` | `rec.remove(name)` | `rec.remove(name)` |
| Delete the record | `rec.delete()` | `rec.Delete()` | `as_udf_rec_delete()` | `rec.delete_record()` | `rec.delete()` |

The code on this page is compiled: it comes from the sources under `book/tutorial/`, one file per
language. The language chapters that follow cover building a module and list every call in full.
