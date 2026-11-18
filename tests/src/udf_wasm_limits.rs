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

//! Resource limits for WASM UDFs: what a module may use of the node, beyond staying in its sandbox
//! (`udf_wasm_sandbox.rs`).
//!
//! The module is assembled in the test by `module()`, so the suite needs no WASM toolchain and every
//! byte is in review. It imports `aerospike.log` and exports, each `(i32, i32) -> i32`:
//!
//! | export                    | what it does                                      |
//! | ------------------------- | ------------------------------------------------- |
//! | `ok`                      | returns                                           |
//! | `spin_record`, `spin_end` | a stream UDF whose per-record call never returns  |
//! | `fin_record`, `fin_end`   | a stream UDF whose final call never returns       |
//! | `grow`                    | grows its memory by 16 MiB, traps if refused      |
//! | `flood`                   | writes 1000 log lines, each with a newline in it  |
//! | `taint`                   | writes a mark at two addresses of its memory      |
//! | `stale`                   | traps if it finds that mark                       |
//! | `taintspin_record`        | writes the mark, then never returns               |
//! | `trap`                    | executes `unreachable`                            |
//!
//! `taint` and `stale` are also exported as stream UDFs (`taint_record`, `stale_record`, each with an
//! `_end` that returns), so the same pair checks memory between record calls and between queries.
//!
//! Two tests run against any server. The others need a node configured for them and skip themselves
//! unless told it is there:
//!
//! | variable                          | the node's `mod-wamr` block has            |
//! | --------------------------------- | ------------------------------------------ |
//! | `AEROSPIKE_WASM_SMALL_LIMITS=1`   | `max-total-memory 8M` and `max-modules 3`   |
//! | `AEROSPIKE_WASM_SLOW_STREAM=1`    | `stream-timeout-ms 120000`                  |

use std::time::{Duration, Instant};

use futures::StreamExt;

use crate::common;

use aerospike::*;

/// The message `flood` logs: a line, a newline, and something dressed as a line of the server's own.
const FLOOD_MESSAGE: &[u8] = b"flood\nNov 17 2026 00:00:00 GMT: WARNING (security): forged line";

fn leb(mut n: u32) -> Vec<u8> {
    let mut out = Vec::new();

    loop {
        let byte = (n & 0x7f) as u8;

        n >>= 7;

        if n == 0 {
            out.push(byte);
            return out;
        }

        out.push(byte | 0x80);
    }
}

/// A length-prefixed run of bytes: a name, a function body, a section's contents.
fn sized(body: &[u8]) -> Vec<u8> {
    let mut out = leb(body.len() as u32);

    out.extend_from_slice(body);
    out
}

fn section(id: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![id];

    out.extend(sized(body));
    out
}

/// The module, with a memory of `initial_pages` 64 KiB pages and no declared maximum.
#[rustfmt::skip]
fn module(initial_pages: u32) -> Vec<u8> {
    // Function 0 is the import; the eight bodies below are functions 1 to 8.
    const OK: u8 = 1;
    const SPIN: u8 = 2;
    const GROW: u8 = 3;
    const FLOOD: u8 = 4;
    const TAINT: u8 = 5;
    const STALE: u8 = 6;
    const TAINTSPIN: u8 = 7;
    const TRAP: u8 = 8;

    let bodies: [&[u8]; 8] = [
        // ok: 0
        &[0x00, 0x41, 0x00, 0x0b],
        // spin: loop { }
        &[0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x41, 0x00, 0x0b],
        // grow: if memory.grow(256) == -1 { unreachable }; 0
        &[0x00, 0x41, 0x80, 0x02, 0x40, 0x00, 0x41, 0x7f, 0x46, 0x04, 0x40, 0x00, 0x0b, 0x41, 0x00, 0x0b],
        // flood: one i32 local; do { log(1, 0, len) } while (++local < 1000); 0
        &[0x01, 0x01, 0x7f,
          0x03, 0x40,
          0x41, 0x01, 0x41, 0x00, 0x41, FLOOD_MESSAGE.len() as u8, 0x10, 0x00,
          0x20, 0x02, 0x41, 0x01, 0x6a, 0x22, 0x02, 0x41, 0xe8, 0x07, 0x49, 0x0d, 0x00,
          0x0b, 0x41, 0x00, 0x0b],
        // taint: mem[1024] = 0x5a; mem[65535] = 0x5a; 0. The first and the last page a one-page memory has
        // anything to do with: its low addresses and its very end.
        &[0x00,
          0x41, 0x80, 0x08, 0x41, 0xda, 0x00, 0x3a, 0x00, 0x00,
          0x41, 0xff, 0xff, 0x03, 0x41, 0xda, 0x00, 0x3a, 0x00, 0x00,
          0x41, 0x00, 0x0b],
        // stale: if (mem[1024] | mem[65535]) != 0 { unreachable }; 0
        &[0x00,
          0x41, 0x80, 0x08, 0x2d, 0x00, 0x00,
          0x41, 0xff, 0xff, 0x03, 0x2d, 0x00, 0x00,
          0x72, 0x04, 0x40, 0x00, 0x0b,
          0x41, 0x00, 0x0b],
        // taintspin: the two stores of taint, then loop { }
        &[0x00,
          0x41, 0x80, 0x08, 0x41, 0xda, 0x00, 0x3a, 0x00, 0x00,
          0x41, 0xff, 0xff, 0x03, 0x41, 0xda, 0x00, 0x3a, 0x00, 0x00,
          0x03, 0x40, 0x0c, 0x00, 0x0b,
          0x41, 0x00, 0x0b],
        // trap: unreachable
        &[0x00, 0x00, 0x0b],
    ];

    let exports: [(&str, u8); 16] = [
        ("ok", OK),
        ("spin_record", SPIN), ("spin_end", OK),
        ("fin_record", OK), ("fin_end", SPIN),
        ("grow", GROW),
        ("flood", FLOOD),
        ("taint", TAINT), ("taint_record", TAINT), ("taint_end", OK),
        ("stale", STALE), ("stale_record", STALE), ("stale_end", OK),
        ("taintspin_record", TAINTSPIN), ("taintspin_end", OK),
        ("trap", TRAP),
    ];

    assert!(FLOOD_MESSAGE.len() < 0x40, "the length is written as a one-byte constant");

    let mut out = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // type 0: (i32, i32) -> i32, a UDF. type 1: (i32, i32, i32) -> (), the log import.
    out.extend(section(1, &[0x02, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, 0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x00]));

    // import: aerospike.log, of type 1
    let mut import = vec![0x01];
    import.extend(sized(b"aerospike"));
    import.extend(sized(b"log"));
    import.extend([0x00, 0x01]);
    out.extend(section(2, &import));

    // function: eight functions of type 0
    out.extend(section(3, &[0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));

    // memory: one memory, a minimum and no maximum
    let mut memory = vec![0x01, 0x00];
    memory.extend(leb(initial_pages));
    out.extend(section(5, &memory));

    // export
    let mut export = leb(exports.len() as u32);
    for (name, function) in exports {
        export.extend(sized(name.as_bytes()));
        export.extend([0x00, function]);
    }
    out.extend(section(7, &export));

    // code
    let mut code = leb(bodies.len() as u32);
    for body in bodies {
        code.extend(sized(body));
    }
    out.extend(section(10, &code));

    // data: the flood message at address 0
    let mut data = vec![0x01, 0x00, 0x41, 0x00, 0x0b];
    data.extend(sized(FLOOD_MESSAGE));
    out.extend(section(11, &data));

    out
}

/// Standard base64 to text; anything that is not base64 ends the input.
fn unbase64(text: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut out = Vec::new();
    let mut bits = 0u32;
    let mut n = 0;

    for byte in text.bytes() {
        let Some(value) = ALPHABET.iter().position(|&c| c == byte) else {
            break;
        };

        bits = (bits << 6) | value as u32;
        n += 6;

        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8);
        }
    }

    String::from_utf8_lossy(&out).into_owned()
}

/// A module whose start function never returns, and one well-behaved export, `ok`.
///
/// The runtime calls a start function while it makes the instance, before there is anything for a
/// deadline to stop. Fifty-seven bytes.
#[rustfmt::skip]
fn start_spins() -> Vec<u8> {
    let mut out = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // type 0: (i32, i32) -> i32. type 1: () -> (), which a start function must be.
    out.extend(section(1, &[0x02, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, 0x60, 0x00, 0x00]));
    out.extend(section(3, &[0x02, 0x00, 0x01]));
    out.extend(section(5, &[0x01, 0x00, 0x01]));

    let mut export = vec![0x01];
    export.extend(sized(b"ok"));
    export.extend([0x00, 0x00]);
    out.extend(section(7, &export));

    // start: function 1
    out.extend(section(8, &[0x01]));

    let mut code = vec![0x02];
    code.extend(sized(&[0x00, 0x41, 0x00, 0x0b]));              // ok: 0
    code.extend(sized(&[0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b])); // loop { }
    out.extend(section(10, &code));

    out
}

/// A module whose exports do not have the type the server calls a UDF with.
///
/// | export   | type                                   |
/// | -------- | -------------------------------------- |
/// | `ok`     | `(i32, i32) -> i32`                    |
/// | `wide`   | `(i32, i32) -> (i64, i64, i64, i64)`   |
/// | `long`   | `(i32, i32) -> i64`                    |
/// | `bare`   | `() -> i32`                            |
/// | `floats` | `(f32, f32) -> i32`                    |
#[rustfmt::skip]
fn wrong_shapes() -> Vec<u8> {
    let mut out = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    out.extend(section(1, &[
        0x05,
        0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
        0x60, 0x02, 0x7f, 0x7f, 0x04, 0x7e, 0x7e, 0x7e, 0x7e,
        0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e,
        0x60, 0x00, 0x01, 0x7f,
        0x60, 0x02, 0x7d, 0x7d, 0x01, 0x7f,
    ]));
    out.extend(section(3, &[0x05, 0x00, 0x01, 0x02, 0x03, 0x04]));
    out.extend(section(5, &[0x01, 0x00, 0x01]));

    let mut export = vec![0x05];
    for (i, name) in ["ok", "wide", "long", "bare", "floats"].iter().enumerate() {
        export.extend(sized(name.as_bytes()));
        export.extend([0x00, i as u8]);
    }
    out.extend(section(7, &export));

    let mut code = vec![0x05];
    code.extend(sized(&[0x00, 0x41, 0x00, 0x0b]));
    // wide: four times i64.const 0x4141414141414141
    let mut wide = vec![0x00];
    for _ in 0..4 {
        wide.extend([0x42, 0xc1, 0x82, 0x85, 0x8a, 0x94, 0xa8, 0xd0, 0xa0, 0xc1, 0x00]);
    }
    wide.push(0x0b);
    code.extend(sized(&wide));
    code.extend(sized(&[0x00, 0x42, 0x00, 0x0b]));
    code.extend(sized(&[0x00, 0x41, 0x00, 0x0b]));
    code.extend(sized(&[0x00, 0x41, 0x00, 0x0b]));
    out.extend(section(10, &code));

    out
}

/// A signed 32-bit constant as LEB128.
fn sleb(mut n: i64) -> Vec<u8> {
    let mut out = Vec::new();

    loop {
        let byte = (n & 0x7f) as u8;

        n >>= 7;

        if (n == 0 && byte & 0x40 == 0) || (n == -1 && byte & 0x40 != 0) {
            out.push(byte);
            return out;
        }

        out.push(byte | 0x80);
    }
}

/// A module that logs through the Java runtime's import, and answers the three callbacks that import
/// makes in a way no Java toolchain would.
///
/// `teavm.logString(str)` asks the module for the string's character array (`teavm_stringData`), the
/// array's length (`teavm_arrayLength`) and where its characters are (`teavm_charArrayData`). The one
/// UDF, `go`, calls it.
///
/// - `wide_data`: `teavm_stringData` is declared `(i32) -> (i64, i64, i64, i64)`.
/// - otherwise: the three are well typed, and the length is two to the thirty-first characters, at the
///   last two bytes of the module's memory.
#[rustfmt::skip]
fn java_helpers(wide_data: bool) -> Vec<u8> {
    let mut out = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // 0: a UDF. 1: the import, (i32) -> (). 2: (i32) -> four i64. 3: (i32) -> i32, a callback.
    out.extend(section(1, &[
        0x04,
        0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
        0x60, 0x01, 0x7f, 0x00,
        0x60, 0x01, 0x7f, 0x04, 0x7e, 0x7e, 0x7e, 0x7e,
        0x60, 0x01, 0x7f, 0x01, 0x7f,
    ]));

    let mut import = vec![0x01];
    import.extend(sized(b"teavm"));
    import.extend(sized(b"logString"));
    import.extend([0x00, 0x01]);
    out.extend(section(2, &import));

    out.extend(section(3, &[0x04, 0x00, if wide_data { 0x02 } else { 0x03 }, 0x03, 0x03]));
    out.extend(section(5, &[0x01, 0x00, 0x01]));

    let mut export = vec![0x04];
    for (i, name) in ["go", "teavm_stringData", "teavm_arrayLength", "teavm_charArrayData"].iter().enumerate() {
        export.extend(sized(name.as_bytes()));
        export.extend([0x00, 1 + i as u8]);
    }
    out.extend(section(7, &export));

    let mut code = vec![0x04];

    // go: logString(8); 0
    code.extend(sized(&[0x00, 0x41, 0x08, 0x10, 0x00, 0x41, 0x00, 0x0b]));

    // teavm_stringData
    let mut data = vec![0x00];
    if wide_data {
        for _ in 0..4 {
            data.extend([0x42, 0xc1, 0x82, 0x85, 0x8a, 0x94, 0xa8, 0xd0, 0xa0, 0xc1, 0x00]);
        }
    } else {
        data.extend([0x41, 0x10]);
    }
    data.push(0x0b);
    code.extend(sized(&data));

    // teavm_arrayLength: 2^31, which doubled is zero bytes to check
    let mut length = vec![0x00, 0x41];
    length.extend(sleb(-(1i64 << 31)));
    length.push(0x0b);
    code.extend(sized(&length));

    // teavm_charArrayData: the last two bytes of the one page
    let mut chars = vec![0x00, 0x41];
    chars.extend(sleb(65534));
    chars.push(0x0b);
    code.extend(sized(&chars));

    out.extend(section(10, &code));

    out
}

/// A module that has the host build things for it.
///
/// | export   | what it does                                                                    |
/// | -------- | ------------------------------------------------------------------------------- |
/// | `hog`    | pushes a 60,000-byte string onto a list until a push is refused, then returns;  |
/// |          | traps if 100,000 pushes, six gigabytes, were all accepted                       |
/// | `borrow` | appends to the list of its own arguments, and returns what the host answered    |
#[rustfmt::skip]
fn host_builder() -> Vec<u8> {
    let mut out = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

    // 0: a UDF. 1: list_new, (i32) -> i32. 2: list_push_str, (i32, i32, i32) -> i32.
    out.extend(section(1, &[
        0x03,
        0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
        0x60, 0x01, 0x7f, 0x01, 0x7f,
        0x60, 0x03, 0x7f, 0x7f, 0x7f, 0x01, 0x7f,
    ]));

    let mut import = vec![0x02];
    import.extend(sized(b"aerospike"));
    import.extend(sized(b"list_new"));
    import.extend([0x00, 0x01]);
    import.extend(sized(b"aerospike"));
    import.extend(sized(b"list_push_str"));
    import.extend([0x00, 0x02]);
    out.extend(section(2, &import));

    out.extend(section(3, &[0x02, 0x00, 0x00]));
    out.extend(section(5, &[0x01, 0x00, 0x01]));

    let mut export = vec![0x02];
    export.extend(sized(b"hog"));
    export.extend([0x00, 0x02]);
    export.extend(sized(b"borrow"));
    export.extend([0x00, 0x03]);
    out.extend(section(7, &export));

    let mut code = vec![0x02];

    // hog: two i32 locals, the list's handle (2) and a count (3)
    let mut hog = vec![0x01, 0x02, 0x7f];
    hog.extend([0x41, 0x08, 0x10, 0x00, 0x21, 0x02]);           // list = list_new(8)
    hog.extend([0x03, 0x40]);                                   // loop
    hog.extend([0x20, 0x02, 0x41, 0x00, 0x41]);                 //   list_push_str(list, 0, 60000)
    hog.extend(sleb(60_000));
    hog.extend([0x10, 0x01]);
    hog.extend([0x04, 0x40, 0x41, 0x00, 0x0f, 0x0b]);           //   if refused { return 0 }
    hog.extend([0x20, 0x03, 0x41, 0x01, 0x6a, 0x22, 0x03, 0x41]); // if ++count < 100000 { continue }
    hog.extend(sleb(100_000));
    hog.extend([0x49, 0x0d, 0x00]);
    hog.extend([0x0b, 0x00, 0x0b]);                             // end loop; unreachable
    code.extend(sized(&hog));

    // borrow: list_push_str(arguments, 0, 4)
    code.extend(sized(&[0x00, 0x20, 0x00, 0x41, 0x00, 0x41, 0x04, 0x10, 0x01, 0x0b]));

    out.extend(section(10, &code));

    out
}

fn skipped(test: &str, reason: &str) {
    eprintln!("skipping {test}: {reason}");
}

/// Register `bytes` as `package`, or say why this server cannot.
async fn register(client: &Client, package: &str, bytes: &[u8]) -> std::result::Result<(), String> {
    match client
        .register_udf(&AdminPolicy::default(), bytes, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => {
            task.wait_till_complete(None).await.map_err(|e| format!("{e}"))?;
            Ok(())
        }
        Err(e) => Err(format!("{e:?}")),
    }
}

async fn unregister(client: &Client, package: &str) {
    let _ = client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await;
}

/// Run `function` as a stream UDF over `set_name` and gather everything the server sent back about it:
/// string values and errors, as text.
async fn aggregate(client: &Client, namespace: &str, set_name: &str, package: &str, function: &str) -> (bool, String) {
    let mut policy = QueryPolicy::default();

    // The server's limit is what is under test, so the client must not give up first.
    policy.base_policy.socket_timeout = 180_000;
    policy.base_policy.total_timeout = 0;
    policy.base_policy.max_retries = 0;

    let mut statement = Statement::new(namespace, set_name, Bins::All);

    statement.set_aggregate_function(package, function, &[]);

    let mut failed = false;
    let mut said = String::new();

    match client.query(&policy, PartitionFilter::all(), statement).await {
        Ok(records) => {
            let mut stream = records.into_stream();

            while let Some(record) = stream.next().await {
                match record {
                    Ok(record) => {
                        for (_, value) in record.bins.iter() {
                            said.push_str(&format!("{value} "));
                        }
                    }
                    Err(e) => {
                        failed = true;
                        said.push_str(&format!("{e} "));
                    }
                }
            }
        }
        Err(e) => {
            failed = true;
            said.push_str(&format!("{e} "));
        }
    }

    (failed, said)
}

#[aerospike_macro::test]
async fn a_wasm_stream_udf_that_loops_is_stopped() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    if let Err(reason) = register(&client, package, &module(1)).await {
        skipped("a_wasm_stream_udf_that_loops_is_stopped", &reason);
        return;
    }

    for i in 1..=5 {
        client
            .put(&WritePolicy::default(), &as_key!(namespace, set_name, i), &[as_bin!("n", i)])
            .await
            .unwrap();
    }

    // `spin` never returns from a record; `fin` never returns from its final call. Each must be ended by
    // the server, on the stream's own clock: an aggregation query has no deadline to borrow.
    for function in ["spin", "fin"] {
        let start = Instant::now();
        let (failed, said) = aggregate(&client, namespace, set_name, package, function).await;
        let took = start.elapsed();

        eprintln!("wasm_limits: stream {function} ended after {took:?}: failed={failed} {said}");

        assert!(
            failed || said.contains("stopped"),
            "{function}: a stream UDF that never returns came back as if it had worked: {said}"
        );
        assert!(took < Duration::from_secs(120), "{function}: stopped only after {took:?}");

        // The node carries on: the thread the loop held is back, and the module still runs.
        client
            .execute_udf(&WritePolicy::default(), &key, package, "ok", &[])
            .await
            .unwrap_or_else(|e| panic!("the server did not recover from stream {function}: {e}"));
    }

    unregister(&client, package).await;
}

/// Every instance starts with a memory of zeroes, whatever the one before it on the same thread wrote.
///
/// The server keeps a freed memory block for the thread's next instance instead of mapping a new one, and
/// keeps a stream UDF's instance for the length of a query. Neither may carry a guest's data forward: a
/// record call must not see the call before it, and a query must not see the query before it, including
/// one that was stopped half-way.
#[aerospike_macro::test]
async fn nothing_a_guest_wrote_reaches_the_next_instance() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));

    if let Err(reason) = register(&client, package, &module(1)).await {
        skipped("nothing_a_guest_wrote_reaches_the_next_instance", &reason);
        return;
    }

    for i in 0..32 {
        client
            .put(&WritePolicy::default(), &as_key!(namespace, set_name, i), &[as_bin!("n", i)])
            .await
            .unwrap();
    }

    // Record calls, from several tasks at once so that many of the server's threads take part: each one
    // marks its memory, and the call after it on whichever thread must find none.
    let mut tasks = Vec::new();

    for t in 0..8i64 {
        let (client, package, namespace, set_name) =
            (client.clone(), package.clone(), namespace.to_owned(), set_name.clone());

        tasks.push(aerospike_rt::spawn(async move {
            let key = as_key!(namespace.as_str(), set_name.as_str(), t);
            let write = WritePolicy::default();

            for round in 0..250 {
                client
                    .execute_udf(&write, &key, &package, "taint", &[])
                    .await
                    .unwrap_or_else(|e| panic!("taint failed in round {round}: {e}"));
                client
                    .execute_udf(&write, &key, &package, "stale", &[])
                    .await
                    .unwrap_or_else(|e| panic!("round {round}: a record call found the memory of an earlier one: {e}"));
            }
        }));
    }

    for task in tasks {
        task.await.expect("a record-call task panicked");
    }

    // Queries: one marks the memory of every thread it runs on and ends normally; the next must find none.
    for round in 0..10 {
        let (failed, said) = aggregate(&client, namespace, set_name, package, "taint").await;

        assert!(!failed, "round {round}: the marking query failed: {said}");

        let (failed, said) = aggregate(&client, namespace, set_name, package, "stale").await;

        assert!(!failed, "round {round}: a query found the memory of the query before it: {said}");
    }

    // And one that marks its memory and is then stopped by the server, mid-call.
    let (failed, _) = aggregate(&client, namespace, set_name, package, "taintspin").await;

    assert!(failed, "a stream UDF that never returns was not stopped");

    for round in 0..10 {
        let (failed, said) = aggregate(&client, namespace, set_name, package, "stale").await;

        assert!(!failed, "round {round}: a query found the memory of a stopped query: {said}");
    }

    client
        .execute_udf(&WritePolicy::default(), &as_key!(namespace, set_name, 0), package, "stale", &[])
        .await
        .expect("a record call found the memory of a stopped query");

    unregister(&client, package).await;
}

/// A module's start function is guest code like any other, and is stopped at the deadline like any other.
///
/// It used not to be: the runtime ran it while making the instance, where nothing could reach it, and each
/// call of such a module took a server thread, and the record's lock, for good.
#[aerospike_macro::test]
async fn a_start_function_that_loops_is_stopped() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));

    if let Err(reason) = register(&client, package, &start_spins()).await {
        skipped("a_start_function_that_loops_is_stopped", &reason);
        return;
    }

    let Some(stopped_before) = statistic(&client, "udf_wasm_timeouts").await else {
        skipped("a_start_function_that_loops_is_stopped", "this server exports no udf_wasm statistics");
        unregister(&client, package).await;
        return;
    };

    let mut write = WritePolicy::default();

    write.base_policy.total_timeout = 500;
    write.base_policy.socket_timeout = 500;
    write.base_policy.max_retries = 0;

    // A few at once, and briefly: each holds a server thread until its deadline, and other tests are
    // running beside this one.
    const CALLS: i64 = 8;

    let mut tasks = Vec::new();

    for i in 0..CALLS {
        let (client, package, namespace, set_name, write) =
            (client.clone(), package.clone(), namespace.to_owned(), set_name.clone(), write.clone());

        tasks.push(aerospike_rt::spawn(async move {
            let key = as_key!(namespace.as_str(), set_name.as_str(), i);

            client.execute_udf(&write, &key, &package, "ok", &[]).await.is_err()
        }));
    }

    for task in tasks {
        assert!(task.await.unwrap(), "a call of a module whose start function never returns succeeded");
    }

    // They were ended by the server, at their deadlines: that is what this counter counts. Not
    // necessarily all of them - a call that waited in the queue past its own timeout never started.
    aerospike_rt::sleep(Duration::from_secs(1)).await;

    let stopped = statistic(&client, "udf_wasm_timeouts").await.unwrap() - stopped_before;

    assert!(stopped >= 1, "{CALLS} calls looped in their start function and the server stopped none");
    eprintln!("wasm_limits: the server stopped {stopped} of {CALLS} looping start functions");

    // And the records they held are free again.
    for i in 0..CALLS {
        client
            .put(&WritePolicy::default(), &as_key!(namespace, set_name, i), &[as_bin!("n", i)])
            .await
            .unwrap_or_else(|e| panic!("record {i} is still held by a stopped call: {e}"));
    }

    unregister(&client, package).await;
}

/// The server calls a UDF as `(i32, i32) -> i32`, and an export of any other type is refused with an error
/// that says so, however the module declared it. A wider result used to be written over the server's own
/// stack.
#[aerospike_macro::test]
async fn an_export_of_another_type_is_refused() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    if let Err(reason) = register(&client, package, &wrong_shapes()).await {
        skipped("an_export_of_another_type_is_refused", &reason);
        return;
    }

    client.put(&write, &key, &[as_bin!("n", 7)]).await.unwrap();

    for round in 0..3 {
        for export in ["wide", "long", "bare", "floats"] {
            let outcome = client.execute_udf(&write, &key, package, export, &[]).await;
            let text = format!("{outcome:?}");

            assert!(outcome.is_err(), "round {round}: {export} was called: {text}");
            assert!(text.contains("-> i32"), "round {round}: {export} was refused without saying why: {text}");

            // The server is as it was: the well-typed export runs, and the record is untouched.
            client
                .execute_udf(&write, &key, package, "ok", &[])
                .await
                .unwrap_or_else(|e| panic!("round {round}: the server did not recover from {export}: {e}"));

            let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

            assert_eq!(record.bins.get("n"), Some(&Value::Int(7)), "round {round}: after {export}");
        }
    }

    // A stream UDF's two exports are held to the same type.
    let (failed, said) = aggregate(&client, namespace, set_name, package, "wide").await;

    assert!(failed, "a stream UDF with no `_record` and `_end` exports ran: {said}");

    unregister(&client, package).await;
}

/// The Java runtime's log import calls back into the module for the string, and those callbacks are the
/// module's own functions: they are held to their type, and their answers are not believed.
///
/// A callback declared to return four 64-bit values used to have them written over a one-word array on
/// the server's stack. A length of two to the thirty-first characters used to pass its bounds check as
/// zero bytes, after which the server read past the end of the module's memory.
#[aerospike_macro::test]
async fn the_java_log_callbacks_are_not_trusted() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    client.put(&write, &key, &[as_bin!("n", 7)]).await.unwrap();

    for (what, wide_data) in [("a callback of the wrong type", true), ("a length that wraps", false)] {
        let package = &format!("wasm_limits_{}", common::rand_str(6));

        if let Err(reason) = register(&client, package, &java_helpers(wide_data)).await {
            skipped("the_java_log_callbacks_are_not_trusted", &reason);
            return;
        }

        for round in 0..20 {
            let outcome = client.execute_udf(&write, &key, package, "go", &[]).await;

            if wide_data {
                // Refused, by name: the call fails the way any wrongly typed export does.
                let text = format!("{outcome:?}");

                assert!(outcome.is_err(), "{what}, round {round}: the callback was called: {text}");
                assert!(text.contains("-> i32"), "{what}, round {round}: refused without saying why: {text}");
            } else {
                // Checked against the memory there really is: the read is refused as any access outside
                // the module's memory is, and the call ends in that trap.
                let text = format!("{outcome:?}");

                assert!(outcome.is_err(), "{what}, round {round}: the length was believed: {text}");
                assert!(text.contains("out of bounds"), "{what}, round {round}: {text}");
            }
        }

        let record = client.get(&ReadPolicy::default(), &key, Bins::All).await.unwrap();

        assert_eq!(record.bins.get("n"), Some(&Value::Int(7)), "{what}");

        unregister(&client, package).await;
    }
}

/// A module cannot make the server allocate by claiming to be larger than it is.
///
/// Thirty-nine bytes whose type section says it holds half a billion types used to cost the server four
/// gigabytes and several seconds at registration: the runtime's loader made room for them before reading
/// any. It is refused now, at once, for saying so.
#[aerospike_macro::test]
async fn a_section_that_lies_about_its_size_is_refused() {
    #[rustfmt::skip]
    const TOO_MANY_TYPES: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
        // type section: ten bytes, the first five a count of 494,927,865
        0x01, 0x0a, 0xf9, 0xff, 0xff, 0xeb, 0x01, 0x07, 0x06, 0x01, 0x02, 0x00,
        0x08, 0x01, 0x01,
        0x0a, 0x0e, 0x02, 0x04, 0x00, 0x41, 0x00, 0x0b, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b,
    ];

    let client = common::client().await;
    let package = &format!("wasm_limits_{}", common::rand_str(6));

    // A server without WASM refuses it too, for another reason: nothing to check there.
    if let Err(reason) = register(&client, &format!("{package}_ok"), &module(1)).await {
        skipped("a_section_that_lies_about_its_size_is_refused", &reason);
        return;
    }

    unregister(&client, &format!("{package}_ok")).await;

    let start = Instant::now();
    let outcome = register(&client, package, TOO_MANY_TYPES).await;
    let took = start.elapsed();

    eprintln!("wasm_limits: the lying module was answered in {took:?}");

    let refusal = outcome.expect_err("a module claiming half a billion types was registered");
    let reason = refusal
        .split("message=")
        .nth(1)
        .map(|rest| rest.split(|c: char| !(c.is_ascii_alphanumeric() || "+/=".contains(c))).next().unwrap_or(""))
        .map(unbase64)
        .unwrap_or_default();

    assert!(reason.contains("more entries"), "refused, but not for its size: {reason} ({refusal})");
    assert!(took < Duration::from_millis(500), "refusing it took {took:?}: the server did the work first");
}

/// What a guest has the host build for it - strings, lists, maps made through host calls - is on the
/// server's heap, not in the guest's memory, and is counted against the call all the same.
///
/// It used not to be counted at all: a guest could append to a list until its deadline.
#[aerospike_macro::test]
async fn what_a_guest_builds_on_the_host_is_bounded() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    if let Err(reason) = register(&client, package, &host_builder()).await {
        skipped("what_a_guest_builds_on_the_host_is_bounded", &reason);
        return;
    }

    let Some(refused_before) = statistic(&client, "udf_wasm_host_refusals").await else {
        skipped("what_a_guest_builds_on_the_host_is_bounded", "this server does not count host refusals");
        unregister(&client, package).await;
        return;
    };

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    // `hog` returns only when a push was refused, and traps if none ever was.
    let start = Instant::now();

    for round in 0..5 {
        client
            .execute_udf(&write, &key, package, "hog", &[])
            .await
            .unwrap_or_else(|e| panic!("round {round}: the host built six gigabytes for one call: {e}"));
    }

    eprintln!("wasm_limits: five calls, each stopped at its budget, took {:?}", start.elapsed());

    let refused = statistic(&client, "udf_wasm_host_refusals").await.unwrap() - refused_before;

    assert!(refused >= 5, "five calls ran into their budget and the node counted {refused}");

    // What they built went with them: the node is not still holding five budgets' worth.
    aerospike_rt::sleep(Duration::from_millis(500)).await;

    let held = statistic(&client, "udf_wasm_guest_memory_bytes").await.unwrap();

    assert!(held < 32 * 1024 * 1024, "the node still counts {held} bytes of guest memory after the calls ended");

    // The list of a call's own arguments is the server's, lent to the call: it may not be added to.
    let outcome = client.execute_udf(&write, &key, package, "borrow", &[Value::Int(1)]).await;

    assert!(outcome.is_err(), "a guest appended to its own argument list: {outcome:?}");

    unregister(&client, package).await;
}

/// One `name=value` statistic of the node, as a number.
async fn statistic(client: &Client, name: &str) -> Option<u64> {
    let all = client.info(&AdminPolicy::default(), &["statistics"]).await.ok()?;

    all.values()
        .flat_map(|v| v.split(';'))
        .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
        .and_then(|v| v.parse().ok())
}

/// What the engine counts is what happened: a call, a trap, a failure of the guest's own, dropped log
/// lines; and `udf-stats` says the same per module.
///
/// Counters are node-wide and other tests run beside this one, so the node's numbers are checked to have
/// moved by at least what this test did, and the module's own, which nothing else touches, exactly.
#[aerospike_macro::test]
async fn the_engine_counts_what_wasm_udfs_do() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);
    let write = WritePolicy::default();

    if let Err(reason) = register(&client, package, &module(1)).await {
        skipped("the_engine_counts_what_wasm_udfs_do", &reason);
        return;
    }

    let Some(calls_before) = statistic(&client, "udf_wasm_calls").await else {
        skipped("the_engine_counts_what_wasm_udfs_do", "this server exports no udf_wasm statistics");
        unregister(&client, package).await;
        return;
    };

    let traps_before = statistic(&client, "udf_wasm_traps").await.unwrap();
    let dropped_before = statistic(&client, "udf_wasm_log_lines_dropped").await.unwrap();

    client.put(&write, &key, &[as_bin!("n", 1)]).await.unwrap();

    for _ in 0..7 {
        client.execute_udf(&write, &key, package, "ok", &[]).await.unwrap();
    }

    for _ in 0..3 {
        assert!(client.execute_udf(&write, &key, package, "trap", &[]).await.is_err());
    }

    // 1000 lines asked for, 16 allowed to the call.
    client.execute_udf(&write, &key, package, "flood", &[]).await.unwrap();

    let calls = statistic(&client, "udf_wasm_calls").await.unwrap() - calls_before;
    let traps = statistic(&client, "udf_wasm_traps").await.unwrap() - traps_before;
    let dropped = statistic(&client, "udf_wasm_log_lines_dropped").await.unwrap() - dropped_before;

    assert!(calls >= 11, "11 calls were made and the node counted {calls}");
    assert!(traps >= 3, "3 calls trapped and the node counted {traps}");
    assert!(dropped >= 984, "984 log lines were over the call's limit and the node counted {dropped}");

    for gauge in ["udf_wasm_guest_memory_bytes", "udf_wasm_modules_loaded", "udf_wasm_compile_queue"] {
        assert!(statistic(&client, gauge).await.is_some(), "the node does not report {gauge}");
    }

    assert!(statistic(&client, "udf_wasm_modules_loaded").await.unwrap() >= 1);

    // The module's own line.
    let listing = client.info(&AdminPolicy::default(), &["udf-stats"]).await.unwrap();
    let mine = listing
        .values()
        .flat_map(|v| v.split(';'))
        .find(|entry| entry.contains(package.as_str()))
        .unwrap_or_else(|| panic!("udf-stats does not list {package}: {listing:?}"))
        .to_owned();

    eprintln!("wasm_limits: udf-stats: {mine}");

    let field = |name: &str| -> String {
        mine.split(',')
            .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
            .unwrap_or_else(|| panic!("udf-stats has no {name} for the module: {mine}"))
            .to_owned()
    };

    assert_eq!(field("calls"), "11");
    assert_eq!(field("traps"), "3");
    assert_eq!(field("timeouts"), "0");
    assert_eq!(field("errors"), "0");
    assert_eq!(field("log_lines_dropped"), "984");
    assert!(!field("tier").is_empty());

    unregister(&client, package).await;
}

#[aerospike_macro::test]
async fn a_wasm_udf_that_floods_the_log_is_survived() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    if let Err(reason) = register(&client, package, &module(1)).await {
        skipped("a_wasm_udf_that_floods_the_log_is_survived", &reason);
        return;
    }

    client.put(&WritePolicy::default(), &key, &[as_bin!("n", 1)]).await.unwrap();

    // 20,000 lines asked for. What reaches the server log is the module's allowance, with the newline in
    // each written as an escape; that is checked on the node. From here: the calls succeed, promptly.
    let start = Instant::now();

    for _ in 0..20 {
        client
            .execute_udf(&WritePolicy::default(), &key, package, "flood", &[])
            .await
            .unwrap_or_else(|e| panic!("a call that logs too much failed outright: {e}"));
    }

    eprintln!("wasm_limits: 20 flooding calls of {package} took {:?}", start.elapsed());

    client
        .execute_udf(&WritePolicy::default(), &key, package, "ok", &[])
        .await
        .unwrap();

    unregister(&client, package).await;
}

#[aerospike_macro::test]
async fn wasm_memory_is_capped_across_the_node() {
    if std::env::var("AEROSPIKE_WASM_SMALL_LIMITS").is_err() {
        skipped("wasm_memory_is_capped_across_the_node", "needs a node with max-total-memory 8M");
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let small = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    client.put(&WritePolicy::default(), &key, &[as_bin!("n", 1)]).await.unwrap();
    register(&client, small, &module(1)).await.expect("the small module registers");

    // One page fits. Growing by 16 MiB does not fit in 8 MiB: the grow is refused, which this module
    // turns into a trap.
    client
        .execute_udf(&WritePolicy::default(), &key, small, "ok", &[])
        .await
        .expect("a one-page instance fits under the ceiling");

    let grown = client.execute_udf(&WritePolicy::default(), &key, small, "grow", &[]).await;

    eprintln!("wasm_limits: grow past the ceiling: {grown:?}");
    assert!(grown.is_err(), "a 16 MiB grow succeeded under an 8 MiB ceiling: {grown:?}");

    // What was refused was given back: the module still runs.
    client
        .execute_udf(&WritePolicy::default(), &key, small, "ok", &[])
        .await
        .expect("the node recovered from a refused grow");

    unregister(&client, small).await;

    // A module that asks for 16 MiB up front registers - registration does not instantiate - and is
    // refused when called, with an error that names the ceiling.
    let big = &format!("wasm_limits_{}", common::rand_str(6));

    register(&client, big, &module(256)).await.expect("the large module registers");

    let refused = client.execute_udf(&WritePolicy::default(), &key, big, "ok", &[]).await;
    let text = format!("{refused:?}");

    eprintln!("wasm_limits: an instance over the ceiling: {text}");
    assert!(refused.is_err(), "a 16 MiB instance started under an 8 MiB ceiling");
    assert!(text.contains("max-total-memory"), "the error does not name the ceiling: {text}");

    unregister(&client, big).await;
}

#[aerospike_macro::test]
async fn wasm_modules_are_capped_in_number() {
    if std::env::var("AEROSPIKE_WASM_SMALL_LIMITS").is_err() {
        skipped("wasm_modules_are_capped_in_number", "needs a node with max-modules 3");
        return;
    }

    let client = common::client().await;
    let bytes = module(1);
    let mut registered = Vec::new();
    let mut refusal = None;

    // The ceiling is three, and other tests may hold some of it: by the fourth of these at the latest
    // a registration is refused.
    for _ in 0..4 {
        let package = format!("wasm_limits_{}", common::rand_str(6));

        match register(&client, &package, &bytes).await {
            Ok(()) => registered.push(package),
            Err(e) => {
                refusal = Some(e);
                break;
            }
        }
    }

    eprintln!("wasm_limits: {} registered, then: {refusal:?}", registered.len());

    let refusal = refusal.expect("four modules registered under a ceiling of three");

    // The server sends a registration error's text in base64, after `message=`.
    let reason = refusal
        .split("message=")
        .nth(1)
        .map(|rest| rest.split(|c: char| !(c.is_ascii_alphanumeric() || "+/=".contains(c))).next().unwrap_or(""))
        .map(unbase64)
        .unwrap_or_default();

    eprintln!("wasm_limits: the refusal reads: {reason}");
    assert!(reason.contains("max-modules"), "the refusal does not name the ceiling: {reason} ({refusal})");

    // Registering a name that exists replaces it, and is allowed at the ceiling.
    if let Some(existing) = registered.first() {
        register(&client, existing, &bytes)
            .await
            .unwrap_or_else(|e| panic!("re-registering {existing} at the ceiling was refused: {e}"));
    }

    // Removing one makes room for one.
    if let Some(gone) = registered.pop() {
        unregister(&client, &gone).await;
        aerospike_rt::sleep(Duration::from_secs(1)).await;

        let package = format!("wasm_limits_{}", common::rand_str(6));

        register(&client, &package, &bytes)
            .await
            .unwrap_or_else(|e| panic!("no room after removing a module: {e}"));
        registered.push(package);
    }

    for package in &registered {
        unregister(&client, package).await;
    }
}

#[aerospike_macro::test]
async fn an_aborted_query_stops_its_wasm_stream_udf() {
    if std::env::var("AEROSPIKE_WASM_SLOW_STREAM").is_err() {
        skipped("an_aborted_query_stops_its_wasm_stream_udf", "needs a node with stream-timeout-ms 120000");
        return;
    }

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_limits_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    register(&client, package, &module(1)).await.expect("the module registers");
    client.put(&WritePolicy::default(), &key, &[as_bin!("n", 1)]).await.unwrap();

    // The query spins in the guest. Two seconds in, find it and abort it: the time budget is two minutes
    // away, so anything that ends it sooner is the abort reaching a running guest.
    let aborter = {
        let client = client.clone();

        aerospike_rt::spawn(async move {
            aerospike_rt::sleep(Duration::from_secs(2)).await;

            let shown = client.info(&AdminPolicy::default(), &["query-show"]).await.unwrap();
            let jobs: String = shown.values().cloned().collect();
            let mut aborted = 0;

            for job in jobs.split(';') {
                let field = |name: &str| {
                    job.split(':')
                        .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
                        .map(str::to_owned)
                };

                if field("job-type").as_deref() != Some("aggregation") {
                    continue;
                }

                if let Some(trid) = field("trid") {
                    let command = format!("query-abort:trid={trid}");
                    let answer = client.info(&AdminPolicy::default(), &[command.as_str()]).await;

                    eprintln!("wasm_limits: {command} -> {answer:?}");
                    aborted += 1;
                }
            }

            aborted
        })
    };

    let start = Instant::now();
    let (failed, said) = aggregate(&client, namespace, set_name, package, "spin").await;
    let took = start.elapsed();

    eprintln!("wasm_limits: aborted stream ended after {took:?}: failed={failed} {said}");

    assert!(took < Duration::from_secs(30), "the abort did not reach the guest: it ran {took:?}");

    let _ = aborter.await;

    client
        .execute_udf(&WritePolicy::default(), &key, package, "ok", &[])
        .await
        .expect("the node recovered from the aborted query");

    unregister(&client, package).await;
}
