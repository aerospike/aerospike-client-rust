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

//! Sandbox conformance for WASM UDFs: a module that steps outside what a guest is allowed must be
//! stopped with an error, on whichever tier the server runs it, and the server must carry on.
//!
//! The module is assembled by hand below, so the suite needs no WASM toolchain and its bytes are in
//! review. It has no imports. Each export has the UDF signature `(i32, i32) -> i32` and does one thing:
//!
//! | export         | what it does                                    | what must happen            |
//! | -------------- | ----------------------------------------------- | --------------------------- |
//! | `ok`           | returns                                         | succeeds                    |
//! | `oob_read`     | loads from the last word of the address space   | trap                        |
//! | `oob_write`    | stores to the last word of the address space    | trap                        |
//! | `oob_edge`     | loads the first byte past its one page          | trap                        |
//! | `recurse`      | calls itself without end                        | trap, or the deadline       |
//! | `bad_indirect` | calls through a table index that is not there   | trap                        |
//! | `spin`         | loops without end                               | stopped by the deadline     |
//!
//! A module compiled ahead of time is built in the background after registration, so every case runs
//! twice with a pause between: the first round is the interpreter (or the JIT), the second is the
//! compiled module on a server that compiles. Both rounds must pass. The registered tier is printed.

use crate::common;

use aerospike::*;

/// The module, section by section.
#[rustfmt::skip]
const MODULE: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,             // magic, version 1
    // type: one signature, (i32, i32) -> i32
    0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
    // function: seven functions of that type
    0x03, 0x08, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    // table: one funcref table with one (null) slot
    0x04, 0x04, 0x01, 0x70, 0x00, 0x01,
    // memory: exactly one 64 KiB page
    0x05, 0x04, 0x01, 0x01, 0x01, 0x01,
    // export
    0x07, 0x48, 0x07,
    0x02, b'o', b'k', 0x00, 0x00,
    0x08, b'o', b'o', b'b', b'_', b'r', b'e', b'a', b'd', 0x00, 0x01,
    0x09, b'o', b'o', b'b', b'_', b'w', b'r', b'i', b't', b'e', 0x00, 0x02,
    0x08, b'o', b'o', b'b', b'_', b'e', b'd', b'g', b'e', 0x00, 0x03,
    0x07, b'r', b'e', b'c', b'u', b'r', b's', b'e', 0x00, 0x04,
    0x0c, b'b', b'a', b'd', b'_', b'i', b'n', b'd', b'i', b'r', b'e', b'c', b't', 0x00, 0x05,
    0x04, b's', b'p', b'i', b'n', 0x00, 0x06,
    // code
    0x0a, 0x4c, 0x07,
    0x04, 0x00, 0x41, 0x00, 0x0b,                               // ok: 0
    0x0a, 0x00, 0x41, 0x7c, 0x28, 0x02, 0x00, 0x1a, 0x41, 0x00, 0x0b, // oob_read: i32.load at -4
    0x0b, 0x00, 0x41, 0x7c, 0x41, 0x01, 0x36, 0x02, 0x00, 0x41, 0x00, 0x0b, // oob_write: i32.store at -4
    0x0c, 0x00, 0x41, 0x80, 0x80, 0x04, 0x2d, 0x00, 0x00, 0x1a, 0x41, 0x00, 0x0b, // oob_edge: load8_u at 65536
    0x0b, 0x00, 0x20, 0x00, 0x20, 0x01, 0x10, 0x04, 0x41, 0x01, 0x6a, 0x0b, // recurse: self(a, b) + 1
    0x0b, 0x00, 0x20, 0x00, 0x20, 0x01, 0x41, 0x05, 0x11, 0x00, 0x00, 0x0b, // bad_indirect: table[5]
    0x09, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x41, 0x00, 0x0b, // spin: loop { }
];

/// Every case that must fail, with why.
const MUST_FAIL: [(&str, &str); 6] = [
    ("oob_read", "a load outside linear memory"),
    ("oob_write", "a store outside linear memory"),
    ("oob_edge", "a load one byte past the end of linear memory"),
    ("recurse", "unbounded recursion"),
    ("bad_indirect", "an indirect call through a missing table slot"),
    ("spin", "a loop that never returns"),
];

fn skipped(reason: &str) {
    eprintln!("skipping wasm_sandbox: {reason}");
}

#[aerospike_macro::test]
async fn a_wasm_udf_that_leaves_its_sandbox_is_stopped_on_every_tier() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);
    let package = &format!("wasm_sandbox_{}", common::rand_str(6));
    let key = as_key!(namespace, set_name, 1);

    // A deadline, so the two cases that never return are stopped by the server and not by patience.
    let mut write = WritePolicy::default();
    write.base_policy.total_timeout = 2_000;
    write.base_policy.socket_timeout = 2_000;
    write.base_policy.max_retries = 0;

    client.put(&WritePolicy::default(), &key, &[as_bin!("n", 7)]).await.unwrap();

    match client
        .register_udf(&AdminPolicy::default(), MODULE, &format!("{package}.wasm"), UdfLang::Wasm)
        .await
    {
        Ok(task) => {
            task.wait_till_complete(None).await.expect("the module became available");
        }
        Err(e) if format!("{e:?}").contains("invalid_udf_type") => {
            skipped("this server does not support WASM UDFs");
            return;
        }
        Err(e) => panic!("the hand-assembled module was refused: {e:?}"),
    }

    for round in ["as registered", "after the background build"] {
        if let Ok(listing) = client.info(&AdminPolicy::default(), &["udf-list"]).await {
            let mine = listing
                .values()
                .flat_map(|v| v.split(';'))
                .find(|entry| entry.contains(package.as_str()))
                .map(str::to_owned);
            eprintln!("wasm_sandbox, {round}: {}", mine.unwrap_or_else(|| "(not listed)".into()));
        }

        // The well-behaved export works, before and between the others.
        client
            .execute_udf(&write, &key, package, "ok", &[])
            .await
            .unwrap_or_else(|e| panic!("{round}: the well-behaved export failed: {e}"));

        for (function, what) in MUST_FAIL {
            let outcome = client.execute_udf(&write, &key, package, function, &[]).await;
            assert!(
                outcome.is_err(),
                "{round}: {function} ({what}) returned {outcome:?} instead of being stopped"
            );

            // The server is still serving, the module still runs, and the record is as it was.
            client
                .execute_udf(&write, &key, package, "ok", &[])
                .await
                .unwrap_or_else(|e| panic!("{round}: the server did not recover from {function}: {e}"));
            let record = client
                .get(&ReadPolicy::default(), &key, Bins::All)
                .await
                .unwrap_or_else(|e| panic!("{round}: the record is unreadable after {function}: {e}"));
            assert_eq!(record.bins.get("n"), Some(&Value::Int(7)), "{round}: after {function}");
        }

        // Long enough for the background compile of a module this small.
        aerospike_rt::sleep(std::time::Duration::from_secs(4)).await;
    }

    let _ = client
        .remove_udf(&AdminPolicy::default(), &format!("{package}.wasm"))
        .await;
}
