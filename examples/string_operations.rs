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

//! String operations: read and edit string bins on the server.
//!
//! A string op runs where the data is, so a client can append to a log line,
//! normalise a name or test a pattern without reading the whole value back
//! and writing it again. Reads (`strlen`, `find`, `contains`, `split`, …)
//! return their answer under the bin name; edits (`append`, `upper`,
//! `replace`, …) change the bin and take a `StringPolicy` like list and map
//! writes do. The same functions exist as expressions.
//!
//! Requires Aerospike server 8.2.0+; the example skips on older servers.
//!
//! ```bash
//! cargo run --example string_operations
//! ```

use std::env;

use aerospike::expressions::{self as exp, string as str_exp};
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::operations::string::{self as string, StringPolicy, StringWriteFlags};
use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, Value, WritePolicy};

const SET: &str = "string_demo";

#[tokio::main]
async fn main() {
    run().await;
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let mut cpolicy = ClientPolicy::default();
    cpolicy.use_services_alternate = env::var("AEROSPIKE_USE_SERVICES_ALTERNATE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let supported = client
        .random_node()
        .map(|n| n.version().supports_string_operations())
        .unwrap_or(false);
    if !supported {
        println!("Server does not support string operations (requires 8.2.0+); skipping.");
        client.close().await.unwrap();
        return;
    }

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let key = as_key!("test", SET, "greeting");
    let _ = client.delete(&wpolicy, &key).await;

    client
        .put(&wpolicy, &key, &[as_bin!("text", "  Hello, Aerospike  "), as_bin!("csv", "a,b,c")])
        .await
        .unwrap();

    // ---- Reads: the answer comes back under the bin name ----
    let ops = [
        string::strlen("text"),
        string::find("text", "Aerospike"),
        string::contains("text", "Hello"),
        string::starts_with("text", "  He"),
        string::substr("text", 2, 7),
        string::split_by_separator("csv", ","),
        string::is_numeric("csv"),
        string::regex_compare("text", "^\\s*Hello.*$"),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    let results = rec.results.as_deref().unwrap_or(&[]);
    println!("strlen       = {:?}", results.first());
    println!("find         = {:?}", results.get(1));
    println!("contains     = {:?}", results.get(2));
    println!("starts_with  = {:?}", results.get(3));
    println!("substr(2,7)  = {:?}", results.get(4));
    println!("split        = {:?}", results.get(5));
    println!("is_numeric   = {:?}", results.get(6));
    println!("regex match  = {:?}", results.get(7));

    // ---- Edits: a StringPolicy carries the write flags ----
    let policy = StringPolicy::default();
    let ops = [
        string::trim(&policy, "text"),
        string::replace(&policy, "text", "Hello", "Hi"),
        string::append(&policy, "text", "!"),
        string::prepend(&policy, "text", "> "),
        string::upper(&policy, "text"),
    ];
    client.operate(&wpolicy, &key, &ops).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::from(["text"])).await.unwrap();
    println!("after edits  = {:?}", rec.bins.get("text"));

    // CREATE_ONLY refuses to touch a bin that already exists; NO_FAIL turns
    // that refusal into a no-op instead of an error.
    let create_only = StringPolicy::new(StringWriteFlags::CREATE_ONLY | StringWriteFlags::NO_FAIL);
    client
        .operate(&wpolicy, &key, &[string::append(&create_only, "text", " (ignored)")])
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key, Bins::from(["text"])).await.unwrap();
    println!("create-only append was a no-op: {:?}", rec.bins.get("text"));

    // `snip` cuts a range out; `repeat` and `to_integer` round things off.
    client
        .put(&wpolicy, &key, &[as_bin!("num", "0042"), as_bin!("word", "ab")])
        .await
        .unwrap();
    let ops = [
        string::to_integer("num"),
        string::repeat(&policy, "word", 3),
        string::snip(&policy, "text", 0, 2),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("to_integer   = {:?}", rec.results.as_deref().and_then(|r| r.first()));
    let rec = client.get(&rpolicy, &key, Bins::from(["word", "text"])).await.unwrap();
    println!("repeat ×3    = {:?}, snip(0,2) = {:?}", rec.bins.get("word"), rec.bins.get("text"));

    // ---- Expressions: the same functions inside read_exp and filters ----
    let ops = [
        read_exp(
            "shout",
            str_exp::upper(&policy, exp::string_bin("text")),
            ExpReadFlags::DEFAULT,
        ),
        read_exp(
            "len",
            str_exp::strlen(exp::string_bin("text")),
            ExpReadFlags::DEFAULT,
        ),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("expression upper = {:?}, strlen = {:?}", rec.bins.get("shout"), rec.bins.get("len"));

    let mut only_hi = ReadPolicy::default();
    only_hi.base_policy.filter_expression = Some(str_exp::contains(
        exp::string_bin("text"),
        exp::string_val("HI"),
    ));
    let rec = client.get(&only_hi, &key, Bins::from(["text"])).await;
    println!(
        "filter `contains(text, \"HI\")` passes: {}",
        rec.as_ref().map(|r| r.bins.get("text") != Some(&Value::Nil)).unwrap_or(false)
    );

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
