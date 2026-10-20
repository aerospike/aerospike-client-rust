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

//! Expression operations: compute and write with expressions inside `operate`.
//!
//! Filter expressions decide whether a record is touched. Expression
//! *operations* go further: `read_exp` evaluates an expression on the server
//! and returns the value under a name of your choice, and `write_exp` stores
//! the result in a bin. Both run in the same `operate` as ordinary ops, so a
//! derived value is computed from the record's current state with no round
//! trip. The expression modules for lists, maps, bits, HLL and strings all
//! plug in here, as do the path expressions on nested documents.
//!
//! ```bash
//! cargo run --example expression_operations
//! ```

use std::env;

use aerospike::expressions::{self as exp, bitwise as bit_exp, lists as list_exp, string as str_exp};
use aerospike::operations::cdt_context::ctx_map_key;
use aerospike::operations::exp::{read_exp, write_exp, ExpReadFlags, ExpWriteFlags};
use aerospike::operations::lists::{ListPolicy, ListReturnType};
use aerospike::operations::path::SelectFlag;
use aerospike::operations::string::StringPolicy;
use aerospike::{as_bin, as_blob, as_key, as_list, as_map, as_val, Bins, Client, ClientPolicy};
use aerospike::{ReadPolicy, Value, WritePolicy};

const SET: &str = "exp_ops_demo";

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

    let wpolicy = WritePolicy::default();
    let rpolicy = ReadPolicy::default();
    let key = as_key!("test", SET, "order-1");
    let _ = client.delete(&wpolicy, &key).await;

    client
        .put(
            &wpolicy,
            &key,
            &[
                as_bin!("price", 250),
                as_bin!("qty", 4),
                as_bin!("tags", as_list!("rush", "gift")),
                as_bin!("flags", as_blob!(vec![0b1011_0000u8])),
                as_bin!("customer", "ada lovelace"),
                as_bin!("doc", as_map!("items" => as_list!(10, 20, 30), "coupon" => "SAVE10")),
            ],
        )
        .await
        .unwrap();

    // ---- read_exp: a computed value returned under a name you choose ----
    let total = exp::num_mul(vec![exp::int_bin("price"), exp::int_bin("qty")]);
    let ops = [
        read_exp("total", total.clone(), ExpReadFlags::DEFAULT),
        read_exp("tag_count", list_exp::size(exp::list_bin("tags"), &[]), ExpReadFlags::DEFAULT),
        read_exp(
            "flag_bits",
            bit_exp::count(exp::int_val(0), exp::int_val(8), exp::blob_bin("flags")),
            ExpReadFlags::DEFAULT,
        ),
        read_exp(
            "shout",
            str_exp::upper(&StringPolicy::default(), exp::string_bin("customer")),
            ExpReadFlags::DEFAULT,
        ),
    ];
    let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
    println!("price × qty      = {:?}", rec.bins.get("total"));
    println!("tags.size()      = {:?}", rec.bins.get("tag_count"));
    println!("flags bit count  = {:?}", rec.bins.get("flag_bits"));
    println!("upper(customer)  = {:?}", rec.bins.get("shout"));

    // ---- write_exp: store the result in a bin, with a condition ----
    // A 10% discount when the "rush" tag is present, else the full total;
    // `cond` is if/else-if/else with a trailing default.
    let discounted = exp::cond(vec![
        list_exp::get_by_value(
            ListReturnType::EXISTS,
            exp::string_val("rush"),
            exp::list_bin("tags"),
            &[],
        ),
        exp::num_sub(vec![total.clone(), exp::num_mul(vec![total.clone(), exp::int_val(10)])]),
        total.clone(),
    ]);
    let ops = [
        write_exp("due", discounted, ExpWriteFlags::DEFAULT),
        write_exp(
            "tags",
            list_exp::append(ListPolicy::default(), exp::string_val("billed"), exp::list_bin("tags"), &[]),
            ExpWriteFlags::DEFAULT,
        ),
    ];
    client.operate(&wpolicy, &key, &ops).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::from(["due", "tags"])).await.unwrap();
    println!("due (written)    = {:?}", rec.bins.get("due"));
    println!("tags (appended)  = {:?}", rec.bins.get("tags"));

    // CREATE_ONLY: write only when the bin is absent; POLICY_NO_FAIL makes a
    // refused write a no-op instead of an error.
    let ops = [write_exp(
        "due",
        exp::int_val(0),
        ExpWriteFlags::CREATE_ONLY | ExpWriteFlags::POLICY_NO_FAIL,
    )];
    client.operate(&wpolicy, &key, &ops).await.unwrap();
    let rec = client.get(&rpolicy, &key, Bins::from(["due"])).await.unwrap();
    println!("due after create-only write = {:?} (unchanged)", rec.bins.get("due"));

    // ---- Path expressions on a nested document (server 8.1.1+) ----
    let path_ok = client
        .random_node()
        .map(|n| n.version().supports_cdt_path_expressions())
        .unwrap_or(false);
    if path_ok {
        // Select every element under doc.items as a list value.
        let ctx = [ctx_map_key(as_val!("items")), aerospike::operations::cdt_context::ctx_all_children()];
        let ops = [read_exp(
            "items",
            exp::exp_select_by_path(exp::ExpType::List, SelectFlag::VALUE, exp::map_bin("doc"), &ctx),
            ExpReadFlags::DEFAULT,
        )];
        let rec = client.operate(&wpolicy, &key, &ops).await.unwrap();
        println!("doc.items[*] via path expression = {:?}", rec.bins.get("items"));
    } else {
        println!("Server does not support path expressions (requires 8.1.1+); skipping that part.");
    }

    // ---- The same expressions as a filter: only rush orders over 500 ----
    let mut rush_only = ReadPolicy::default();
    rush_only.base_policy.filter_expression = Some(exp::and(vec![
        exp::gt(exp::int_bin("due"), exp::int_val(500)),
        exp::eq(
            list_exp::get_by_value(
                ListReturnType::EXISTS,
                exp::string_val("rush"),
                exp::list_bin("tags"),
                &[],
            ),
            exp::bool_val(true),
        ),
    ]));
    let rec = client.get(&rush_only, &key, Bins::from(["due"])).await;
    println!(
        "passes `due > 500 and rush`: {}",
        rec.as_ref().map(|r| r.bins.get("due").is_some_and(|v| *v != Value::Nil)).unwrap_or(false)
    );

    let _ = client.delete(&wpolicy, &key).await;
    client.close().await.unwrap();
}
