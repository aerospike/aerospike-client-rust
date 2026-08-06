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

//! What a **deeply nested** document costs jsonpath, and therefore what a packed cursor could recover.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test deepbench -- --ignored --nocapture
//! ```
//!
//! # Why this exists separately from `bench.rs`
//!
//! That benchmark filters 200,000 tiny records, and tiny records cannot show this: its whole root expansion costs
//! 265 ns per record, which at the ~36 ns per element that `as_val` materialisation costs leaves room for only a
//! handful of values. It measured the query machinery, not the tree.
//!
//! The document here is `map → list → map → list → map → list → map`, wide at every level. Walking it forces
//! `rec.get(bin)` to deserialise the entire nesting into one `as_val` per node before jsonpath sees any of it, so the
//! cost should scale with the node count — and if it does, that scaling *is* the prize a cursor would take.

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use aerospike_core::{as_key, as_val, Client, ClientPolicy, Key, Value, WritePolicy};

fn module() -> String {
    env::var("AEROSPIKE_JSONPATH_MODULE").unwrap_or_else(|_| "jsonpath".to_string())
}

fn ns() -> String {
    env::var("AEROSPIKE_NAMESPACE").unwrap_or_else(|_| "test".to_string())
}

async fn connect() -> Option<Client> {
    let asked = env::var("AEROSPIKE_HOSTS").is_ok();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| "127.0.0.1:3000".to_string());

    match Client::new(&ClientPolicy::default(), &hosts).await {
        Ok(c) => Some(c),
        Err(e) if asked => panic!("AEROSPIKE_HOSTS is set but unreachable: {e}"),
        Err(_) => None,
    }
}

/// Build `map → list → map → …` alternating for `levels` pairs, with `fan` children at each level.
///
/// Every map also carries `scalars` plain keys so the levels are wide rather than a bare chain, and the deepest map is
/// all scalars. Returns the value and the number of nodes in it, so the table below can report cost per node instead of
/// an unanchored total.
fn build(levels: u32, fan: usize, scalars: usize) -> (Value, u64) {
    let mut nodes = 0u64;

    fn maps(levels: u32, fan: usize, scalars: usize, nodes: &mut u64) -> Value {
        let mut m = HashMap::new();

        // The wide part: plain keys that a walk still has to look at.
        for s in 0..scalars {
            m.insert(Value::from(format!("s{s}")), Value::from(s as i64));
            *nodes += 2;
        }

        if levels == 0 {
            *nodes += 1;

            return Value::from(m);
        }

        // The deep part: one key holding a list, whose elements are the next map down.
        let mut items = Vec::with_capacity(fan);

        for _ in 0..fan {
            items.push(maps(levels - 1, fan, scalars, nodes));
        }

        *nodes += 1 + items.len() as u64;
        m.insert(Value::from("kids"), Value::from(items));
        *nodes += 2;

        Value::from(m)
    }

    let v = maps(levels, fan, scalars, &mut nodes);

    (v, nodes)
}

async fn udf(client: &Client, key: &Key, function: &str, args: &[Value]) -> Value {
    client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
        .unwrap_or_else(|e| panic!("{function}: {e}"))
        .unwrap_or(Value::Nil)
}

/// Best-of, so a slow round is something else on the box rather than the work.
async fn best_us(rounds: usize, reps: usize, client: &Client, key: &Key, path: &str) -> f64 {
    let mut best = f64::MAX;

    for _ in 0..rounds {
        let started = Instant::now();

        for _ in 0..reps {
            udf(client, key, "jsonpath", &[as_val!(path)]).await;
        }

        let per = started.elapsed().as_secs_f64() * 1e6 / reps as f64;

        if per < best {
            best = per;
        }
    }

    best
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn deep_document_bench() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping deep_document_bench: no server");
        return;
    };

    let ns = ns();
    let set = format!("jpdeep_{}", std::process::id());
    let write = WritePolicy::default();

    println!("\n=== one deep document: map -> list -> map -> ... , per query ===\n");
    println!(
        "  {:>6} {:>5} {:>5}  {:>9}  {:>13}  {:>13}  {:>12}",
        "levels", "fan", "keys", "nodes", "deep path", "shallow path", "ns/node"
    );

    // Increasing node counts at a fixed shape, so the trend is the signal: if the cost per query rises roughly
    // linearly with node count, that slope is the materialisation the cursor would remove.
    for (levels, fan, scalars) in [(3u32, 3usize, 10usize), (3, 4, 10), (3, 5, 12), (4, 4, 12)] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("d{levels}_{fan}_{scalars}").as_str());

        let _ = client.delete(&write, &key).await;

        let (doc, nodes) = build(levels, fan, scalars);

        client
            .put(
                &write,
                &key,
                &[aerospike_core::as_bin!("doc", doc)],
            )
            .await
            .expect("seed the document");

        // A path that descends the whole nesting, against one that touches only the root's keys. Both pay the root
        // expansion; only the first walks the tree.
        let deep = "$.doc.kids[*].kids[*].kids[*].s0";
        let shallow = "$.nosuchbin";

        let deep_us = best_us(3, 20, &client, &key, deep).await;
        let shallow_us = best_us(3, 20, &client, &key, shallow).await;

        println!(
            "  {levels:>6} {fan:>5} {scalars:>5}  {nodes:>9}  {deep_us:>10.1} us  {shallow_us:>10.1} us  {:>9.1}",
            deep_us * 1000.0 / nodes as f64
        );

        let _ = client.delete(&write, &key).await;
    }

    println!(
        "\n  Both columns pay the root expansion, which is where rec.get() deserialises the whole document into\n  \
         one as_val per node. If ns/node is roughly flat as the node count grows, the cost is proportional to the\n  \
         document and that proportion is what a packed cursor would take out.\n"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}
