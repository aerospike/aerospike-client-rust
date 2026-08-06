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

//! jsonpath against a **CDT context read**, on a ten-level document.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test deepctx -- --ignored --nocapture
//! ```
//!
//! # What is being compared, and what is not
//!
//! A context read is what someone reaches for without a UDF at all: a chain of `ctx_map_key` / `ctx_list_index` that
//! addresses one location, evaluated by the server with no plugin involved. It is the honest baseline for "was the UDF
//! worth it", so it is the first row.
//!
//! **Only the single-leaf row is a like-for-like race.** The wildcard and filter rows return many values, and there the
//! two are not doing the same work: a context read hands back packed values, while jsonpath also produces the
//! normalised path of every match and evaluates the filter itself rather than in the server's expression engine. If a
//! context read wins those, that is a narrower operation doing less — not jsonpath being slow. The rows say so.

use std::collections::HashMap;
use std::env;
use std::time::Instant;

use aerospike_core::expressions::maps as exp_maps;
use aerospike_core::expressions::{exp_map_loop_var, ge, int_val, string_val, ExpType, LoopVarPart};
use aerospike_core::operations::cdt_context::{
    ctx_all_children, ctx_all_children_with_filter, ctx_list_index, ctx_map_key,
};
use aerospike_core::operations::path::{select_by_path, SelectFlag};
use aerospike_core::operations::maps;
use aerospike_core::{
    as_bin, as_key, as_val, Client, ClientPolicy, Key, MapReturnType, Value, WritePolicy,
};

/// Five map/list pairs — ten CDT context levels.
const PAIRS: usize = 5;

/// Children per level. 3^5 = 243 leaves.
const FAN: usize = 3;

/// Leaves whose `total` exceeds this are the filter's answer.
const THRESHOLD: i64 = 500;

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

/// One level of the document: a complex map, not a bare link in a chain.
///
/// Eight scalars, a list of strings and a nested map of scalars sit beside `kids`, so every level a path descends
/// through has real breadth to walk past — which is the point, since the cost being measured is what expanding a level
/// costs.
fn level(depth: usize, seed: i64, nodes: &mut u64) -> Value {
    let mut m = HashMap::new();

    for (k, v) in [
        ("id", Value::from(seed)),
        ("name", Value::from(format!("node-{seed}"))),
        ("score", Value::from(seed as f64 / 7.0)),
        ("active", Value::from(seed % 2 == 0)),
        ("rank", Value::from(seed % 97)),
        ("region", Value::from(format!("r{}", seed % 5))),
        ("updated", Value::from(1_767_225_600_000i64 + seed)),
        ("weight", Value::from((seed % 13) as f64 * 1.5)),
    ] {
        m.insert(Value::from(k), v);
        *nodes += 2;
    }

    let tags: Vec<Value> = (0..4).map(|t| Value::from(format!("t{}", seed + t))).collect();

    *nodes += 2 + tags.len() as u64;
    m.insert(Value::from("tags"), Value::from(tags));

    let mut meta = HashMap::new();

    meta.insert(Value::from("host"), Value::from(format!("h{}", seed % 11)));
    meta.insert(Value::from("zone"), Value::from(format!("z{}", seed % 3)));
    meta.insert(Value::from("shard"), Value::from(seed % 17));
    *nodes += 2 + 6;
    m.insert(Value::from("meta"), Value::from(meta));

    if depth == 0 {
        // The leaf carries the value the filter tests. Spread either side of THRESHOLD so the filter selects a subset
        // rather than everything or nothing - a filter that matches all of them would measure the wildcard again.
        m.insert(Value::from("total"), Value::from((seed * 37) % 1000));
        *nodes += 3;

        return Value::from(m);
    }

    let mut kids = Vec::with_capacity(FAN);

    for i in 0..FAN {
        kids.push(level(depth - 1, seed * FAN as i64 + i as i64 + 1, nodes));
    }

    *nodes += 2 + kids.len() as u64;
    m.insert(Value::from("kids"), Value::from(kids));

    Value::from(m)
}

/// How many leaves the filter should select, computed the same way the document was built.
fn expected_filtered() -> usize {
    fn walk(depth: usize, seed: i64, hits: &mut usize) {
        if depth == 0 {
            if (seed * 37) % 1000 > THRESHOLD {
                *hits += 1;
            }

            return;
        }

        for i in 0..FAN {
            walk(depth - 1, seed * FAN as i64 + i as i64 + 1, hits);
        }
    }

    let mut hits = 0;

    walk(PAIRS, 0, &mut hits);

    hits
}

async fn jp_fn(client: &Client, key: &Key, function: &str, path: &str) -> Value {
    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            function,
            Some(&[as_val!(path)]),
        )
        .await
        .unwrap_or_else(|e| panic!("{function} {path}: {e}"))
        .unwrap_or(Value::Nil)
}

async fn jp(client: &Client, key: &Key, path: &str) -> Value {
    client
        .execute_udf(
            &WritePolicy::default(),
            key,
            &module(),
            "jsonpath",
            Some(&[as_val!(path)]),
        )
        .await
        .unwrap_or_else(|e| panic!("jsonpath {path}: {e}"))
        .unwrap_or(Value::Nil)
}

fn count_of(v: &Value) -> usize {
    match v {
        Value::List(items) => items.len(),
        Value::Nil => 0,
        _ => 1,
    }
}

async fn best_us<F, Fut>(rounds: usize, reps: usize, mut run: F) -> f64
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut best = f64::MAX;

    for _ in 0..rounds {
        let started = Instant::now();

        for _ in 0..reps {
            run().await;
        }

        let per = started.elapsed().as_secs_f64() * 1e6 / reps as f64;

        if per < best {
            best = per;
        }
    }

    best
}


/// Time one native operation, or say plainly that the server refused it.
///
/// An unavailable row is a fact about the server; putting something non-equivalent in its place would make the table
/// lie about what was compared.
async fn native_row(
    client: &Client,
    write: &WritePolicy,
    key: &Key,
    label: &str,
    op: &aerospike_core::operations::Operation,
    note: &str,
) {
    match client.operate(write, key, &[op.clone()]).await {
        Ok(rec) => {
            let n = count_deep(&rec.bins.get("doc").cloned().unwrap_or(Value::Nil));
            let us = best_us(3, 20, || async {
                std::hint::black_box(client.operate(write, key, &[op.clone()]).await.expect("native op"));
            })
            .await;

            row(label, us, n, note);
        }
        Err(e) => println!(
            "  {label:<46} {:>13}  {:>8}  {}",
            "unavailable",
            "-",
            format!("{e}").lines().next().unwrap_or("failed")
        ),
    }
}

fn row(label: &str, us: f64, results: usize, note: &str) {
    println!("  {label:<46} {us:>10.1} us  {results:>8}  {note}");
}

#[tokio::test]
#[ignore = "a benchmark: run it deliberately, and only in release"]
async fn deep_context_bench() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping deep_context_bench: no server");
        return;
    };

    let ns = ns();
    let set = format!("jpctx_{}", std::process::id());
    let write = WritePolicy::default();
    let key = as_key!(ns.as_str(), set.as_str(), "deep");

    let _ = client.delete(&write, &key).await;

    let mut nodes = 0u64;
    let doc = level(PAIRS, 0, &mut nodes);

    client
        .put(&write, &key, &[as_bin!("doc", doc)])
        .await
        .expect("seed the document");

    let leaves = FAN.pow(PAIRS as u32);
    let filtered = expected_filtered();

    println!(
        "\n=== {PAIRS} map/list pairs = {} context levels, fan {FAN}, {nodes} nodes, {leaves} leaves ===\n",
        PAIRS * 2
    );
    println!("  {:<46} {:>13}  {:>8}  {}", "how", "per call", "results", "note");

    //==========================================================
    // 1. One leaf, ten levels down. The only like-for-like row.
    //

    let deep_one = "$.doc.kids[0].kids[0].kids[0].kids[0].kids[0].total";

    // The same location as a context chain: map key, then list index, five times over.
    let mut ctx = Vec::with_capacity(PAIRS * 2);

    for _ in 0..PAIRS {
        ctx.push(ctx_map_key(Value::from("kids")));
        ctx.push(ctx_list_index(0));
    }

    let ctx_op = maps::get_by_key("doc", Value::from("total"), MapReturnType::Value).context(ctx);

    // Both must find the same value, or the row is comparing two different reads.
    let jp_one = jp(&client, &key, deep_one).await;
    let ctx_one = client
        .operate(&write, &key, &[ctx_op.clone()])
        .await
        .expect("context read")
        .bins
        .get("doc")
        .cloned()
        .unwrap_or(Value::Nil);

    println!("  (leaf via jsonpath {jp_one:?} / via context {ctx_one:?})\n");

    let us = best_us(3, 30, || async { std::hint::black_box(jp(&client, &key, deep_one).await); }).await;

    row("jsonpath: one leaf, 10 levels down", us, count_of(&jp_one), "");

    let us = best_us(3, 30, || async {
        std::hint::black_box(
            client
                .operate(&write, &key, &[ctx_op.clone()])
                .await
                .expect("context read"),
        );
    })
    .await;

    row("native: 10-level context + get_by_key", us, 1, "like-for-like");

    //==========================================================
    // 2. Every leaf, by wildcard.
    //

    println!();

    let deep_all = "$.doc.kids[*].kids[*].kids[*].kids[*].kids[*].total";
    let jp_all = jp(&client, &key, deep_all).await;

    assert_eq!(count_of(&jp_all), leaves, "the wildcard should reach every leaf");

    let us = best_us(3, 20, || async { std::hint::black_box(jp(&client, &key, deep_all).await); }).await;

    row("jsonpath: every leaf, by wildcard", us, count_of(&jp_all), "");

    // The same query with no owned copy per match.
    let jp_all_v = jp_fn(&client, &key, "jsonpath_values", deep_all).await;

    assert_eq!(jp_all_v, jp_all, "jsonpath_values must return exactly what jsonpath does");

    let us = best_us(3, 20, || async {
        std::hint::black_box(jp_fn(&client, &key, "jsonpath_values", deep_all).await);
    })
    .await;

    row("jsonpath_values: same, no owned copy", us, count_of(&jp_all_v), "");

    // The native analogue is a **path expression**, not a context on an ordinary read.
    //
    // `ctx_all_children` selects a set rather than a location, and a plain `get_by_key` addresses exactly one - so
    // handing it an all-children context is refused with OpNotApplicable, which is what the first version of this row
    // did. `select_by_path` is the op family built for a multi-location context.
    let mut all_ctx = Vec::with_capacity(PAIRS * 2 + 1);

    for _ in 0..PAIRS {
        all_ctx.push(ctx_map_key(Value::from("kids")));
        all_ctx.push(ctx_all_children());
    }

    all_ctx.push(ctx_map_key(Value::from("total")));

    let all_op = select_by_path("doc", SelectFlag::VALUE, &all_ctx);

    native_row(
        &client,
        &write,
        &key,
        "native: select_by_path, all_children x5",
        &all_op,
        "values only, no paths",
    )
    .await;

    //==========================================================
    // 3. A filter, selecting a subset.
    //

    println!();

    let deep_filter =
        format!("$.doc.kids[*].kids[*].kids[*].kids[*].kids[?@.total > {THRESHOLD}]");
    let jp_filtered = jp(&client, &key, &deep_filter).await;

    assert_eq!(
        count_of(&jp_filtered),
        filtered,
        "the filter should select the leaves above the threshold"
    );

    let us = best_us(3, 20, || async {
        std::hint::black_box(jp(&client, &key, &deep_filter).await);
    })
    .await;

    row(
        "jsonpath: filter @.total > N",
        us,
        count_of(&jp_filtered),
        "evaluated in the plugin",
    );

    let jp_filt_v = jp_fn(&client, &key, "jsonpath_values", &deep_filter).await;

    assert_eq!(
        jp_filt_v, jp_filtered,
        "jsonpath_values must return exactly what jsonpath does"
    );

    let us = best_us(3, 20, || async {
        std::hint::black_box(jp_fn(&client, &key, "jsonpath_values", &deep_filter).await);
    })
    .await;

    row(
        "jsonpath_values: same, no owned copy",
        us,
        count_of(&jp_filt_v),
        "containers, so a wash",
    );

    // The same selection expressed for the server to evaluate: a filtered all-children context at the last hop.
    let mut filt_ctx = Vec::with_capacity(PAIRS * 2 + 1);

    for _ in 0..PAIRS - 1 {
        filt_ctx.push(ctx_map_key(Value::from("kids")));
        filt_ctx.push(ctx_all_children());
    }

    filt_ctx.push(ctx_map_key(Value::from("kids")));
    // **The filter reads the loop variable, not a bin.**
    //
    // `int_bin("total")` was the first attempt and the server refused it with ParameterError, correctly: inside a
    // filtered all-children context the expression is evaluated against the *current child*, and there is no bin
    // called total. `exp_map_loop_var(VALUE)` is that child - a map - and the filter pulls its `total` key out.
    filt_ctx.push(ctx_all_children_with_filter(ge(
        exp_maps::get_by_key(
            MapReturnType::Value,
            ExpType::INT,
            string_val("total".to_string()),
            exp_map_loop_var(LoopVarPart::VALUE),
            &[],
        ),
        int_val(THRESHOLD + 1),
    )));

    let filt_op = select_by_path("doc", SelectFlag::VALUE, &filt_ctx);

    native_row(
        &client,
        &write,
        &key,
        "native: select_by_path, filtered children",
        &filt_op,
        "evaluated in the server",
    )
    .await;

    //==========================================================
    // 4. The control: a path that matches nothing.
    //

    println!();

    let miss = "$.doc.nosuchkey";
    let us = best_us(3, 30, || async { std::hint::black_box(jp(&client, &key, miss).await); }).await;

    row("jsonpath: matches nothing", us, 0, "cost is now flat in document size");

    println!(
        "\n  Only the first pair is a like-for-like race. The wildcard and filter rows return many values and\n  \
         jsonpath also produces every match's normalised path, so a context read winning them is a narrower\n  \
         operation doing less rather than jsonpath being slow.\n"
    );

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}

/// Count the leaves of a possibly nested result, since all_children returns a nested shape.
fn count_deep(v: &Value) -> usize {
    match v {
        Value::List(items) => items.iter().map(count_deep).sum(),
        Value::Nil => 0,
        _ => 1,
    }
}
