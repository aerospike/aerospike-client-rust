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

//! What the immediate-op path is worth, measured against the staged path it replaces.
//!
//! ```sh
//! AEROSPIKE_HOSTS=… cargo test --release --test opbench -- --ignored --nocapture
//! ```
//!
//! Three A/B pairs, each running identical arithmetic and differing only in how it reaches the record:
//!
//! - **reading a blob** — `get_bytes` (the host materialises an as_val: malloc + memcpy of the whole value) against
//!   `with_bytes_ref` (a pointer into the stored particle).
//! - **incrementing an integer** — read, add, stage, commit against one immediate op.
//! - **appending to a blob** — read it all, copy, extend, stage, commit against one immediate op that never reads it.
//!
//! Every number is a round trip, so the ~65 µs the query and dispatch cost is in both sides of every pair and the
//! difference is the part that changed.

use std::env;
use std::time::Instant;

use aerospike_core::{
    as_bin, as_key, as_val, Bins, Client, ClientPolicy, Key, ReadPolicy, Value,
    WritePolicy,
};

const STEP: i64 = 60_000;

fn module() -> String {
    env::var("AEROSPIKE_TIMESERIES_MODULE").unwrap_or_else(|_| "timeseries".to_string())
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

async fn udf(client: &Client, key: &Key, function: &str, args: &[Value]) -> Value {
    client
        .execute_udf(&WritePolicy::default(), key, &module(), function, Some(args))
        .await
        .unwrap_or_else(|e| panic!("{function}: {e}"))
        .unwrap_or(Value::Nil)
}


/// A `map -> list -> map -> ...` document, wide at every level, with its node count.
fn build_tree(levels: u32, fan: usize, scalars: usize) -> (Value, u64) {
    fn maps(levels: u32, fan: usize, scalars: usize, nodes: &mut u64) -> Value {
        let mut m = std::collections::HashMap::new();

        for s in 0..scalars {
            m.insert(Value::from(format!("s{s}")), Value::from(s as i64));
            *nodes += 2;
        }

        if levels == 0 {
            *nodes += 1;

            return Value::from(m);
        }

        let mut items = Vec::with_capacity(fan);

        for _ in 0..fan {
            items.push(maps(levels - 1, fan, scalars, nodes));
        }

        *nodes += 2 + items.len() as u64;
        m.insert(Value::from("kids"), Value::from(items));

        Value::from(m)
    }

    let mut nodes = 0u64;
    let v = maps(levels, fan, scalars, &mut nodes);

    (v, nodes)
}

/// Best-of, because the thing being measured is the work and a slow round is something else on the box.
async fn best_us(
    rounds: usize,
    reps: usize,
    client: &Client,
    key: &Key,
    function: &str,
    args: &[Value],
) -> f64 {
    let mut best = f64::MAX;

    for _ in 0..rounds {
        let started = Instant::now();

        for _ in 0..reps {
            udf(client, key, function, args).await;
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
async fn opbench() {
    assert!(
        !cfg!(debug_assertions),
        "run this with --release, or the numbers describe the optimiser"
    );

    let Some(client) = connect().await else {
        eprintln!("skipping opbench: no server");
        return;
    };

    let ns = ns();
    let set = format!("opbench_{}", std::process::id());
    let write = WritePolicy::default();

    //==========================================================
    // Reading. `n` distinct blob bins per call, so one round trip covers `n` copies - see ts_probe_copy for why one
    // bin read repeatedly would measure nothing.
    println!("\n=== reading blob bins: as_val copy against a borrowed particle ===\n");
    println!(
        "  {:>5} x {:>8}  {:>10}  {:>13}  {:>15}  {:>7}",
        "bins", "bytes", "total", "get_bytes", "with_bytes_ref", "gain"
    );

    for (bins, each) in [(8i64, 8_192usize), (8, 65_536), (4, 131_072)] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("blobs{bins}x{each}").as_str());

        let _ = client.delete(&write, &key).await;

        // Blob content is NaN-free f64s, so `count_known` counts every slot and neither side can short-circuit.
        let mut payload = vec![0u8; each];

        for (i, chunk) in payload.chunks_exact_mut(8).enumerate() {
            chunk.copy_from_slice(&(i as f64).to_le_bytes());
        }

        for i in 0..bins {
            client
                .put(&write, &key, &[as_bin!(format!("b{i}").as_str(), payload.clone())])
                .await
                .expect("seed a blob bin");
        }

        let a = udf(&client, &key, "ts_probe_copy", &[as_val!("b"), as_val!(bins)]).await;
        let b = udf(&client, &key, "ts_probe_ref", &[as_val!("b"), as_val!(bins)]).await;

        assert_eq!(a, b, "the two reads must return the same count");

        let args = [as_val!("b"), as_val!(bins)];
        let copy = best_us(4, 50, &client, &key, "ts_probe_copy", &args).await;
        let borrow = best_us(4, 50, &client, &key, "ts_probe_ref", &args).await;

        println!(
            "  {bins:>5} x {each:>8}  {:>10}  {copy:>10.1} us  {borrow:>12.1} us  {:>6.2}x",
            bins as usize * each,
            copy / borrow.max(f64::MIN_POSITIVE)
        );

        let _ = client.delete(&write, &key).await;
    }

    //==========================================================
    // The allocation question: a list bin is deserialised into one as_val per element before a plugin sees anything.
    // The same values as a blob allocate nothing at all.
    println!("\n=== summing N values: a list bin against the same values as a blob ===\n");
    println!(
        "  {:>9}  {:>15}  {:>15}  {:>15}  {:>7}",
        "elements", "list (as_vals)", "list (cursor)", "blob (floor)", "gain"
    );

    for n in [100usize, 1_000, 10_000] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("cmp{n}").as_str());

        let _ = client.delete(&write, &key).await;

        let values: Vec<Value> = (0..n).map(|i| Value::from(i as f64)).collect();
        let mut packed = vec![0u8; n * 8];

        for (i, chunk) in packed.chunks_exact_mut(8).enumerate() {
            chunk.copy_from_slice(&(i as f64).to_le_bytes());
        }

        client
            .put(&write, &key, &[as_bin!("l", values), as_bin!("p", packed)])
            .await
            .expect("seed");

        let a = udf(&client, &key, "ts_probe_list_sum", &[as_val!("l")]).await;
        let b = udf(&client, &key, "ts_probe_blob_sum", &[as_val!("p")]).await;
        let c = udf(&client, &key, "ts_probe_list_cursor", &[as_val!("l")]).await;

        assert_eq!(a, b, "list and blob must sum to the same total");
        assert_eq!(a, c, "the cursor must sum to the same total as the as_val path");

        let list = best_us(3, 60, &client, &key, "ts_probe_list_sum", &[as_val!("l")]).await;
        let cursor = best_us(3, 60, &client, &key, "ts_probe_list_cursor", &[as_val!("l")]).await;
        let blob = best_us(3, 60, &client, &key, "ts_probe_blob_sum", &[as_val!("p")]).await;

        println!(
            "  {n:>9}  {list:>12.1} us  {cursor:>12.1} us  {blob:>12.1} us  {:>6.2}x",
            list / cursor.max(f64::MIN_POSITIVE)
        );

        let _ = client.delete(&write, &key).await;
    }

    //==========================================================
    // The same question for maps: look 10 keys up in a map of N entries.
    println!("\n=== looking 10 keys up in a map of N entries ===\n");
    println!(
        "  {:>9}  {:>15}  {:>15}  {:>7}",
        "entries", "map (as_vals)", "map (cursor)", "gain"
    );

    for n in [100usize, 1_000, 10_000] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("map{n}").as_str());

        let _ = client.delete(&write, &key).await;

        let mut entries = std::collections::HashMap::new();

        for i in 0..n {
            entries.insert(Value::from(format!("k{i}")), Value::from(i as i64));
        }

        client
            .put(&write, &key, &[as_bin!("m", entries)])
            .await
            .expect("seed the map");

        // The count has to match what the client wrote, or the order element is being mishandled and every key after
        // it would be read from the wrong offset.
        let len = udf(&client, &key, "ts_probe_map_len", &[as_val!("m")]).await;

        assert_eq!(len, Value::Int(n as i64), "cursor entry count must match the map written");

        let lookups = 10i64;
        let args = [as_val!("m"), as_val!(lookups)];
        let a = udf(&client, &key, "ts_probe_map_sum", &args).await;
        let b = udf(&client, &key, "ts_probe_map_cursor", &args).await;

        assert_eq!(a, b, "both lookups must find the same values");

        let asval = best_us(3, 40, &client, &key, "ts_probe_map_sum", &args).await;
        let cursor = best_us(3, 40, &client, &key, "ts_probe_map_cursor", &args).await;

        println!(
            "  {n:>9}  {asval:>12.1} us  {cursor:>12.1} us  {:>6.2}x",
            asval / cursor.max(f64::MIN_POSITIVE)
        );

        let _ = client.delete(&write, &key).await;
    }

    //==========================================================
    // How much of building a result is allocation. This is the prize an arena would be going after.
    println!("\n=== building an N-element list result ===\n");
    println!(
        "  {:>9}  {:>15}  {:>15}  {:>13}",
        "elements", "build + return", "loop only", "per element"
    );

    let bkey = as_key!(ns.as_str(), set.as_str(), "build");

    client
        .put(&write, &bkey, &[as_bin!("x", 1i64)])
        .await
        .expect("seed");

    for n in [100i64, 1_000, 10_000] {
        let built = best_us(3, 40, &client, &bkey, "ts_probe_build_list", &[as_val!(n)]).await;
        let bare = best_us(3, 40, &client, &bkey, "ts_probe_build_none", &[as_val!(n)]).await;

        println!(
            "  {n:>9}  {built:>12.1} us  {bare:>12.1} us  {:>10.1} ns",
            (built - bare) * 1000.0 / n as f64
        );
    }

    let _ = client.delete(&write, &bkey).await;

    //==========================================================
    println!("\n=== writing an N-element list to a bin ===\n");
    println!(
        "  {:>9}  {:>15}  {:>15}  {:>7}",
        "elements", "as_vals + set", "packed writer", "gain"
    );

    for n in [100i64, 1_000, 10_000] {
        let ak = as_key!(ns.as_str(), set.as_str(), format!("wa{n}").as_str());
        let pk2 = as_key!(ns.as_str(), set.as_str(), format!("wp{n}").as_str());

        // **Both records are seeded first**, because the immediate-op path requires one that already exists:
        // `udf_aerospike_rec_op` reports NOT_FOUND on an unopened record, where `set` + `write` would have created it.
        // That is a real gap in the op path rather than a quirk of this benchmark - noted, and seeding keeps the A/B
        // fair in the meantime.
        for k in [&ak, &pk2] {
            let _ = client.delete(&write, k).await;

            client
                .put(&write, k, &[as_bin!("seed", 1i64)])
                .await
                .expect("seed the record");
        }

        let args = [as_val!("l"), as_val!(n)];
        let a = udf(&client, &ak, "ts_probe_wlist_asval", &args).await;
        let b = udf(&client, &pk2, "ts_probe_wlist_packed", &args).await;

        assert_eq!(a, b, "both must report writing the same count");

        // And both must actually have written the same list.
        let ra = client.get(&ReadPolicy::default(), &ak, Bins::All).await.expect("get a");
        let rb = client.get(&ReadPolicy::default(), &pk2, Bins::All).await.expect("get b");

        assert_eq!(
            ra.bins.get("l"),
            rb.bins.get("l"),
            "the packed writer must produce the identical list"
        );

        let asval = best_us(3, 30, &client, &ak, "ts_probe_wlist_asval", &args).await;
        let packed = best_us(3, 30, &client, &pk2, "ts_probe_wlist_packed", &args).await;

        println!(
            "  {n:>9}  {asval:>12.1} us  {packed:>12.1} us  {:>6.2}x",
            asval / packed.max(f64::MIN_POSITIVE)
        );

        for k in [&ak, &pk2] {
            let _ = client.delete(&write, k).await;
        }
    }

    //==========================================================
    // The nested cursor against the as_val tree, on a document deep enough for the difference to be the deserialisation
    // rather than the round trip.
    println!("\n=== walking a nested document: map -> list -> map -> ... ===\n");
    println!(
        "  {:>6} {:>5}  {:>9}  {:>15}  {:>15}  {:>7}",
        "levels", "fan", "nodes", "as_val tree", "cursor", "gain"
    );

    for (levels, fan) in [(2u32, 4usize), (3, 4), (3, 6), (4, 4)] {
        let key = as_key!(ns.as_str(), set.as_str(), format!("tree{levels}_{fan}").as_str());

        let _ = client.delete(&write, &key).await;

        let (doc, nodes) = build_tree(levels, fan, 8);

        client
            .put(&write, &key, &[as_bin!("doc", doc)])
            .await
            .expect("seed the document");

        // The two walks must agree, or this is timing two different pieces of work.
        let a = udf(&client, &key, "ts_probe_tree_asval", &[as_val!("doc")]).await;
        let b = udf(&client, &key, "ts_probe_tree_cursor", &[as_val!("doc")]).await;

        assert_eq!(a, b, "the cursor walk must sum to the same total as the as_val walk");

        let asval = best_us(3, 20, &client, &key, "ts_probe_tree_asval", &[as_val!("doc")]).await;
        let cursor = best_us(3, 20, &client, &key, "ts_probe_tree_cursor", &[as_val!("doc")]).await;

        println!(
            "  {levels:>6} {fan:>5}  {nodes:>9}  {asval:>12.1} us  {cursor:>12.1} us  {:>6.2}x",
            asval / cursor.max(f64::MIN_POSITIVE)
        );

        let _ = client.delete(&write, &key).await;
    }

    //==========================================================
    println!("\n=== incrementing an integer bin ===\n");
    println!("  {:>28}  {:>13}", "how", "per call");

    let ikey = as_key!(ns.as_str(), set.as_str(), "counter");

    let _ = client.delete(&write, &ikey).await;

    client
        .put(&write, &ikey, &[as_bin!("n", 0i64)])
        .await
        .expect("seed");

    let staged = best_us(3, 200, &client, &ikey, "ts_probe_incr_staged", &[as_val!("n"), as_val!(1)]).await;
    let immediate = best_us(3, 200, &client, &ikey, "ts_probe_incr_op", &[as_val!("n"), as_val!(1)]).await;

    println!("  {:>28}  {staged:>10.1} us", "read, add, stage, commit");
    println!("  {:>28}  {immediate:>10.1} us", "one immediate op");
    println!(
        "\n  {:.2}x - and both are one round trip, so the difference is the as_val round trip only.\n",
        staged / immediate.max(f64::MIN_POSITIVE)
    );

    //==========================================================
    println!("=== appending 64 bytes to a blob bin ===\n");
    println!("  {:>11}  {:>13}  {:>13}  {:>7}", "blob bytes", "read+copy+set", "immediate op", "gain");

    for size in [8_192usize, 65_536, 524_288] {
        let tail = 64i64;

        for (label, function) in [("staged", "ts_probe_append_staged"), ("op", "ts_probe_append_op")] {
            let _ = label;
            let _ = function;
        }

        // A fresh blob per side, so neither pays for the other's growth.
        let sk = as_key!(ns.as_str(), set.as_str(), format!("blob_s{size}").as_str());
        let ok = as_key!(ns.as_str(), set.as_str(), format!("blob_o{size}").as_str());

        for k in [&sk, &ok] {
            let _ = client.delete(&write, k).await;

            client
                .put(&write, k, &[as_bin!("b", vec![0u8; size])])
                .await
                .expect("seed the blob");
        }

        let staged = best_us(2, 40, &client, &sk, "ts_probe_append_staged", &[as_val!("b"), as_val!(tail)]).await;
        let immediate = best_us(2, 40, &client, &ok, "ts_probe_append_op", &[as_val!("b"), as_val!(tail)]).await;

        println!(
            "  {size:>11}  {staged:>10.1} us  {immediate:>10.1} us  {:>6.2}x",
            staged / immediate.max(f64::MIN_POSITIVE)
        );

        let _ = client.delete(&write, &sk).await;
        let _ = client.delete(&write, &ok).await;
    }

    println!();

    let _ = client
        .truncate(&aerospike_core::AdminPolicy::default(), &ns, &set, 0)
        .await;
}
