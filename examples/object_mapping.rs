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

//! Object mapping: structs in, structs out.
//!
//! `#[derive(RecordMapper)]` from `aerospike_macro` maps a struct onto a
//! record. A struct with a `#[record(key)]` field is an *entity*: the key
//! field becomes the record's user key, every other field a bin, and the
//! derive implements `RecordMapper` (`to_bins`, `from_record`, `id`). A
//! struct without a key field is a *value*: it implements `ToValue` /
//! `FromValue` and is stored as a map, so it can nest inside an entity.
//!
//! Field attributes: `#[record(bin = "…")]` renames the bin,
//! `#[record(generation)]` receives the record generation on reads, and
//! `#[record(skip)]` leaves a field out (it is rebuilt with `Default`).
//!
//! With the `serialization` feature, any `serde` type can go the same way
//! through `aerospike::mapping::serde::{to_bins, from_bins, to_value,
//! from_value}`.
//!
//! ```bash
//! cargo run --example object_mapping --features serialization
//! ```

use std::env;

use aerospike::mapping::{FromValue, RecordMapper, ToValue};
use aerospike::{as_key, Bin, Bins, Client, ClientPolicy, Key, ReadPolicy, WritePolicy};
use aerospike_macro::RecordMapper;

const SET: &str = "mapping_demo";

/// A value type: no key, so it becomes a map inside another record.
#[derive(Debug, Clone, PartialEq, RecordMapper)]
#[record(crate = "aerospike")]
struct Address {
    street: String,
    city: String,
    #[record(bin = "zip")]
    postal_code: String,
}

/// An entity: `id` is the user key, the rest are bins.
#[derive(Debug, Clone, PartialEq, RecordMapper)]
#[record(crate = "aerospike")]
struct Customer {
    #[record(key)]
    id: i64,
    name: String,
    /// Bin names are limited to 15 bytes; this one is renamed to fit.
    #[record(bin = "loyalty_pts")]
    loyalty_points: u64,
    tags: Vec<String>,
    address: Option<Address>,
    /// Filled from the record on reads, never written.
    #[record(generation)]
    generation: u32,
    /// Derived at runtime, not stored.
    #[record(skip)]
    greeting: String,
}

/// A plain serde type, mapped through the `serialization` feature.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Invoice {
    number: String,
    amount_cents: i64,
    paid: bool,
}

/// The entity's bins as the client's `Bin` slice.
fn bins_of<T: RecordMapper>(entity: &T) -> Vec<Bin> {
    entity
        .to_bins()
        .expect("entity maps to bins")
        .into_iter()
        .map(|(name, value)| Bin::new(name, value))
        .collect()
}

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

    // Keep the user key with the record so `from_record` can recover it.
    let mut wpolicy = WritePolicy::default();
    wpolicy.send_key = true;
    let rpolicy = ReadPolicy::default();

    // ---- Entity: write a struct, read it back ----
    let ada = Customer {
        id: 42,
        name: "Ada Lovelace".into(),
        loyalty_points: 1_250,
        tags: vec!["founder".into(), "vip".into()],
        address: Some(Address {
            street: "12 St James's Square".into(),
            city: "London".into(),
            postal_code: "SW1Y 4JH".into(),
        }),
        generation: 0,
        greeting: String::new(),
    };
    // `id()` is the key the entity carries; build the full Key from it.
    let key = Key::new("test", SET, ada.id().unwrap()).unwrap();
    let _ = client.delete(&wpolicy, &key).await;
    client.put(&wpolicy, &key, &bins_of(&ada)).await.unwrap();
    println!("wrote bins {:?}", ada.to_bins().unwrap().keys().collect::<Vec<_>>());

    let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
    let mut back = Customer::from_record(&rec.bins, &key, rec.generation).unwrap();
    back.greeting = format!("Hello, {}!", back.name);
    println!("read back: {back:#?}");
    assert_eq!(back.id, ada.id);
    assert_eq!(back.address, ada.address);
    assert_eq!(back.generation, rec.generation);

    // ---- Value: the nested struct is an ordinary map value ----
    let value = ada.address.as_ref().unwrap().to_value().unwrap();
    println!("address as a Value: {value:?}");
    let address = Address::from_value(&value).unwrap();
    assert_eq!(Some(address), ada.address);

    // ---- Missing optional bins come back as None ----
    let key2 = as_key!("test", SET, 7);
    let _ = client.delete(&wpolicy, &key2).await;
    let nobody = Customer {
        id: 7,
        name: "Walk-in".into(),
        loyalty_points: 0,
        tags: vec![],
        address: None,
        generation: 0,
        greeting: String::new(),
    };
    client.put(&wpolicy, &key2, &bins_of(&nobody)).await.unwrap();
    let rec = client.get(&rpolicy, &key2, Bins::All).await.unwrap();
    let back = Customer::from_record(&rec.bins, &key2, rec.generation).unwrap();
    println!("walk-in customer has address {:?}", back.address);

    // ---- serde types through the `serialization` feature ----
    let invoice = Invoice { number: "INV-1001".into(), amount_cents: 19_900, paid: false };
    let bins = aerospike::mapping::serde::to_bins(&invoice).unwrap();
    let key3 = as_key!("test", SET, "INV-1001");
    let _ = client.delete(&wpolicy, &key3).await;
    client
        .put(
            &wpolicy,
            &key3,
            &bins.into_iter().map(|(n, v)| Bin::new(n, v)).collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    let rec = client.get(&rpolicy, &key3, Bins::All).await.unwrap();
    let back: Invoice = aerospike::mapping::serde::from_bins(&rec.bins).unwrap();
    println!("serde invoice round trip: {back:?}");
    assert_eq!(back, invoice);

    for k in [&key, &key2, &key3] {
        let _ = client.delete(&wpolicy, k).await;
    }
    client.close().await.unwrap();
}
