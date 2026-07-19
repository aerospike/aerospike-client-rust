//! Geospatial queries with a geo2dsphere secondary index.
//!
//! Ports of the Java client's `QueryRegion`, `QueryRegionFilter` and
//! `QueryGeoCollection` examples: points-within-region, radius queries and
//! region-contains-point.

use std::env;

use aerospike::query::{Filter, PartitionFilter};
use aerospike::{as_bin, as_geo, as_key};
use aerospike::{
    AdminPolicy, Bins, Client, ClientPolicy, CollectionIndexType, IndexType, QueryPolicy,
    Statement, Task, WritePolicy,
};
use futures::StreamExt;

const SET: &str = "geo_demo";
const BIN: &str = "loc";

#[tokio::main]
async fn main() {
    run().await;
}

async fn count_matching(client: &Client, filter: Filter) -> usize {
    let mut stmt = Statement::new("test", SET, Bins::All);
    stmt.add_filter(filter);
    client
        .query(&QueryPolicy::default(), PartitionFilter::all(), stmt)
        .await
        .unwrap()
        .into_stream()
        .filter(|r| futures::future::ready(r.is_ok()))
        .count()
        .await
}

/// Example body. Standalone via `cargo run --example`, and also driven by
/// the integration test suite (`tests/src/examples.rs`).
pub async fn run() {
    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS").unwrap_or_else(|_| String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .await
        .expect("Failed to connect to cluster");

    let apolicy = AdminPolicy::default();
    let wpolicy = WritePolicy::default();

    // ---- Create the geo2dsphere index ----
    let index_name = "geo_demo_loc_idx";
    let _ = client.drop_index(&apolicy, "test", SET, index_name).await;
    let task = client
        .create_index_on_bin(
            &apolicy,
            "test",
            SET,
            BIN,
            index_name,
            IndexType::Geo2DSphere,
            CollectionIndexType::Default,
            None,
        )
        .await
        .expect("Failed to create geo index");
    task.wait_till_complete(None).await.unwrap();

    // ---- Store a few points (GeoJSON values) ----
    let points = [
        ("sf-office", -122.40, 37.79),
        ("oakland", -122.27, 37.80),
        ("san-jose", -121.89, 37.34),
        ("la", -118.24, 34.05), // far away
    ];
    for (name, lng, lat) in points {
        let key = as_key!("test", SET, name);
        let geo = format!(r#"{{"type": "Point", "coordinates": [{lng}, {lat}]}}"#);
        client
            .put(&wpolicy, &key, &[as_bin!(BIN, as_geo!(&geo))])
            .await
            .unwrap();
    }

    // ---- Points within a polygon (Java QueryRegion) ----
    let bay_area = r#"{
        "type": "Polygon",
        "coordinates": [[[-123.0, 37.0], [-121.5, 37.0],
                         [-121.5, 38.2], [-123.0, 38.2], [-123.0, 37.0]]]
    }"#;
    let n = count_matching(&client, Filter::geo_within_region(BIN, bay_area)).await;
    println!("points within Bay Area polygon: {n}");
    assert_eq!(n, 3); // sf-office + oakland + san-jose (only LA is outside)

    // ---- Points within a radius (Java QueryRegionFilter-style) ----
    // 50km around downtown San Jose.
    let n = count_matching(
        &client,
        Filter::geo_within_radius(BIN, -121.89, 37.33, 50_000.0),
    )
    .await;
    println!("points within 50km of San Jose: {n}");

    // ---- Regions containing a point (Java QueryGeoCollection inverse) ----
    // Store a region record, then find which stored regions contain a point.
    let region_key = as_key!("test", SET, "bay-area-region");
    client
        .put(&wpolicy, &region_key, &[as_bin!(BIN, as_geo!(bay_area))])
        .await
        .unwrap();
    let n = count_matching(
        &client,
        Filter::geo_contains(BIN, r#"{"type": "Point", "coordinates": [-122.40, 37.79]}"#),
    )
    .await;
    println!("stored regions containing SF office: {n}");
    assert_eq!(n, 1);

    // ---- Cleanup ----
    for (name, ..) in points {
        let _ = client.delete(&wpolicy, &as_key!("test", SET, name)).await;
    }
    let _ = client.delete(&wpolicy, &region_key).await;
    let _ = client.drop_index(&apolicy, "test", SET, index_name).await;
    client.close().await.unwrap();
}
