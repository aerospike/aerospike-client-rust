// Copyright 2015-2020 Aerospike, Inc.
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

use crate::common;

use aerospike::operations::bitwise;
use aerospike::operations::bitwise::{BitPolicy, BitwiseOverflowActions};
use aerospike::operations::Operation;
use aerospike::{as_bin, as_blob, as_key, Client, Error, Key, Record, ResultCode, Value, WritePolicy};

async fn bitwise_write_then_read(
    client: &Client,
    wpolicy: &WritePolicy,
    key: &Key,
    write: Operation,
    read: Operation,
) -> Record {
    client.operate(wpolicy, key, &[write]).await.unwrap();
    client.operate(wpolicy, key, &[read]).await.unwrap()
}

#[aerospike_macro::test]
async fn cdt_bitwise() {
    let client = common::singleton_client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let seed_bytes = vec![
        0b00000001, 0b01000010, 0b00000011, 0b00000100, 0b00000101,
    ];
    let val = Value::Blob(seed_bytes.clone());
    let bpolicy = BitPolicy::default();

    let _ = common::delete_for_test_reset(client, &wpolicy, &key).await;
    let _ = common::delete_on_cluster(client, &wpolicy, &key).await;

    // `insert` prepends into an existing BYTES bin. On SC, `delete_for_test_reset` is a no-op, so
    // durable delete clears any stale record, then we seed the bitmap this test assumes.
    let bins = vec![as_bin!("bin", as_blob!(seed_bytes.clone()))];
    match client.put(&wpolicy, &key, &bins).await {
        Ok(()) => {}
        Err(Error::ServerError(ResultCode::ParameterError, _, _)) => {
            eprintln!(
                "cdt_bitwise: skipped — seed `put` returned ParameterError (cluster policy); \
                 needs a namespace that accepts blob bins on `put`"
            );
            return;
        }
        Err(e) => panic!("cdt_bitwise seed put: {e}"),
    }

    // Some servers reject BitWrite + BitRead in one operate; split each write+read pair.
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::insert("bin", 0, val, &bpolicy),
        bitwise::get("bin", 9, 5),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b10000000]));

    // Verify the Count command
    let ops = &vec![bitwise::count("bin", 20, 4)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(2));

    // Verify the set command
    let val = Value::Blob(vec![0b11100000]);
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::set("bin", 13, 3, val, &bpolicy),
        bitwise::get("bin", 0, 40),
    )
    .await;
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![
            0b00000001, 0b01000111, 0b00000011, 0b00000100, 0b00000101
        ])
    );

    // Verify Remove command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::remove("bin", 0, 1, &bpolicy),
        bitwise::get("bin", 0, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000111]));

    // Verify OR command
    let val = Value::Blob(vec![0b10101010]);
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::or("bin", 0, 8, val, &bpolicy),
        bitwise::get("bin", 0, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b11101111]));

    // Verify XOR command
    let val = Value::Blob(vec![0b10101100]);
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::xor("bin", 0, 8, val, &bpolicy),
        bitwise::get("bin", 0, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000011]));

    // Verify AND command
    let val = Value::Blob(vec![0b01011010]);
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::and("bin", 0, 8, val, &bpolicy),
        bitwise::get("bin", 0, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b01000010]));

    // Verify NOT command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::not("bin", 0, 8, &bpolicy),
        bitwise::get("bin", 0, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b10111101]));

    // Verify LSHIFT command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::lshift("bin", 24, 8, 3, &bpolicy),
        bitwise::get("bin", 24, 8),
    )
    .await;
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Blob(vec![0b00101000]));

    // Verify RSHIFT command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::rshift("bin", 0, 9, 1, &bpolicy),
        bitwise::get("bin", 0, 16),
    )
    .await;
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b10000011])
    );

    // Verify Add command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::add(
            "bin",
            0,
            8,
            128,
            false,
            BitwiseOverflowActions::Fail,
            &bpolicy,
        ),
        bitwise::get("bin", 0, 32),
    )
    .await;
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b11011110, 0b10000011, 0b00000100, 0b00101000])
    );

    // Verify Subtract command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::subtract(
            "bin",
            0,
            8,
            128,
            false,
            BitwiseOverflowActions::Fail,
            &bpolicy,
        ),
        bitwise::get("bin", 0, 32),
    )
    .await;
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b10000011, 0b00000100, 0b00101000])
    );

    // Verify the set int command
    let rec = bitwise_write_then_read(
        client,
        &wpolicy,
        &key,
        bitwise::set_int("bin", 8, 8, 255, &bpolicy),
        bitwise::get("bin", 0, 32),
    )
    .await;
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        Value::Blob(vec![0b01011110, 0b11111111, 0b00000100, 0b00101000])
    );

    // Verify the get int command
    let ops = &vec![bitwise::get_int("bin", 8, 8, false)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(255));

    // Verify the LSCAN command
    let ops = &vec![bitwise::lscan("bin", 19, 8, true)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(2));

    // Verify the RSCAN command
    let ops = &vec![bitwise::rscan("bin", 19, 8, true)];
    let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), Value::Int(7));
}
