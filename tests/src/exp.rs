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
use env_logger;

use aerospike::expressions::*;
use aerospike::ParticleType;
use aerospike::*;
use std::sync::Arc;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", i);
        let sbin = as_bin!("bin2", format!("{}", i));
        let fbin = as_bin!("bin3", i as f64 / 3 as f64);
        let str = format!("{}{}", "blob", i);
        let bbin = as_bin!("bin4", str.as_bytes());
        let lbin = as_bin!("bin5", as_list!("a", "b", i));
        let mbin = as_bin!("bin6", as_map!("a" => "test", "b" => i));
        let bins = vec![ibin, sbin, fbin, bbin, lbin, mbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }
    set_name
}

#[aerospike_macro::test]
async fn expression_compare() {
    let client = common::client().await;
    let _ = env_logger::try_init();

    let set_name = create_test_set(&client, EXPECTED).await;

    // EQ
    let rs = test_filter(
        &client,
        eq(int_bin("bin".to_string()), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "EQ Test Failed");

    // NE
    let rs = test_filter(
        &client,
        ne(int_bin("bin".to_string()), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "NE Test Failed");

    // LT
    let rs = test_filter(
        &client,
        lt(int_bin("bin".to_string()), int_val(10)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 10, "LT Test Failed");

    // LE
    let rs = test_filter(
        &client,
        le(int_bin("bin".to_string()), int_val(10)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 11, "LE Test Failed");

    // GT
    let rs = test_filter(
        &client,
        gt(int_bin("bin".to_string()), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "GT Test Failed");

    // GE
    let rs = test_filter(
        &client,
        ge(int_bin("bin".to_string()), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "GT Test Failed");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn expression_condition() {
    let _ = env_logger::try_init();
    let client = common::client().await;

    let set_name = create_test_set(&client, EXPECTED).await;

    // AND
    let rs = test_filter(
        &client,
        and(vec![
            eq(int_bin("bin".to_string()), int_val(1)),
            eq(string_bin("bin2".to_string()), string_val("1".to_string())),
        ]),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "AND Test Failed");

    // OR
    let rs = test_filter(
        &client,
        or(vec![
            eq(int_bin("bin".to_string()), int_val(1)),
            eq(int_bin("bin".to_string()), int_val(3)),
        ]),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 2, "OR Test Failed");

    // NOT
    let rs = test_filter(
        &client,
        not(eq(int_bin("bin".to_string()), int_val(1))),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "NOT Test Failed");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn expression_data_types() {
    let client = common::client().await;
    let _ = env_logger::try_init();

    let set_name = create_test_set(&client, EXPECTED).await;

    // INT
    let rs = test_filter(
        &client,
        eq(int_bin("bin".to_string()), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "INT Test Failed");

    // STRING
    let rs = test_filter(
        &client,
        eq(string_bin("bin2".to_string()), string_val("1".to_string())),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "STRING Test Failed");

    let rs = test_filter(
        &client,
        eq(float_bin("bin3".to_string()), float_val(2f64)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "FLOAT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            blob_bin("bin4".to_string()),
            blob_val(format!("{}{}", "blob", 5).into_bytes()),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "BLOB Test Failed");

    let rs = test_filter(
        &client,
        ne(
            bin_type("bin".to_string()),
            int_val(ParticleType::NULL as i64),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "BIN TYPE Test Failed");
    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn expression_aero_5_6() {
    let client = common::client().await;
    let _ = env_logger::try_init();

    let set_name = create_test_set(&client, EXPECTED).await;

    let rs = test_filter(
        &client,
        eq(
            num_add(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(20),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_ADD Test Failed");

    let rs = test_filter(
        &client,
        eq(
            num_sub(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(20),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_SUB Test Failed");

    let rs = test_filter(
        &client,
        eq(
            num_mul(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(20),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_MUL Test Failed");

    let rs = test_filter(
        &client,
        gt(
            num_div(vec![int_bin("bin".to_string()), int_val(5)]),
            int_val(10),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 45, "NUM_DIV Test Failed");

    let rs = test_filter(
        &client,
        eq(
            num_pow(float_bin("bin3".to_string()), float_val(2.0)),
            float_val(4.0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_POW Test Failed");

    let rs = test_filter(
        &client,
        eq(
            num_log(float_bin("bin3".to_string()), float_val(2.0)),
            float_val(4.0),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_LOG Test Failed");

    let rs = test_filter(
        &client,
        eq(num_mod(int_bin("bin".to_string()), int_val(10)), int_val(0)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 10, "NUM_MOD Test Failed");

    let rs = test_filter(
        &client,
        eq(num_abs(int_bin("bin".to_string())), int_val(1)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "NUM_ABS Test Failed");

    let rs = test_filter(
        &client,
        eq(num_floor(float_bin("bin3".to_string())), float_val(2.0)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 3, "NUM_FLOOR Test Failed");

    let rs = test_filter(
        &client,
        eq(num_ceil(float_bin("bin3".to_string())), float_val(2.0)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 3, "NUM_CEIL Test Failed");

    let rs = test_filter(
        &client,
        eq(to_int(float_bin("bin3".to_string())), int_val(2)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 3, "TO_INT Test Failed");

    let rs = test_filter(
        &client,
        eq(to_float(int_bin("bin".to_string())), float_val(2.0)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "TO_FLOAT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            int_and(vec![int_bin("bin".to_string()), int_val(0xff)]),
            int_val(0x11),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "INT_AND Test Failed");

    let rs = test_filter(
        &client,
        eq(
            int_xor(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(16),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "INT_XOR Test Failed");

    let rs = test_filter(
        &client,
        eq(int_not(int_bin("bin".to_string())), int_val(-50)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "INT_NOT Test Failed");

    let rs = test_filter(
        &client,
        gt(
            int_lshift(int_bin("bin".to_string()), int_val(8)),
            int_val(0xff),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "INT_LSHIFT Test Failed");

    let rs = test_filter(
        &client,
        gt(
            int_rshift(int_bin("bin".to_string()), int_val(1)),
            int_val(0x2a),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 14, "INT_RSHIFT Test Failed");

    let rs = test_filter(
        &client,
        gt(
            int_arshift(int_bin("bin".to_string()), int_val(1)),
            int_val(0x2a),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 14, "INT_ARSHIFT Test Failed");

    let rs = test_filter(
        &client,
        eq(int_count(int_bin("bin".to_string())), int_val(3)),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 32, "INT_COUNT Test Failed");

    let rs = test_filter(
        &client,
        gt(
            int_lscan(int_bin("bin".to_string()), bool_val(true)),
            int_val(60),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 7, "INT_LSCAN Test Failed");

    let rs = test_filter(
        &client,
        gt(
            int_rscan(int_bin("bin".to_string()), bool_val(true)),
            int_val(60),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 87, "INT_RSCAN Test Failed");

    let rs = test_filter(
        &client,
        eq(
            min(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(10),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 90, "MIN Test Failed");

    let rs = test_filter(
        &client,
        eq(
            max(vec![int_bin("bin".to_string()), int_val(10)]),
            int_val(10),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 11, "MAX Test Failed");

    let rs = test_filter(
        &client,
        gt(
            cond(vec![
                eq(num_mod(int_bin("bin".to_string()), int_val(2)), int_val(0)),
                num_add(vec![int_bin("bin".to_string()), int_val(100)]),
                gt(num_mod(int_bin("bin".to_string()), int_val(2)), int_val(0)),
                num_add(vec![int_bin("bin".to_string()), int_val(10)]),
                int_val(-1),
            ]),
            int_val(100),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 54, "COND Test Failed");

    let rs = test_filter(
        &client,
        exp_let(vec![
            def("x".to_string(), int_bin("bin".to_string())),
            and(vec![
                lt(int_val(5), var("x".to_string())),
                lt(var("x".to_string()), int_val(10)),
            ]),
        ]),
        &set_name,
    )
    .await;

    let count = count_results(rs);
    assert_eq!(count, 4, "LET/DEF/VAR Test Failed");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
fn expression_rec_ops() {
    let client = common::client().await;
    let _ = env_logger::try_init();

    let set_name = create_test_set(&client, EXPECTED).await;

    let rs = test_filter(&client, le(device_size(), int_val(0)), &set_name).await;
    let mut count = count_results(rs);
    if count == 0 {
        // Not in-memory
        let rs = test_filter(&client, le(device_size(), int_val(2000)), &set_name).await;
        count = count_results(rs);
    }
    assert_eq!(count, 100, "DEVICE SIZE Test Failed");

    let rs = test_filter(&client, gt(last_update(), int_val(15000)), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "LAST UPDATE Test Failed");

    let rs = test_filter(&client, gt(since_update(), int_val(10)), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "SINCE UPDATE Test Failed");

    // Records dont expire
    let rs = test_filter(&client, ge(void_time(), int_val(0)), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "VOID TIME Test Failed");

    let rs = test_filter(&client, ge(ttl(), int_val(0)), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "TTL Test Failed");

    let rs = test_filter(&client, not(is_tombstone()), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "TOMBSTONE Test Failed");

    let rs = test_filter(
        &client,
        eq(expressions::set_name(), string_val(set_name.clone())),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "SET NAME Test Failed");

    let rs = test_filter(&client, bin_exists("bin4".to_string()), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "BIN EXISTS Test Failed");

    let rs = test_filter(&client, eq(digest_modulo(3), int_val(1)), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count > 0 && count < 100, true, "DIGEST MODULO Test Failed");

    let rs = test_filter(&client, eq(key(ExpType::INT), int_val(50)), &set_name).await;
    let count = count_results(rs);
    // 0 because key is not saved
    assert_eq!(count, 0, "KEY Test Failed");

    let rs = test_filter(&client, key_exists(), &set_name).await;
    let count = count_results(rs);
    // 0 because key is not saved
    assert_eq!(count, 0, "KEY EXISTS Test Failed");

    let rs = test_filter(&client, eq(nil(), nil()), &set_name).await;
    let count = count_results(rs);
    assert_eq!(count, 100, "NIL Test Failed");

    let rs = test_filter(
        &client,
        regex_compare(
            "[1-5]".to_string(),
            RegexFlag::ICASE as i64,
            string_bin("bin2".to_string()),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 75, "REGEX Test Failed");

    client.close().await.unwrap();
}

#[aerospike_macro::test]
async fn expression_commands() {
    let _ = env_logger::try_init();

    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..EXPECTED as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", i);

        let bins = vec![ibin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }
    let mut wpolicy = WritePolicy::default();
    let mut rpolicy = ReadPolicy::default();
    let mut spolicy = ScanPolicy::default();
    let mut bpolicy = BatchPolicy::default();

    // DELETE
    let key = as_key!(namespace, &set_name, 15);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(16)));
    let test = client.delete(&wpolicy, &key).await;
    assert_eq!(test.is_err(), true, "DELETE EXP Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client.delete(&wpolicy, &key).await;
    assert_eq!(test.is_ok(), true, "DELETE EXP Ok Test Failed");

    // PUT
    let key = as_key!(namespace, &set_name, 25);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client.put(&wpolicy, &key, &[as_bin!("bin", 26)]).await;
    assert_eq!(test.is_err(), true, "PUT Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(25)));
    let test = client.put(&wpolicy, &key, &[as_bin!("bin", 26)]).await;
    assert_eq!(test.is_ok(), true, "PUT Ok Test Failed");

    // GET
    let key = as_key!(namespace, &set_name, 35);
    rpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client.get(&rpolicy, &key, Bins::All).await;
    assert_eq!(test.is_err(), true, "GET Err Test Failed");

    rpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(35)));
    let test = client.get(&rpolicy, &key, Bins::All).await;
    assert_eq!(test.is_ok(), true, "GET Ok Test Failed");

    // EXISTS
    let key = as_key!(namespace, &set_name, 45);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client.exists(&wpolicy, &key).await;
    assert_eq!(test.is_err(), true, "EXISTS Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(45)));
    let test = client.exists(&wpolicy, &key).await;
    assert_eq!(test.is_ok(), true, "EXISTS Ok Test Failed");

    // APPEND
    let key = as_key!(namespace, &set_name, 55);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client
        .add(&wpolicy, &key, &[as_bin!("test55", "test")])
        .await;
    assert_eq!(test.is_err(), true, "APPEND Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(55)));
    let test = client
        .add(&wpolicy, &key, &[as_bin!("test55", "test")])
        .await;
    assert_eq!(test.is_ok(), true, "APPEND Ok Test Failed");

    // PREPEND
    let key = as_key!(namespace, &set_name, 55);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client
        .prepend(&wpolicy, &key, &[as_bin!("test55", "test")])
        .await;
    assert_eq!(test.is_err(), true, "PREPEND Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(55)));
    let test = client
        .prepend(&wpolicy, &key, &[as_bin!("test55", "test")])
        .await;
    assert_eq!(test.is_ok(), true, "PREPEND Ok Test Failed");

    // TOUCH
    let key = as_key!(namespace, &set_name, 65);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let test = client.touch(&wpolicy, &key).await;
    assert_eq!(test.is_err(), true, "TOUCH Err Test Failed");

    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(65)));
    let test = client.touch(&wpolicy, &key).await;
    assert_eq!(test.is_ok(), true, "TOUCH Ok Test Failed");

    // SCAN
    spolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(75)));
    match client.scan(&spolicy, namespace, &set_name, Bins::All).await {
        Ok(records) => {
            let mut count = 0;
            for record in &*records {
                match record {
                    Ok(_) => count += 1,
                    Err(err) => panic!("Error executing scan: {}", err),
                }
            }
            assert_eq!(count, 1, "SCAN Test Failed");
        }
        Err(err) => println!("Failed to execute scan: {}", err),
    }

    // OPERATE
    let bin = as_bin!("test85", 85);
    let ops = vec![operations::add(&bin)];

    let key = as_key!(namespace, &set_name, 85);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(15)));
    let op = client.operate(&wpolicy, &key, &ops).await;
    assert_eq!(op.is_err(), true, "OPERATE Err Test Failed");

    let key = as_key!(namespace, &set_name, 85);
    wpolicy.filter_expression = Some(eq(int_bin("bin".to_string()), int_val(85)));
    let op = client.operate(&wpolicy, &key, &ops).await;
    assert_eq!(op.is_ok(), true, "OPERATE Ok Test Failed");

    // BATCH GET
    let mut batch_reads = vec![];
    let b = Bins::from(["bin"]);
    for i in 85..90 {
        let key = as_key!(namespace, &set_name, i);
        batch_reads.push(BatchRead::new(key, b.clone()));
    }
    bpolicy.filter_expression = Some(gt(int_bin("bin".to_string()), int_val(84)));
    match client.batch_get(&bpolicy, batch_reads).await {
        Ok(results) => {
            for result in results {
                let mut count = 0;
                match result.record {
                    Some(_) => count += 1,
                    None => {}
                }
                assert_eq!(count, 1, "BATCH GET Ok Test Failed")
            }
        }
        Err(err) => panic!("Error executing batch request: {}", err),
    }

    client.close().await.unwrap();
}

async fn test_filter(client: &Client, filter: FilterExpression, set_name: &str) -> Arc<Recordset> {
    let namespace = common::namespace();

    let mut qpolicy = QueryPolicy::default();
    qpolicy.filter_expression = Some(filter);

    let statement = Statement::new(namespace, set_name, Bins::All);
    client.query(&qpolicy, statement).await.unwrap()
}

fn count_results(rs: Arc<Recordset>) -> usize {
    let mut count = 0;

    for res in &*rs {
        match res {
            Ok(_) => {
                count += 1;
            }
            Err(err) => panic!("{:?}", err),
        }
    }

    count
}
