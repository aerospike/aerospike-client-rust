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

use aerospike::expressions::bitwise::*;
use aerospike::expressions::*;
use aerospike::operations::bitwise::{BitPolicy, BitwiseOverflowActions, BitwiseResizeFlags};
use aerospike::*;
use std::sync::Arc;

const EXPECTED: usize = 100;

async fn create_test_set(client: &Client, no_records: usize) -> String {
    let namespace = common::namespace();
    let set_name = common::rand_str(10);

    let wpolicy = WritePolicy::default();
    for i in 0..no_records as i64 {
        let key = as_key!(namespace, &set_name, i);
        let ibin = as_bin!("bin", as_blob!(vec![0b00000001, 0b01000010]));
        let bins = vec![ibin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
    }
    set_name
}

#[aerospike_macro::test]
async fn expression_bitwise() {
    let client = common::client().await;
    let _ = env_logger::try_init();

    let set_name = create_test_set(&client, EXPECTED).await;

    // EQ
    let rs = test_filter(
        &client,
        eq(
            count(int_val(0), int_val(16), blob_bin("bin".to_string())),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(16),
                resize(
                    &BitPolicy::default(),
                    int_val(4),
                    BitwiseResizeFlags::Default,
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "RESIZE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(16),
                insert(
                    &BitPolicy::default(),
                    int_val(0),
                    blob_val(vec![0b11111111]),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(9),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "INSERT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                remove(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(1),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "REMOVE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                set(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    blob_val(vec![0b10101010]),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(4),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "SET Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                bitwise::or(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    blob_val(vec![0b10101010]),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(5),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "OR Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                xor(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    blob_val(vec![0b10101011]),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(4),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "XOR Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                bitwise::and(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    blob_val(vec![0b10101011]),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "AND Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                bitwise::not(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(7),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "NOT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                lshift(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(16),
                    int_val(9),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "LSHIFT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                rshift(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    int_val(3),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "RSHIFT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                add(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    int_val(128),
                    false,
                    BitwiseOverflowActions::Wrap,
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "ADD Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                subtract(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    int_val(1),
                    false,
                    BitwiseOverflowActions::Wrap,
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(0),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "SUBTRACT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            count(
                int_val(0),
                int_val(8),
                set_int(
                    &BitPolicy::default(),
                    int_val(0),
                    int_val(8),
                    int_val(255),
                    blob_bin("bin".to_string()),
                ),
            ),
            int_val(8),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "SET INT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get(int_val(0), int_val(8), blob_bin("bin".to_string())),
            blob_val(vec![0b00000001]),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "GET Test Failed");

    let rs = test_filter(
        &client,
        eq(
            lscan(
                int_val(8),
                int_val(8),
                bool_val(true),
                blob_bin("bin".to_string()),
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "LSCAN Test Failed");

    let rs = test_filter(
        &client,
        eq(
            rscan(
                int_val(8),
                int_val(8),
                bool_val(true),
                blob_bin("bin".to_string()),
            ),
            int_val(6),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "RSCAN Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_int(int_val(0), int_val(8), false, blob_bin("bin".to_string())),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let item_count = count_results(rs);
    assert_eq!(item_count, 100, "RSCAN Test Failed");

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
