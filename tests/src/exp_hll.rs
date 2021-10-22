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

use aerospike::expressions::hll::*;
use aerospike::expressions::lists::*;
use aerospike::expressions::*;
use aerospike::operations::hll::HLLPolicy;
use aerospike::operations::lists::ListReturnType;
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
        let lbin = as_bin!("lbin", as_list!(i, "a"));
        let bins = vec![ibin, lbin];
        client.delete(&wpolicy, &key).await.unwrap();
        client.put(&wpolicy, &key, &bins).await.unwrap();
        let data = vec![Value::from("asd"), Value::from(i)];
        let data2 = vec![Value::from("asd"), Value::from(i), Value::from(i + 1)];
        let ops = [
            operations::hll::add_with_index_and_min_hash(
                &HLLPolicy::default(),
                "hllbin",
                &data,
                8,
                0,
            ),
            operations::hll::add_with_index_and_min_hash(
                &HLLPolicy::default(),
                "hllbin2",
                &data2,
                8,
                0,
            ),
        ];
        client
            .operate(&WritePolicy::default(), &key, &ops)
            .await
            .unwrap();
    }
    set_name
}

#[aerospike_macro::test]
async fn expression_hll() {
    let _ = env_logger::try_init();
    let client = common::client().await;
    let set_name = create_test_set(&client, EXPECTED).await;

    let rs = test_filter(
        &client,
        eq(
            get_count(add_with_index_and_min_hash(
                HLLPolicy::default(),
                list_val(vec![Value::from(48715414)]),
                int_val(8),
                int_val(0),
                hll_bin("hllbin".to_string()),
            )),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "HLL INIT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            may_contain(
                list_val(vec![Value::from(55)]),
                hll_bin("hllbin".to_string()),
            ),
            int_val(1),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 1, "HLL MAY CONTAIN Test Failed");

    let rs = test_filter(
        &client,
        lt(
            get_by_index(
                ListReturnType::Values,
                ExpType::INT,
                int_val(0),
                describe(hll_bin("hllbin".to_string())),
                &[],
            ),
            int_val(10),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 100, "HLL DESCRIBE Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_count(get_union(
                hll_bin("hllbin".to_string()),
                hll_bin("hllbin2".to_string()),
            )),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "HLL GET UNION Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_union_count(
                hll_bin("hllbin".to_string()),
                hll_bin("hllbin2".to_string()),
            ),
            int_val(3),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 98, "HLL GET UNION COUNT Test Failed");

    let rs = test_filter(
        &client,
        eq(
            get_intersect_count(
                hll_bin("hllbin".to_string()),
                hll_bin("hllbin2".to_string()),
            ),
            int_val(2),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "HLL GET INTERSECT COUNT Test Failed");

    let rs = test_filter(
        &client,
        gt(
            get_similarity(
                hll_bin("hllbin".to_string()),
                hll_bin("hllbin2".to_string()),
            ),
            float_val(0.5f64),
        ),
        &set_name,
    )
    .await;
    let count = count_results(rs);
    assert_eq!(count, 99, "HLL GET INTERSECT COUNT Test Failed");

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
