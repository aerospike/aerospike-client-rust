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

use crate::common;
use aerospike::expressions::{int_bin, int_val, num_add};
use aerospike::operations::exp::{read_exp, write_exp, ExpReadFlags, ExpWriteFlags};
use aerospike::{as_bin, as_key, as_val, Bins, ReadPolicy, WritePolicy};

#[aerospike_macro::test]
async fn exp_ops() {
    let client = common::client().await;
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let wbin = as_bin!("bin", as_val!(25));
    let bins = vec![wbin];

    common::delete_durably(&client, &wpolicy, &key)
        .await
        .unwrap();

    client.put(&wpolicy, &key, &bins).await.unwrap();
    let rec = client.get(&policy, &key, Bins::All).await.unwrap();
    assert_eq!(
        *rec.bins.get("bin").unwrap(),
        as_val!(25),
        "EXP OPs init failed"
    );
    let flt = num_add(vec![int_bin("bin".to_string()), int_val(4)]);
    let ops = &[read_exp("example", flt.clone(), ExpReadFlags::DEFAULT)];
    let rec = client.operate(&wpolicy, &key, ops).await;
    let rec = rec.unwrap();

    assert_eq!(
        *rec.bins.get("example").unwrap(),
        as_val!(29),
        "EXP OPs read failed"
    );

    let flt2 = int_bin("bin2".to_string());
    let ops = &[
        write_exp("bin2", flt, ExpWriteFlags::DEFAULT),
        read_exp("example", flt2, ExpReadFlags::DEFAULT),
    ];

    let rec = client.operate(&wpolicy, &key, ops).await;
    let rec = rec.unwrap();

    assert_eq!(
        *rec.bins.get("example").unwrap(),
        as_val!(29),
        "EXP OPs write failed"
    );

    client.close().await.unwrap();
}
