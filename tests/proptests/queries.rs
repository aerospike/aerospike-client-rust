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
use crate::proptest::prelude::*;
use crate::proptest_async;

use crate::proptests::value::*;

use aerospike::query::*;
use aerospike::*;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn query(
        query_policy in query_policy(1000, 5000)
            .prop_filter("ShortQuery and rps together are invalid",
                |qp| !(qp.expected_duration == QueryDuration::Short && qp.records_per_second > 0)),
            mut pf in partition_filter(common::namespace().into(), common::prop_setname_multi()),
            stmt in statement_scan(common::namespace().into(), common::prop_setname_multi()))
    {
        let client = common::singleton_client().await;

        // `LongRelaxAp` is rejected with `ParameterError` on strong-consistency namespaces; keep
        // randomized policies for AP unchanged by only adjusting when `namespace_sc!` is true.
        let mut query_policy = query_policy;
        if namespace_sc!(&client)
            && query_policy.expected_duration == QueryDuration::LongRelaxAp
        {
            query_policy.expected_duration = QueryDuration::Long;
        }

        // let now = aerospike_rt::time::Instant::now();

        // let mut recs = vec![];
        let mut count = 0;
        let mut iter = 0;
        while !pf.done() {
            iter+= 1;

            let rs = match client.query(&query_policy, pf, stmt.clone()).await {
                Err(e) if common::is_index_not_found(&e) => return,
                Err(e) => panic!("{}", e),
                Ok(rs) => rs,
            };
            let rs = rs.into_stream();
            tokio::pin!(rs);

            while let Some(res) = rs.next().await {
                match res {
                    Ok(_) => count+=1,
                    Err(e) if common::is_index_not_found(&e) => return,
                    Err(e) => panic!("{}", e),
                }
            }

            pf = rs.partition_filter().unwrap();
        }

        // println!("Query returned {} records in {:?}", count, now.elapsed());

        assert!(query_policy.max_records == 0 || count <= query_policy.max_records || iter > 1);
    }
}

pub fn filter(bin_name: String) -> impl Strategy<Value = Filter> {
    prop_oneof![
        filter_eq(bin_name.clone()),
        filter_range(bin_name.clone()),
        // filter_contains(bin_name.clone()),
        // filter_contains_range(bin_name.clone()),
    ]
}

prop_compose! {
    pub fn filter_eq(bin_name: String)(val in value_for_eq_filter()) -> Filter {
        Filter::equal(&bin_name, val)
   }
}

prop_compose! {
    pub fn filter_range(bin_name: String)((begin, end) in value_for_range_filter()) -> Filter {
        Filter::range(&bin_name, begin, end)
   }
}

prop_compose! {
    pub fn filter_contains(bin_name: String)(val in value_any(), cit in collection_index_type()) -> Filter {
        Filter::contains(&bin_name, val, cit)
   }
}

prop_compose! {
    pub fn filter_contains_range(bin_name: String)(begin in value_any(), end in value_any(), cit in collection_index_type()) -> Filter {
        Filter::contains_range(&bin_name, begin, end, cit)
   }
}

prop_compose! {
    pub fn statement(ns: String, set_name: String)(bins in latin_bins(50), filter in filter("bin_i".into()), with_filter in any::<bool>()) -> Statement {
       let mut stmt = Statement::new(&ns, &set_name, bins);
       if with_filter {
            stmt.set_filter(filter);
       }
       stmt
   }
}

prop_compose! {
    pub fn statement_scan(ns: String, set_name: String)(bins in latin_bins(50)) -> Statement {
       Statement::new(&ns, &set_name, bins)
   }
}
