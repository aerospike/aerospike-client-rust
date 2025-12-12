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
            mut pf in partition_filter(common::namespace().into(), "multi".into()),
            stmt in statement(common::namespace().into(), "multi".into()))
    {
        let client = common::singleton_client().await;

        // let now = aerospike_rt::time::Instant::now();

        // let mut recs = vec![];
        let mut count = 0;
        let mut iter = 0;
        while !pf.done() {
            iter+= 1;

            let rs = client.query(&query_policy, pf, stmt.clone()).await.unwrap();
            let rs = rs.into_stream();
            tokio::pin!(rs);

            while let Some(res) = rs.next().await {
                match res {
                    Ok(_) => count+=1,
                    Err(Error::ServerError(ResultCode::IndexNotFound, _, _)) => (), // it's fine
                    Err(e) => panic!("{}", e),
                }
            }

            pf = rs.partition_filter().await.unwrap();
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
        as_eq!(&bin_name, val)
   }
}

prop_compose! {
    pub fn filter_range(bin_name: String)((begin, end) in value_for_range_filter()) -> Filter {
        as_range!(&bin_name, begin, end)
   }
}

prop_compose! {
    pub fn filter_contains(bin_name: String)(val in value_any(), cit in collection_index_type()) -> Filter {
        as_contains!(&bin_name, val, cit)
   }
}

prop_compose! {
    pub fn filter_contains_range(bin_name: String)(begin in value_any(), end in value_any(), cit in collection_index_type()) -> Filter {
        as_contains_range!(&bin_name, begin, end, cit)
   }
}

prop_compose! {
    pub fn statement(ns: String, set_name: String)(bins in latin_bins(50), filter in filter("bin_i".into()), with_filter in any::<bool>()) -> Statement {
       let mut stmt = Statement::new(&ns, &set_name, bins);
       if with_filter {
            stmt.add_filter(filter);
       }
       stmt
   }
}
