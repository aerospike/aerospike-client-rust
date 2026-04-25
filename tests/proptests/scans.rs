use crate::common;
use crate::proptest_async;
use crate::proptests::{policy::*, queries::*};

use futures::stream::StreamExt;
use proptest::prelude::*;

use aerospike::{QueryDuration, *};

proptest_async::proptest! {
    #[test]
    async fn scan(
        // Full-partition scans over `insert_bins` data (many bins / records) often exceed
        // short socket reads; keep floor timeouts well above interactive defaults.
        query_policy in query_policy(20_000, 60_000)
            .prop_filter("ShortQuery and rps together are invalid",
                |qp| !(qp.expected_duration == QueryDuration::Short && qp.records_per_second > 0)),
        stmt in statement_scan(common::namespace().into(), common::prop_setname_multi().into()))
    {
        let client = common::singleton_client().await;

        // let now = aerospike_rt::time::Instant::now();

        // let mut recs = vec![];
        let mut count = 0;
        let mut iter = 0;

        let mut pf = PartitionFilter::all();

        while !pf.done() {
            iter+=1;

            // println!("Scan starting...");
            let rs = client.query(&query_policy, pf, stmt.clone()).await.unwrap();

            let rs = rs.into_stream();
            tokio::pin!(rs);

            while let Some(res) = rs.next().await {
                match res {
                    Ok(_) => count+=1,
                    Err(e) => panic!("{}", e),
                }
            }

            pf = rs.partition_filter().await.unwrap();
        }
        // println!("Scan succeeded in {:?} for {} records", now.elapsed(), count);

        assert!(query_policy.max_records == 0 || count <= query_policy.max_records || iter > 1);
    }
}
