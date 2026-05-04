use crate::common;
use crate::proptest_async;
use crate::proptests::{policy::*, queries::*};

use futures::stream::StreamExt;

use aerospike::*;

/// Applies SC-only tweaks to a proptest-generated [`QueryPolicy`] for full-set scans of seeded
/// `test-multi`. AP namespaces leave `policy` unchanged.
///
/// The incoming `policy` is random (`query_policy(1000, 5000)` and friends in `policy.rs`).
/// Each field below fixes a different bad draw for a large scan on SC (illegal mode, throttle,
/// tiny queue, short client deadlines, no retries)—not redundant copies of the same check.
async fn adjust_query_policy_for_scan_proptest(client: &Client, mut policy: QueryPolicy) -> QueryPolicy {
    if !namespace_sc!(client) {
        return policy;
    }

    if policy.expected_duration == QueryDuration::LongRelaxAP {
        policy.expected_duration = QueryDuration::Long;
    }
    policy.records_per_second = 0;
    policy.record_queue_size = policy.record_queue_size.max(64);
    policy.base_policy.total_timeout = policy.base_policy.total_timeout.max(45_000);
    policy.base_policy.socket_timeout = policy.base_policy.socket_timeout.max(8_000);
    policy.base_policy.socket_timeout = policy
        .base_policy
        .socket_timeout
        .min(policy.base_policy.total_timeout);
    policy.base_policy.max_retries = policy.base_policy.max_retries.max(1);

    policy
}

proptest_async::proptest! {
    #[test]
    async fn scan(
        query_policy in query_policy(1000, 5000),
        stmt in statement_scan(common::namespace().into(), common::prop_setname_multi().into()))
    {
        let client = common::singleton_client().await;
        let query_policy = adjust_query_policy_for_scan_proptest(client, query_policy).await;

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
