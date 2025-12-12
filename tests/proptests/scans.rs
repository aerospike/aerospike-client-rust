use crate::common;
use crate::proptest_async;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn scan(scan_policy in scan_policy(1000, 5000), mut pf in partition_filter(common::namespace().into(), "multi".into()), bins in bins(100)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();

        // let now = aerospike_rt::time::Instant::now();

        // let mut recs = vec![];
        let mut count = 0;
        let mut iter = 0;

        while !pf.done() {
            iter+=1;

            // println!("Scan starting...");
            let rs = client.scan(&scan_policy, pf, namespace, "multi", bins.clone()).await.unwrap();

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

        assert!(scan_policy.max_records == 0 || count <= scan_policy.max_records || iter > 1);
    }
}
