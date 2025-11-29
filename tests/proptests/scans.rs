use crate::common;
use crate::proptest_async;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn scan(scan_policy in scan_policy(30000), mut pf in partition_filter(common::namespace().into(), "test".into()), bins in bins(100)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name = "test";

        // let mut recs = vec![];
        let mut count = 0;
        let mut iter = 0;
        while !pf.done() {
            iter+=1;

            // let now = aerospike_rt::time::Instant::now();
            // println!("Scan starting...");
            let rs = client.scan(&scan_policy, pf, namespace, set_name, bins.clone()).await.unwrap();

            let rs = rs.into_stream();
            tokio::pin!(rs);

            while let Some(res) = rs.next().await {
                // println!("RECORD RECEIVED!");
                if res.is_ok() {
                    count+=1;
                } else {
                    panic!("{}", res.err().unwrap())
                }
            }

            pf = rs.partition_filter().await.unwrap();
        }
        // println!("Scan succeeded in {:?}", now.elapsed());

        assert!(scan_policy.max_records == 0 || count <= scan_policy.max_records || iter > 1);
    }
}
