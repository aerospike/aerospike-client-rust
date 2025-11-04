use crate::common;
use crate::proptest::prelude::*;
use crate::proptest_async;

use futures::stream::StreamExt;

use crate::proptests::{bins::*, partition_filter::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn scan(scan_policy in scan_policy(30000), pf in partition_filter(common::namespace().into(), "test".into()), bins in bins(100)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name = "test";

        let rs = client.scan(&scan_policy, pf, namespace, set_name, bins).await.unwrap();

        let rs = rs.into_stream();
        tokio::pin!(rs);

        // let mut recs = vec![];
        let mut count = 0;
        while let Some(res) = rs.next().await {
            if res.is_ok() {
                count+=1;
            } else {
                panic!("{}", res.err().unwrap())
            }
        }

        assert!(scan_policy.max_records == 0 || count <= scan_policy.max_records);
    }
}
