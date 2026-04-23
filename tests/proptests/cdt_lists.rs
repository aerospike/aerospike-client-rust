use crate::common;
use crate::proptest_async;

use aerospike::operations::lists;
use aerospike::operations::lists::ListPolicy;
use aerospike::*;

proptest_async::proptest! {
    #[test]
    async fn cdt_list_insert(
        v1 in 0..100, v2 in 0..100, v3 in 0..100
    ) {
        let client = common::singleton_client().await;
        let namespace = common::namespace();
        let set_name = &common::prop_setname();

        let rpolicy = ReadPolicy::default();
        let wpolicy = WritePolicy::default();
        let key = as_key!(namespace, set_name, -1);
        let lpolicy = ListPolicy::default();
        let val = as_list!("0", 1, 2.1f64);
        let bins = vec![as_bin!("bin", val.clone())];

        client.delete(&wpolicy, &key).await.unwrap();

        client.put(&wpolicy, &key, &bins).await.unwrap();
        let rec = client.get(&rpolicy, &key, Bins::All).await.unwrap();
        assert_eq!(*rec.bins.get("bin").unwrap(), val);

        let values = vec![as_val!(v1), as_val!(v2), as_val!(v3)];
        let ops = &vec![
            lists::insert_items(&lpolicy, "bin", 1, values),
            operations::get_bin("bin"),
        ];
        let rec = client.operate(&wpolicy, &key, ops).await.unwrap();
        assert_eq!(
            *rec.bins.get("bin").unwrap(),
            Value::MultiResult(as_values!(6, as_list!("0", v1, v2, v3, 1, 2.1f64)))
        );
    }
}
