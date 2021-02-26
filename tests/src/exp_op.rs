use crate::common;
use aerospike::expressions::{int_bin, int_val, num_add};
use aerospike::operations::exp::{read_exp, ExpReadFlags};
use aerospike::{as_bin, as_key, as_val, Bins, ReadPolicy, Value, WritePolicy};

#[test]
fn exp_ops() {
    let _ = env_logger::try_init();

    let client = common::client();
    let namespace = common::namespace();
    let set_name = &common::rand_str(10);

    let policy = ReadPolicy::default();

    let wpolicy = WritePolicy::default();
    let key = as_key!(namespace, set_name, -1);
    let wbin = as_bin!("bin", as_val!(25));
    let bins = vec![&wbin];

    client.delete(&wpolicy, &key).unwrap();

    client.put(&wpolicy, &key, &bins).unwrap();
    let rec = client.get(&policy, &key, Bins::All).unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(25), "EXP OPs init failed");
    let flt = num_add(vec![
        int_bin("bin".to_string()),
        int_val(4),
    ]);
    let ops = &vec![read_exp(&flt, ExpReadFlags::Default)];
    let rec = client.operate(&wpolicy, &key, ops);
    println!("{:?}", rec);
    let rec = rec.unwrap();
    assert_eq!(*rec.bins.get("bin").unwrap(), as_val!(35), "EXP OPs read failed");
}
