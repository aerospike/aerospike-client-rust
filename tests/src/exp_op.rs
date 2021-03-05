use crate::common;
use aerospike::expressions::{int_bin, int_val, num_add};
use aerospike::operations::exp::{read_exp, ExpReadFlags, write_exp, ExpWriteFlags};
use aerospike::{as_bin, as_key, as_val, Bins, ReadPolicy, Value, WritePolicy};
use aerospike::operations::OperationType::ExpWrite;

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
    let rec = rec.unwrap();

    assert_eq!(*rec.bins.get("").unwrap(), as_val!(29), "EXP OPs read failed");

    let flt2 = int_bin("bin2".to_string());
    let ops = &vec![write_exp(ExpWriteFlags::Default,"bin2", &flt), read_exp(&flt2, ExpReadFlags::Default)];

    let rec = client.operate(&wpolicy, &key, ops);
    let rec = rec.unwrap();

    assert_eq!(*rec.bins.get("").unwrap(), as_val!(29), "EXP OPs write failed");
}
