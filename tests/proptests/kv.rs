use crate::common;
use crate::proptest_async;

use aerospike::{as_bin, as_key};
use aerospike::{policy::Replica, Error, GenerationPolicy, RecordExistsAction, ResultCode};

use crate::proptests::{bins::*, policy::*};

proptest_async::proptest! {
    #[test]
    async fn put(i in 0..10, write_policy in write_policy(1000, 5000), ref bins in many_bins(255)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);

        let err = client.put(&write_policy, &key, bins).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(e @ Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
                if (write_policy.record_exists_action != RecordExistsAction::UpdateOnly) &&
                 (write_policy.record_exists_action != RecordExistsAction::ReplaceOnly) {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn add(i in 0..1000, write_policy in write_policy_without_replace(1000, 5000), val in -1000..1000) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);
        let bins = vec![
            as_bin!("bin_i", val),
            as_bin!("bin_i_not_exist", val),
        ];

        let err = client.add(&write_policy, &key, &bins).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(e @ Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
                if (write_policy.record_exists_action != RecordExistsAction::UpdateOnly) &&
                 (write_policy.record_exists_action != RecordExistsAction::ReplaceOnly) {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn append(i in 0..1000, write_policy in write_policy_without_replace(1000, 5000), s in "[\\w\\d]{1,1000}") {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);
        let bins = vec![
            as_bin!("bin_s", s.clone()),
            as_bin!("bin_s_not_exist", s),
        ];

        let err = client.append(&write_policy, &key, &bins).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(e @ Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
                if (write_policy.record_exists_action != RecordExistsAction::UpdateOnly) &&
                 (write_policy.record_exists_action != RecordExistsAction::ReplaceOnly) {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn prepend(i in 0..1000, write_policy in write_policy_without_replace(1000, 5000), s in "[\\w\\d]{1,1000}") {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);
        let bins = vec![
            as_bin!("bin_s", s.clone()),
            as_bin!("bin_s_not_exist", s),
        ];

        let err = client.prepend(&write_policy, &key, &bins).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(e @ Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => {
                if (write_policy.record_exists_action != RecordExistsAction::UpdateOnly) &&
                 (write_policy.record_exists_action != RecordExistsAction::ReplaceOnly) {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn touch(i in 0..1000, write_policy in write_policy_without_replace(1000, 5000)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);

        let err = client.touch(&write_policy, &key).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => (),
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn delete(i in 0..1000, write_policy in write_policy_without_replace(1000, 5000)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);

        let err = client.touch(&write_policy, &key).await;

        match err {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => (),
            Err(e @ Error::ServerError(ResultCode::KeyExistsError, _, _)) => {
                if write_policy.record_exists_action != RecordExistsAction::CreateOnly {
                    panic!("{}",e);
                 }
            },
            Err(e @ Error::ServerError(ResultCode::GenerationError, _, _)) => {
                if write_policy.generation_policy != GenerationPolicy::None {
                    return; // it's fine
                }
                panic!("{}", e);
            },
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn get(i in 0..1000, read_policy in read_policy(1000, 5000), bins in bins(100)) {
        let client = common::singleton_client().await;

        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);

        let res = client.get(&read_policy, &key, bins).await;

        match res {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => (), // it's fine
            Err(e @ Error::InvalidArgument(_)) => {
                if read_policy.replica != Replica::PreferRack {
                    panic!("{}", e);
                }
            }
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }

    #[test]
    async fn exists(i in 0..1000, read_policy in read_policy(1000, 5000)) {
        let client = common::singleton_client().await;
        let namespace: &str = common::namespace();
        let set_name: &str = common::prop_setname();
        let key = as_key!(namespace, set_name, i);

        let res = client.exists(&read_policy, &key).await;

        match res {
            Err(Error::ServerError(ResultCode::FilteredOut, _, _)) => (), // it's fine
            Err(Error::ServerError(ResultCode::KeyNotFoundError, _, _)) => (), // it's fine
            Err(e @ Error::InvalidArgument(_)) => {
                if read_policy.replica != Replica::PreferRack {
                    panic!("{}", e);
                }
            }
            Err(e) => panic!("{}", e),
            _ => (),
        }
    }
}
