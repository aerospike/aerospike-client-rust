use std::time::Duration;

use crate::proptests::filter_expression::*;

use aerospike::policy::BasePolicy;
use aerospike::policy::Replica;
use aerospike::CollectionIndexType;
use aerospike::CommitLevel;
use aerospike::Concurrency;
use aerospike::GenerationPolicy;
use aerospike::QueryDuration;
use aerospike::QueryPolicy;
use aerospike::ReadTouchTTL;
use aerospike::RecordExistsAction;

use aerospike::{
    BatchDeletePolicy, BatchPolicy, BatchReadPolicy, BatchWritePolicy, Expiration, ReadPolicy,
    ScanPolicy, WritePolicy,
};

use proptest::bool;
use proptest::prelude::*;

use aerospike::ConsistencyLevel;

pub fn read_touch_ttl() -> impl Strategy<Value = ReadTouchTTL> {
    prop_oneof![
        Just(ReadTouchTTL::ServerDefault),
        Just(ReadTouchTTL::DontReset),
        any::<u32>().prop_map(|pct| ReadTouchTTL::Percent((pct % 100) as u8)),
    ]
}

pub fn concurrency() -> impl Strategy<Value = Concurrency> {
    prop_oneof![Just(Concurrency::Sequential), Just(Concurrency::Parallel),]
}

pub fn consistency_level() -> impl Strategy<Value = ConsistencyLevel> {
    prop_oneof![
        Just(ConsistencyLevel::ConsistencyOne),
        Just(ConsistencyLevel::ConsistencyAll),
    ]
}

pub fn duration_ms(d1: u32, d2: u32) -> impl Strategy<Value = Duration> {
    (d1..d2).prop_map(|n| Duration::new(n as u64 / 1000, (n % 1000) * 1_000_000))
}

pub fn duration_ms_opt(d1: u32, d2: u32) -> impl Strategy<Value = Option<Duration>> {
    prop_oneof![
        (d1..d2).prop_map(|n| Some(Duration::new(0, n * 1_000_000))),
        Just(None),
    ]
}

pub fn max_retries(min: usize, max: usize) -> impl Strategy<Value = Option<usize>> {
    prop_oneof![(min..max).prop_map(|n| Some(n)), Just(None),]
}

pub fn expiration(min: u32, max: u32) -> impl Strategy<Value = Expiration> {
    prop_oneof![
        Just(Expiration::Never),
        Just(Expiration::DontUpdate),
        Just(Expiration::NamespaceDefault),
        (min..max).prop_map(|n| Expiration::Seconds(n)),
    ]
}

pub fn expiration_ns_default() -> impl Strategy<Value = Expiration> {
    prop_oneof![Just(Expiration::NamespaceDefault),]
}

pub fn replica() -> impl Strategy<Value = Replica> {
    prop_oneof![
        Just(Replica::Master),
        Just(Replica::Sequence),
        // Just(Replica::PreferRack),
    ]
}

pub fn query_duration() -> impl Strategy<Value = QueryDuration> {
    prop_oneof![
        Just(QueryDuration::Long),
        Just(QueryDuration::Short),
        Just(QueryDuration::LongRelaxAP),
    ]
}

pub fn collection_index_type() -> impl Strategy<Value = CollectionIndexType> {
    prop_oneof![
        Just(CollectionIndexType::Default),
        Just(CollectionIndexType::List),
        Just(CollectionIndexType::MapKeys),
        Just(CollectionIndexType::MapValues),
    ]
}

pub fn record_exists_action() -> impl Strategy<Value = RecordExistsAction> {
    prop_oneof![
        Just(RecordExistsAction::Update),
        // Just(RecordExistsAction::UpdateOnly),
        // Just(RecordExistsAction::Replace),
        // Just(RecordExistsAction::ReplaceOnly),
        // Just(RecordExistsAction::CreateOnly),
    ]
}

pub fn record_exists_action_no_replace() -> impl Strategy<Value = RecordExistsAction> {
    prop_oneof![
        Just(RecordExistsAction::Update),
        Just(RecordExistsAction::UpdateOnly),
        Just(RecordExistsAction::CreateOnly),
    ]
}

pub fn generation_policy() -> impl Strategy<Value = GenerationPolicy> {
    prop_oneof![
        Just(GenerationPolicy::None),
        Just(GenerationPolicy::ExpectGenEqual),
        Just(GenerationPolicy::ExpectGenGreater),
    ]
}

pub fn commit_level() -> impl Strategy<Value = CommitLevel> {
    prop_oneof![
        Just(CommitLevel::CommitAll),
        Just(CommitLevel::CommitMaster),
    ]
}

pub fn base_policy(timeout_ms: u32) -> impl Strategy<Value = BasePolicy> {
    (
        duration_ms(timeout_ms, timeout_ms + 1000).prop_map(|d| Some(d)),
        max_retries(0, 100),
        duration_ms_opt(100, 500),
        consistency_level(),
        read_touch_ttl(),
        Just(None), //true_or_false_filter_expression(),
    )
        .prop_map(
            |(
                total_timeout,
                max_retries,
                sleep_between_retries,
                consistency_level,
                read_touch_ttl,
                filter_expression,
            )| BasePolicy {
                total_timeout,
                max_retries,
                sleep_between_retries,
                consistency_level,
                read_touch_ttl,
                filter_expression,
            },
        )
}

pub fn write_policy(timeout_ms: u32) -> impl Strategy<Value = WritePolicy> {
    (
        base_policy(timeout_ms),
        record_exists_action(),
        generation_policy(),
        commit_level(),
        any::<u32>(),
        expiration_ns_default(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            |(
                base_policy,
                record_exists_action,
                generation_policy,
                commit_level,
                generation,
                expiration,
                send_key,
                respond_per_each_op,
                durable_delete,
            )| WritePolicy {
                base_policy,
                record_exists_action,
                generation_policy,
                commit_level,
                generation,
                expiration,
                send_key,
                respond_per_each_op,
                durable_delete,
            },
        )
}

pub fn write_policy_without_replace(timeout_ms: u32) -> impl Strategy<Value = WritePolicy> {
    (
        base_policy(timeout_ms),
        record_exists_action_no_replace(),
        generation_policy(),
        commit_level(),
        any::<u32>(),
        expiration_ns_default(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            |(
                base_policy,
                record_exists_action,
                generation_policy,
                commit_level,
                generation,
                expiration,
                send_key,
                respond_per_each_op,
                durable_delete,
            )| WritePolicy {
                base_policy,
                record_exists_action,
                generation_policy,
                commit_level,
                generation,
                expiration,
                send_key,
                respond_per_each_op,
                durable_delete,
            },
        )
}

pub fn scan_policy(timeout_ms: u32) -> impl Strategy<Value = ScanPolicy> {
    (
        base_policy(timeout_ms),
        0..256 as usize,
        0..1000 as u64,
        1..u32::MAX,
        1..10_000 as usize,
        5000..10_000 as u32,
        replica(),
    )
        .prop_map(
            |(
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                socket_timeout,
                replica,
            )| ScanPolicy {
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                socket_timeout,
                replica,
            },
        )
}

pub fn query_policy(timeout_ms: u32) -> impl Strategy<Value = QueryPolicy> {
    (
        base_policy(timeout_ms),
        0..256 as usize,
        0..1000 as u64,
        1..u32::MAX,
        1..10_000 as usize,
        5000..10_000 as u32,
        query_duration(),
        replica(),
    )
        .prop_map(
            |(
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                socket_timeout,
                expected_duration,
                replica,
            )| QueryPolicy {
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                socket_timeout,
                expected_duration,
                replica,
            },
        )
}

pub fn read_policy(timeout_ms: u32) -> impl Strategy<Value = ReadPolicy> {
    (base_policy(timeout_ms), replica()).prop_map(|(base_policy, replica)| ReadPolicy {
        base_policy,
        replica,
    })
}

pub fn batch_policy(timeout_ms: u32) -> impl Strategy<Value = BatchPolicy> {
    (
        base_policy(timeout_ms),
        concurrency(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        true_or_false_filter_expression(),
        replica(),
    )
        .prop_map(
            |(
                base_policy,
                concurrency,
                allow_inline,
                allow_inline_ssd,
                respond_all_keys,
                filter_expression,
                replica,
            )| {
                BatchPolicy {
                    base_policy,
                    concurrency,
                    allow_inline,
                    allow_inline_ssd,
                    respond_all_keys,
                    filter_expression,
                    replica,
                }
            },
        )
}

pub fn batch_read_policy() -> impl Strategy<Value = BatchReadPolicy> {
    (read_touch_ttl(), true_or_false_filter_expression()).prop_map(
        |(read_touch_ttl, filter_expression)| BatchReadPolicy {
            read_touch_ttl,
            filter_expression,
        },
    )
}

prop_compose! {
    pub fn batch_write_policy()
    (
        record_exists_action in record_exists_action(),
        expiration in expiration(0, 5),
        durable_delete in any::<bool>(),
        filter_expression in true_or_false_filter_expression(),
    )
    -> BatchWritePolicy {
        BatchWritePolicy {
            record_exists_action,
            expiration,
            durable_delete,
            filter_expression,
            // for all other fields, assume their default values.
            ..Default::default()
        }
    }
}

prop_compose! {
    pub fn batch_delete_policy()
    (
        generation_policy in generation_policy(),
        commit_level in commit_level(),
        durable_delete in any::<bool>(),
        filter_expression in true_or_false_filter_expression(),
    )
    -> BatchDeletePolicy {
        BatchDeletePolicy {
            generation_policy,
            commit_level,
            durable_delete,
            filter_expression,
            // for all other fields, assume their default values.
            ..Default::default()
        }
    }
}
