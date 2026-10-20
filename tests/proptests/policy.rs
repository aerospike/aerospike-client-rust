// Copyright 2015-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

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
use aerospike::ReadTouchTtl;
use aerospike::RecordExistsAction;

use aerospike::{
    BatchDeletePolicy, BatchPolicy, BatchReadPolicy, BatchUdfPolicy, BatchWritePolicy, Expiration,
    ReadPolicy, WritePolicy,
};

use proptest::bool;
use proptest::prelude::*;

use aerospike::{ReadModeAp, ReadModeSc};

pub fn read_touch_ttl() -> impl Strategy<Value = ReadTouchTtl> {
    prop_oneof![
        Just(ReadTouchTtl::ServerDefault),
        Just(ReadTouchTtl::DontReset),
        any::<u32>().prop_map(|pct| ReadTouchTtl::Percent((pct % 100) as u8)),
    ]
}

pub fn concurrency() -> impl Strategy<Value = Concurrency> {
    prop_oneof![Just(Concurrency::Sequential), Just(Concurrency::Parallel),]
}

pub fn read_mode_ap() -> impl Strategy<Value = ReadModeAp> {
    prop_oneof![Just(ReadModeAp::One), Just(ReadModeAp::All),]
}

pub fn read_mode_sc() -> impl Strategy<Value = ReadModeSc> {
    prop_oneof![
        Just(ReadModeSc::Session),
        // Just(ReadModeSc::Linearize),
        // Just(ReadModeSc::AllowReplica),
        // Just(ReadModeSc::AllowUnavailable),
    ]
}

pub fn duration_ms(d1: u32, d2: u32) -> impl Strategy<Value = u32> {
    (d1..d2).prop_map(|n| n)
}

pub fn duration_ms_opt(d1: u32, d2: u32) -> impl Strategy<Value = Option<Duration>> {
    prop_oneof![
        (d1..d2).prop_map(|n| Some(Duration::new(0, n * 1_000_000))),
        Just(None),
    ]
}

pub fn max_retries(min: usize, max: usize) -> impl Strategy<Value = usize> {
    (min..max).prop_map(|n| n)
}

pub fn expiration(min: u32, max: u32) -> impl Strategy<Value = Expiration> {
    prop_oneof![
        Just(Expiration::Never),
        Just(Expiration::DontUpdate),
        Just(Expiration::NamespaceDefault),
        (min..max).prop_map(Expiration::Seconds),
    ]
}

pub fn expiration_ns_default() -> impl Strategy<Value = Expiration> {
    prop_oneof![Just(Expiration::NamespaceDefault),]
}

pub fn replica() -> impl Strategy<Value = Replica> {
    prop_oneof![
        Just(Replica::Master),
        Just(Replica::MasterProles),
        Just(Replica::Random),
        Just(Replica::Sequence),
        // Just(Replica::PreferRack),
    ]
}

pub fn query_duration() -> impl Strategy<Value = QueryDuration> {
    prop_oneof![
        Just(QueryDuration::Long),
        Just(QueryDuration::Short),
        Just(QueryDuration::LongRelaxAp),
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

pub fn base_policy(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = BasePolicy> {
    (
        duration_ms(socket_timeout_ms, socket_timeout_ms * 2),
        duration_ms(total_timeout_ms, total_timeout_ms * 3),
        duration_ms(0, 10000),
        max_retries(0, 100),
        100..500_u32,
        read_mode_ap(),
        read_mode_sc(),
        read_touch_ttl(),
        Just(None), //true_or_false_filter_expression(),
        replica(),
    )
        .prop_map(
            |(
                socket_timeout,
                total_timeout,
                timeout_delay,
                max_retries,
                sleep_between_retries,
                read_mode_ap,
                read_mode_sc,
                read_touch_ttl,
                filter_expression,
                replica,
            )| BasePolicy {
                socket_timeout,
                total_timeout,
                timeout_delay,
                max_retries,
                sleep_between_retries,
                sleep_multiplier: 1.0,
                read_mode_ap,
                read_mode_sc,
                read_touch_ttl,
                replica,
                use_compression: false,
                compression_threshold: 128,
                filter_expression,
                txn: None,
                populate_positional_results: false,
                error_detail_verbosity: 0,
            },
        )
}

pub fn write_policy(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = WritePolicy> {
    (
        base_policy(socket_timeout_ms, total_timeout_ms),
        record_exists_action(),
        generation_policy(),
        commit_level(),
        any::<u32>(),
        expiration_ns_default(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<u32>(),
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
                records_per_second,
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
                on_locking_only: false,
                xdr: false,
                records_per_second,
            },
        )
}

pub fn write_policy_without_replace(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = WritePolicy> {
    (
        base_policy(socket_timeout_ms, total_timeout_ms),
        record_exists_action_no_replace(),
        generation_policy(),
        commit_level(),
        any::<u32>(),
        expiration_ns_default(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        any::<u32>(),
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
                records_per_second,
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
                on_locking_only: false,
                xdr: false,
                records_per_second,
            },
        )
}

pub fn query_policy(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = QueryPolicy> {
    (
        base_policy(socket_timeout_ms, total_timeout_ms),
        0..256_usize,
        0..1000_u64,
        1..u32::MAX,
        1..10_000_usize,
        query_duration(),
    )
        .prop_map(
            |(
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                expected_duration,
            )| QueryPolicy {
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                expected_duration,
                include_bin_data: true,
            },
        )
}

pub fn query_policy_scan(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = QueryPolicy> {
    (
        base_policy(socket_timeout_ms, total_timeout_ms),
        0..256_usize,
        0..1000_u64,
        1..u32::MAX,
        1..10_000_usize,
        Just(QueryDuration::Long),
    )
        .prop_map(
            |(
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                expected_duration,
            )| QueryPolicy {
                base_policy,
                max_concurrent_nodes,
                max_records,
                records_per_second,
                record_queue_size,
                expected_duration,
                include_bin_data: true,
            },
        )
}

pub fn read_policy(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = ReadPolicy> {
    base_policy(socket_timeout_ms, total_timeout_ms).prop_map(|base_policy| ReadPolicy { base_policy })
}

pub fn batch_policy(
    socket_timeout_ms: u32,
    total_timeout_ms: u32,
) -> impl Strategy<Value = BatchPolicy> {
    (
        base_policy(socket_timeout_ms, total_timeout_ms),
        concurrency(),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
        true_or_false_filter_expression(),
    )
        .prop_map(
            |(
                mut base_policy,
                concurrency,
                allow_inline,
                allow_inline_ssd,
                respond_all_keys,
                filter_expression,
            )| {
                base_policy.filter_expression = filter_expression;
                BatchPolicy {
                    base_policy,
                    concurrency,
                    allow_inline,
                    allow_inline_ssd,
                    respond_all_keys,
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

prop_compose! {
    pub fn batch_udf_policy()
    (
        commit_level in commit_level(),
        expiration in expiration(0, 5),
        durable_delete in any::<bool>(),
        send_key in any::<bool>(),
        filter_expression in true_or_false_filter_expression(),
    )
    -> BatchUdfPolicy {
        BatchUdfPolicy {
            commit_level,
            expiration,
            durable_delete,
            send_key,
            filter_expression,
            // for all other fields, assume their default values.
            ..Default::default()
        }
    }
}
