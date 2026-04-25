use aerospike::query::PartitionFilter;

use crate::proptests::key::*;
use proptest::prelude::*;

pub fn partition_filter(ns: String, set_name: String) -> impl Strategy<Value = PartitionFilter> {
    prop_oneof![
        Just(PartitionFilter::all()),
        (0usize..4095).prop_map(|partition_id| PartitionFilter::by_id(partition_id)),
        (0usize..4096 / 2, 1usize..4096 / 2)
            .prop_map(|(begin, count)| PartitionFilter::by_range(begin, count)),
        any_key(ns, set_name).prop_map(|key| { PartitionFilter::by_key(&key) }),
    ]
}

/// Like [`partition_filter`], but omits [`PartitionFilter::by_key`], which the server rejects with
/// `ParameterError` when combined with a secondary-index query (non-nil filter).
pub fn partition_filter_secondary_index_query(
    _ns: String,
    _set_name: String,
) -> impl Strategy<Value = PartitionFilter> {
    prop_oneof![
        Just(PartitionFilter::all()),
        (0usize..4095).prop_map(|partition_id| PartitionFilter::by_id(partition_id)),
        (0usize..4096 / 2, 1usize..4096 / 2)
            .prop_map(|(begin, count)| PartitionFilter::by_range(begin, count)),
    ]
}
