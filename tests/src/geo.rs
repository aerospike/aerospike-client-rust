// Copyright 2015-2016 Aerospike, Inc.
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

use aerospike::*;

use std::sync::Arc;

#[test]
fn geo_filter_macros() {
    let geo_filter = as_within_region!("bin1", "{}");
    assert_eq!(geo_filter.bin_name, "bin1");

    let geo_filter = as_within_region_in_collection!("bin1", CollectionIndexType::MapValues, "{}");
    assert_eq!(geo_filter.bin_name, "bin1");

    let geo_filter = as_regions_containing_point!("bin1", "{}");
    assert_eq!(geo_filter.bin_name, "bin1");

    let geo_filter = as_regions_containing_point_in_collection!("bin1",
                                                                CollectionIndexType::MapValues,
                                                                "{}");
    assert_eq!(geo_filter.bin_name, "bin1");

    let geo_filter = as_within_radius!("bin1", 1, 3, 7);
    assert_eq!(geo_filter.bin_name, "bin1");

    let geo_filter = as_within_radius_in_collection!("bin1", CollectionIndexType::List, 1, 3, 7);
    assert_eq!(geo_filter.bin_name, "bin1");
}
