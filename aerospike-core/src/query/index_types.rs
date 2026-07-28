// Copyright 2015-2018 Aerospike, Inc.
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

use std::fmt;

/// Underlying data type of secondary index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexType {
    /// Numeric index.
    Numeric,

    /// String index.
    String,

    /// 2-dimensional spherical geospatial index.
    Geo2DSphere,

    /// Blob (byte-array) index. Requires server 7.0+, which a node reports
    /// through `Version::supports_blob_index`.
    ///
    /// Indexes bins holding [`Value::Blob`](crate::Value::Blob), which
    /// [`Filter::equal`](crate::query::Filter::equal) already accepts as
    /// `Vec<u8>` or `&[u8]`; without this variant such a filter had no index
    /// that could serve it.
    Blob,
}

/// Secondary index collection type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionIndexType {
    /// Normal, scalar index.
    Default = 0,

    /// Index list elements.
    List,

    /// Index map keys.
    MapKeys,

    /// Index map values.
    MapValues,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            IndexType::Numeric => "NUMERIC".fmt(f),
            IndexType::String => "STRING".fmt(f),
            IndexType::Geo2DSphere => "GEO2DSPHERE".fmt(f),
            IndexType::Blob => "BLOB".fmt(f),
        }
    }
}

impl fmt::Display for CollectionIndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            CollectionIndexType::Default => "DEFAULT".fmt(f),
            CollectionIndexType::List => "LIST".fmt(f),
            CollectionIndexType::MapKeys => "MAPKEYS".fmt(f),
            CollectionIndexType::MapValues => "MAPVALUES".fmt(f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CollectionIndexType, IndexType};

    #[test]
    fn index_type_wire_names() {
        // These strings go into the `sindex-create` info command verbatim, so
        // they are protocol, not cosmetics.
        assert_eq!(IndexType::Numeric.to_string(), "NUMERIC");
        assert_eq!(IndexType::String.to_string(), "STRING");
        assert_eq!(IndexType::Geo2DSphere.to_string(), "GEO2DSPHERE");
        assert_eq!(IndexType::Blob.to_string(), "BLOB");
    }

    #[test]
    fn collection_index_type_wire_names() {
        assert_eq!(CollectionIndexType::Default.to_string(), "DEFAULT");
        assert_eq!(CollectionIndexType::List.to_string(), "LIST");
        assert_eq!(CollectionIndexType::MapKeys.to_string(), "MAPKEYS");
        assert_eq!(CollectionIndexType::MapValues.to_string(), "MAPVALUES");
    }
}
