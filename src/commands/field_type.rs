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

// FieldType signifies the database operation error codes.
// The positive numbers align with the server side file proto.h.

pub enum FieldType {
    Namespace = 0,
    Table = 1,
    Key = 2,
    // BIN  = 3,
    DigestRipe = 4,
    // GUID  = 5,
    // DigestRipeArray = 6,
    TranId = 7, // user supplied transaction id, which is simply passed back,
    // ScanOptions = 8,
    ScanTimeout = 9,
    PIDArray = 11,
    IndexName = 21,
    IndexRange = 22,
    // IndexFilter = 23,
    // IndexLimit = 24,
    // IndexOrderBy = 25,
    IndexType = 26,
    UdfPackageName = 30,
    UdfFunction = 31,
    UdfArgList = 32,
    UdfOp = 33,
    QueryBinList = 40,
    BatchIndex = 41,
    BatchIndexWithSet = 42,
    FilterExp = 43,
}
