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

/// Wire-protocol field type identifiers. Values align with the server-side proto definitions.
pub enum FieldType {
    /// Namespace.
    Namespace = 0,
    /// Table (set).
    Table = 1,
    /// Record key.
    Key = 2,
    // RecordVersion = 3,
    /// Digest (Ripe).
    DigestRipe = 4,
    // MrtId = 5,
    // MrtDeadline = 6,
    /// Query/transaction ID (user-supplied, echoed back).
    QueryId = 7,
    /// Socket timeout.
    SocketTimeout = 9,
    /// Records per second limit.
    RecordsPerSecond = 10,
    /// PID array.
    PIDArray = 11,
    /// Digest array.
    DigestArray = 12,
    /// Max records.
    MaxRecords = 13,
    /// Bin value array.
    BValArray = 15,
    /// Index name.
    IndexName = 21,
    /// Index range.
    IndexRange = 22,
    /// Index context (packed CDT context bytes).
    IndexContext = 23,
    /// Index expression (packed expression bytes for expression-based SI).
    IndexExpression = 24,
    /// Index type.
    IndexType = 26,
    /// UDF package name.
    UdfPackageName = 30,
    /// UDF function name.
    UdfFunction = 31,
    /// UDF argument list.
    UdfArgList = 32,
    /// UDF operation.
    UdfOp = 33,
    // QueryBinList = 40,
    /// Batch index.
    BatchIndex = 41,
    // BatchIndexWithSet = 42,
    /// Filter expression.
    FilterExp = 43,
}
