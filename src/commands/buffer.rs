// Copyright 2015-2018 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::str;
use std::time::Duration;

use byteorder::{ByteOrder, NetworkEndian};

use errors::*;
use BatchRead;
use Bin;
use Bins;
use CollectionIndexType;
use Key;
use Statement;
use Value;
use batch::batch_executor::SharedSlice;
use commands::field_type::FieldType;
use msgpack::encoder;
use operations::{Operation, OperationBin, OperationData, OperationType};
use policy::{BatchPolicy, CommitLevel, ConsistencyLevel, GenerationPolicy, QueryPolicy,
             ReadPolicy, RecordExistsAction, ScanPolicy, WritePolicy};

// Contains a read operation.
const INFO1_READ: u8 = 1;

// Get all bins.
const INFO1_GET_ALL: u8 = (1 << 1);

// Batch read or exists.
const INFO1_BATCH: u8 = (1 << 3);

// Do not read the bins
const INFO1_NOBINDATA: u8 = (1 << 5);

// Involve all replicas in read operation.
const INFO1_CONSISTENCY_ALL: u8 = (1 << 6);

// Create or update record
const INFO2_WRITE: u8 = 1;

// Fling a record into the belly of Moloch.
const INFO2_DELETE: u8 = (1 << 1);

// Update if expected generation == old.
const INFO2_GENERATION: u8 = (1 << 2);

// Update if new generation >= old, good for restore.
const INFO2_GENERATION_GT: u8 = (1 << 3);

// Transaction resulting in record deletion leaves tombstone (Enterprise only).
const INFO2_DURABLE_DELETE: u8 = (1 << 4);

// Create only. Fail if record already exists.
const INFO2_CREATE_ONLY: u8 = (1 << 5);

// Return a result for every operation.
const INFO2_RESPOND_ALL_OPS: u8 = (1 << 7);

// This is the last of a multi-part message.
pub const INFO3_LAST: u8 = 1;

// Commit to master only before declaring success.
const INFO3_COMMIT_MASTER: u8 = (1 << 1);

// Update only. Merge bins.
const INFO3_UPDATE_ONLY: u8 = (1 << 3);

// Create or completely replace record.
const INFO3_CREATE_OR_REPLACE: u8 = (1 << 4);

// Completely replace existing record only.
const INFO3_REPLACE_ONLY: u8 = (1 << 5);

pub const MSG_TOTAL_HEADER_SIZE: u8 = 30;
const FIELD_HEADER_SIZE: u8 = 5;
const OPERATION_HEADER_SIZE: u8 = 8;
pub const MSG_REMAINING_HEADER_SIZE: u8 = 22;
const DIGEST_SIZE: u8 = 20;
const CL_MSG_VERSION: u8 = 2;
const AS_MSG_TYPE: u8 = 3;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers. Tweak this number if you are returning a lot of
// LDT elements in your queries.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

// Holds data buffer for the command
#[derive(Debug, Default)]
pub struct Buffer {
    pub data_buffer: Vec<u8>,
    pub data_offset: usize,
}

impl Buffer {
    pub fn new() -> Self {
        Buffer {
            data_buffer: Vec::with_capacity(1024),
            data_offset: 0,
        }
    }

    fn begin(&mut self) -> Result<()> {
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
        Ok(())
    }

    pub fn size_buffer(&mut self) -> Result<()> {
        let offset = self.data_offset;
        self.resize_buffer(offset)
    }

    pub fn resize_buffer(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            bail!("Invalid size for buffer: {}", size);
        }

        self.data_buffer.resize(size, 0);

        Ok(())
    }

    pub fn reset_offset(&mut self) -> Result<()> {
        // reset data offset
        self.data_offset = 0;
        Ok(())
    }

    pub fn end(&mut self) -> Result<()> {
        let size = ((self.data_offset - 8) as i64) | (((CL_MSG_VERSION as i64) << 56) as i64)
            | ((AS_MSG_TYPE as i64) << 48);

        // reset data offset
        try!(self.reset_offset());
        try!(self.write_i64(size));

        Ok(())
    }

    // Writes the command for write operations
    pub fn set_write<'b, A: AsRef<Bin<'b>>>(
        &mut self,
        policy: &WritePolicy,
        op_type: OperationType,
        key: &Key,
        bins: &[A],
    ) -> Result<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, policy.send_key));

        for bin in bins {
            try!(self.estimate_operation_size_for_bin(bin.as_ref()));
        }

        try!(self.size_buffer());
        try!(self.write_header_with_policy(
            policy,
            0,
            INFO2_WRITE,
            field_count as u16,
            bins.len() as u16
        ));
        try!(self.write_key(key, policy.send_key));

        for bin in bins {
            try!(self.write_operation_for_bin(bin.as_ref(), op_type));
        }

        self.end()
    }

    // Writes the command for write operations
    pub fn set_delete(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header_with_policy(
            policy,
            0,
            INFO2_WRITE | INFO2_DELETE,
            field_count as u16,
            0
        ));
        try!(self.write_key(key, false));
        self.end()
    }

    // Writes the command for touch operations
    pub fn set_touch(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, policy.send_key));

        try!(self.estimate_operation_size());
        try!(self.size_buffer());
        try!(self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count as u16, 1));
        try!(self.write_key(key, policy.send_key));
        try!(self.write_operation_for_operation_type(OperationType::Touch));
        self.end()
    }

    // Writes the command for exist operations
    pub fn set_exists(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header(
            &policy.base_policy,
            INFO1_READ | INFO1_NOBINDATA,
            0,
            field_count,
            0
        ));
        try!(self.write_key(key, false));
        self.end()
    }

    // Writes the command for get operations
    pub fn set_read<'a>(&mut self, policy: &ReadPolicy, key: &Key, bins: &Bins) -> Result<()> {
        match bins {
            &Bins::None => self.set_read_header(policy, key),
            &Bins::All => self.set_read_for_key_only(policy, key),
            &Bins::Some(ref bin_names) => {
                try!(self.begin());
                let field_count = try!(self.estimate_key_size(key, false));
                for bin_name in bin_names {
                    try!(self.estimate_operation_size_for_bin_name(&bin_name));
                }

                self.size_buffer()?;
                self.write_header(policy, INFO1_READ, 0, field_count, bin_names.len() as u16)?;
                self.write_key(key, false)?;
                for bin_name in bin_names {
                    self.write_operation_for_bin_name(&bin_name, OperationType::Read)?;
                }
                self.end()?;
                Ok(())
            }
        }
    }

    // Writes the command for getting metadata operations
    pub fn set_read_header(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.estimate_operation_size_for_bin_name(""));
        try!(self.size_buffer());
        try!(self.write_header(policy, INFO1_READ | INFO1_NOBINDATA, 0, field_count, 1));
        try!(self.write_key(key, false));
        try!(self.write_operation_for_bin_name("", OperationType::Read));
        self.end()
    }

    pub fn set_read_for_key_only(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        try!(self.begin());

        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header(policy, INFO1_READ | INFO1_GET_ALL, 0, field_count, 0));
        try!(self.write_key(key, false));

        self.end()
    }

    // Writes the command for batch read operations
    pub fn set_batch_read<'a>(
        &mut self,
        policy: &BatchPolicy,
        batch_reads: SharedSlice<BatchRead<'a>>,
        offsets: &[usize],
    ) -> Result<()> {
        let field_count = if policy.send_set_name { 2 } else { 1 };

        self.begin()?;
        self.data_offset += FIELD_HEADER_SIZE as usize + 5;

        let mut prev: Option<&BatchRead> = None;
        for idx in offsets {
            let batch_read: &BatchRead = batch_reads.get(*idx).unwrap();
            self.data_offset += batch_read.key.digest.len() + 4;
            match prev {
                Some(prev) if batch_read.match_header(prev, policy.send_set_name) => {
                    self.data_offset += 1;
                }
                _ => {
                    let key = &batch_read.key;
                    self.data_offset += key.namespace.len() + FIELD_HEADER_SIZE as usize + 6;
                    if policy.send_set_name {
                        self.data_offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
                    }
                    if let &Bins::Some(ref bin_names) = batch_read.bins {
                        for name in bin_names {
                            self.estimate_operation_size_for_bin_name(&name)?;
                        }
                    }
                }
            }
            prev = Some(batch_read);
        }

        self.size_buffer()?;
        self.write_header(
            &policy.base_policy,
            INFO1_READ | INFO1_BATCH,
            0,
            field_count,
            0,
        )?;

        let field_size_offset = self.data_offset;
        let field_type = if policy.send_set_name {
            FieldType::BatchIndexWithSet
        } else {
            FieldType::BatchIndex
        };
        self.write_field_header(0, field_type)?;
        self.write_u32(offsets.len() as u32)?;
        self.write_u8(if policy.allow_inline { 1 } else { 0 })?;

        prev = None;
        for idx in offsets {
            let batch_read = batch_reads.get(*idx).unwrap();
            let key = &batch_read.key;
            self.write_u32(*idx as u32)?;
            self.write_bytes(&key.digest)?;
            match prev {
                Some(ref prev) if batch_read.match_header(&prev, policy.send_set_name) => {
                    self.write_u8(1)?;
                }
                _ => {
                    self.write_u8(0)?;
                    match batch_read.bins {
                        &Bins::None => {
                            self.write_u8(INFO1_READ | INFO1_NOBINDATA)?;
                            self.write_u16(field_count)?;
                            self.write_u16(0)?;
                            self.write_field_string(&key.namespace, FieldType::Namespace)?;
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table)?;
                            }
                        }
                        &Bins::All => {
                            self.write_u8(INFO1_READ | INFO1_GET_ALL)?;
                            self.write_u16(field_count)?;
                            self.write_u16(0)?;
                            self.write_field_string(&key.namespace, FieldType::Namespace)?;
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table)?;
                            }
                        }
                        &Bins::Some(ref bin_names) => {
                            self.write_u8(INFO1_READ)?;
                            self.write_u16(field_count)?;
                            self.write_u16(bin_names.len() as u16)?;
                            self.write_field_string(&key.namespace, FieldType::Namespace)?;
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table)?;
                            }
                            for bin in bin_names {
                                self.write_operation_for_bin_name(&bin, OperationType::Read)?;
                            }
                        }
                    }
                }
            }
            prev = Some(batch_read);
        }

        let field_size = self.data_offset - MSG_TOTAL_HEADER_SIZE as usize - 4;
        NetworkEndian::write_u32(
            &mut self.data_buffer[field_size_offset..field_size_offset + 4],
            field_size as u32,
        );

        self.end()
    }

    // Writes the command for getting metadata operations
    pub fn set_operate<'a>(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &'a [Operation<'a>],
    ) -> Result<()> {
        try!(self.begin());

        let mut read_attr = 0;
        let mut write_attr = 0;

        for operation in operations {
            match *operation {
                Operation {
                    op: OperationType::Read,
                    bin: OperationBin::None,
                    ..
                } => read_attr |= INFO1_READ | INFO1_NOBINDATA,
                Operation {
                    op: OperationType::Read,
                    bin: OperationBin::All,
                    ..
                } => read_attr |= INFO1_READ | INFO1_GET_ALL,
                Operation {
                    op: OperationType::Read,
                    ..
                } => read_attr |= INFO1_READ,
                Operation {
                    op: OperationType::CdtRead,
                    ..
                } => read_attr |= INFO1_READ,
                _ => write_attr |= INFO2_WRITE,
            }

            let map_op = match operation.data {
                OperationData::CdtMapOp(_) => true,
                _ => false,
            };
            if policy.respond_per_each_op || map_op {
                write_attr |= INFO2_RESPOND_ALL_OPS;
            }

            self.data_offset += try!(operation.estimate_size()) + OPERATION_HEADER_SIZE as usize;
        }

        let field_count = try!(self.estimate_key_size(key, policy.send_key && write_attr != 0));

        try!(self.size_buffer());

        if write_attr != 0 {
            try!(self.write_header_with_policy(
                policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16
            ));
        } else {
            try!(self.write_header(
                &policy.base_policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16
            ));
        }
        try!(self.write_key(key, policy.send_key && write_attr != 0));

        for operation in operations {
            try!(operation.write_to(self));
        }

        self.end()
    }

    pub fn set_udf(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<()> {
        try!(self.begin());

        let mut field_count = try!(self.estimate_key_size(key, policy.send_key));
        field_count += try!(self.estimate_udf_size(package_name, function_name, args)) as u16;

        try!(self.size_buffer());

        try!(self.write_header(&policy.base_policy, 0, INFO2_WRITE, field_count, 0));
        try!(self.write_key(key, policy.send_key));
        try!(self.write_field_string(package_name, FieldType::UdfPackageName));
        try!(self.write_field_string(function_name, FieldType::UdfFunction));
        try!(self.write_args(args, FieldType::UdfArgList));
        self.end()
    }

    pub fn set_scan(
        &mut self,
        policy: &ScanPolicy,
        namespace: &str,
        set_name: &str,
        bins: &Bins,
        task_id: u64,
    ) -> Result<()> {
        try!(self.begin());

        let mut field_count = 0;
        if namespace != "" {
            self.data_offset += namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if set_name != "" {
            self.data_offset += set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // Estimate scan options size.
        self.data_offset += 2 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Estimate scan timeout size.
        self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Allocate space for task_id field.
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        let bin_count = match *bins {
            Bins::All => 0,
            Bins::None => 0,
            Bins::Some(ref bin_names) => {
                for bin_name in bin_names {
                    try!(self.estimate_operation_size_for_bin_name(&bin_name));
                }
                bin_names.len()
            }
        };

        try!(self.size_buffer());

        let mut read_attr = INFO1_READ;
        if bins.is_none() {
            read_attr |= INFO1_NOBINDATA;
        }

        try!(self.write_header(
            &policy.base_policy,
            read_attr,
            0,
            field_count,
            bin_count as u16
        ));

        if namespace != "" {
            try!(self.write_field_string(namespace, FieldType::Namespace));
        }

        if set_name != "" {
            try!(self.write_field_string(set_name, FieldType::Table));
        }

        try!(self.write_field_header(2, FieldType::ScanOptions));

        let mut priority: u8 = policy.base_policy.priority.clone() as u8;
        priority <<= 4;

        if policy.fail_on_cluster_change {
            priority |= 0x08;
        }

        try!(self.write_u8(priority));
        try!(self.write_u8(policy.scan_percent));

        // Write scan timeout
        try!(self.write_field_header(4, FieldType::ScanTimeout));
        try!(self.write_u32(policy.socket_timeout));

        try!(self.write_field_header(8, FieldType::TranId));
        try!(self.write_u64(task_id));

        if let Bins::Some(ref bin_names) = *bins {
            for bin_name in bin_names {
                try!(self.write_operation_for_bin_name(&bin_name, OperationType::Read));
            }
        }

        self.end()
    }

    pub fn set_query(
        &mut self,
        policy: &QueryPolicy,
        statement: &Statement,
        write: bool,
        task_id: u64,
    ) -> Result<()> {
        let filter = match statement.filters {
            Some(ref filters) => Some(&filters[0]),
            None => None,
        };

        try!(self.begin());

        let mut field_count = 0;
        let mut filter_size = 0;
        let mut bin_name_size = 0;

        if statement.namespace != "" {
            self.data_offset += statement.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if statement.set_name != "" {
            self.data_offset += statement.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if let Some(ref index_name) = statement.index_name {
            if index_name != "" {
                self.data_offset += index_name.len() + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }
        }

        // Allocate space for TaskId field.
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type();
            if idx_type != CollectionIndexType::Default {
                self.data_offset += 1 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            filter_size = 1 + filter.estimate_size()?;
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;

            if let Bins::Some(ref bin_names) = statement.bins {
                self.data_offset += FIELD_HEADER_SIZE as usize;
                bin_name_size += 1;

                for bin_name in bin_names {
                    bin_name_size += bin_name.len() + 1;
                }

                self.data_offset += bin_name_size;
                field_count += 1;
            }
        } else {
            // Calling query with no filters is more efficiently handled by a primary index scan.
            // Estimate scan options size.
            self.data_offset += 2 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if let Some(ref aggregation) = statement.aggregation {
            self.data_offset += 1 + FIELD_HEADER_SIZE as usize; // udf type
            self.data_offset += aggregation.package_name.len() + FIELD_HEADER_SIZE as usize;
            self.data_offset += aggregation.function_name.len() + FIELD_HEADER_SIZE as usize;

            if let Some(ref args) = aggregation.function_args {
                try!(self.estimate_args_size(Some(&args)));
            } else {
                try!(self.estimate_args_size(None));
            }
            field_count += 4;
        }

        if statement.is_scan() {
            if let Bins::Some(ref bin_names) = statement.bins {
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(&bin_name)?;
                }
            }
        }

        try!(self.size_buffer());

        let mut operation_count: usize = 0;
        if statement.is_scan() {
            if let Bins::Some(ref bin_names) = statement.bins {
                operation_count += bin_names.len();
            }
        }

        let info1 = if statement.bins.is_none() {
            INFO1_READ | INFO1_NOBINDATA
        } else {
            INFO1_READ
        };
        let info2 = if write { INFO2_WRITE } else { 0 };

        self.write_header(
            &policy.base_policy,
            info1,
            info2,
            field_count,
            operation_count as u16,
        )?;

        if statement.namespace != "" {
            try!(self.write_field_string(&statement.namespace, FieldType::Namespace));
        }

        if let Some(ref index_name) = statement.index_name {
            if !index_name.is_empty() {
                try!(self.write_field_string(&index_name, FieldType::IndexName));
            }
        }

        if statement.set_name != "" {
            try!(self.write_field_string(&statement.set_name, FieldType::Table));
        }

        try!(self.write_field_header(8, FieldType::TranId));
        try!(self.write_u64(task_id));

        if filter.is_some() {
            let filter = filter.unwrap();
            let idx_type = filter.collection_index_type();

            if idx_type != CollectionIndexType::Default {
                try!(self.write_field_header(1, FieldType::IndexType));
                try!(self.write_u8(idx_type as u8));
            }

            try!(self.write_field_header(filter_size, FieldType::IndexRange));
            try!(self.write_u8(1));

            try!(filter.write(self));

            if let Bins::Some(ref bin_names) = statement.bins {
                if !bin_names.is_empty() {
                    try!(self.write_field_header(bin_name_size, FieldType::QueryBinList));
                    try!(self.write_u8(bin_names.len() as u8));

                    for bin_name in bin_names {
                        try!(self.write_u8(bin_name.len() as u8));
                        try!(self.write_str(&bin_name));
                    }
                }
            }
        } else {
            // Calling query with no filters is more efficiently handled by a primary index scan.
            try!(self.write_field_header(2, FieldType::ScanOptions));
            let priority: u8 = (policy.base_policy.priority.clone() as u8) << 4;
            try!(self.write_u8(priority));
            try!(self.write_u8(100));
        }

        if let Some(ref aggregation) = statement.aggregation {
            try!(self.write_field_header(1, FieldType::UdfOp));
            if statement.bins.is_none() {
                try!(self.write_u8(2));
            } else {
                try!(self.write_u8(1));
            }

            try!(self.write_field_string(&aggregation.package_name, FieldType::UdfPackageName));
            try!(self.write_field_string(&aggregation.function_name, FieldType::UdfFunction));
            if let Some(ref args) = aggregation.function_args {
                try!(self.write_args(Some(args), FieldType::UdfArgList));
            } else {
                try!(self.write_args(None, FieldType::UdfArgList));
            }
        }

        // scan binNames come last
        if statement.is_scan() {
            if let Bins::Some(ref bin_names) = statement.bins {
                for bin_name in bin_names {
                    try!(self.write_operation_for_bin_name(&bin_name, OperationType::Read));
                }
            }
        }

        self.end()
    }

    fn estimate_key_size(&mut self, key: &Key, send_key: bool) -> Result<u16> {
        let mut field_count: u16 = 0;

        if key.namespace != "" {
            self.data_offset += key.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if key.set_name != "" {
            self.data_offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        self.data_offset += (DIGEST_SIZE + FIELD_HEADER_SIZE) as usize;
        field_count += 1;

        if send_key {
            if let Some(ref user_key) = key.user_key {
                // field header size + key size
                self.data_offset += try!(user_key.estimate_size()) + FIELD_HEADER_SIZE as usize + 1;
                field_count += 1;
            }
        }

        Ok(field_count)
    }

    fn estimate_args_size(&mut self, args: Option<&[Value]>) -> Result<()> {
        if let Some(args) = args {
            self.data_offset +=
                try!(encoder::pack_array(&mut None, args)) + FIELD_HEADER_SIZE as usize;
        } else {
            self.data_offset +=
                try!(encoder::pack_empty_args_array(&mut None)) + FIELD_HEADER_SIZE as usize;
        }
        Ok(())
    }

    fn estimate_udf_size(
        &mut self,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<usize> {
        self.data_offset += package_name.len() + FIELD_HEADER_SIZE as usize;
        self.data_offset += function_name.len() + FIELD_HEADER_SIZE as usize;
        try!(self.estimate_args_size(args));
        Ok(3)
    }

    fn estimate_operation_size_for_bin(&mut self, bin: &Bin) -> Result<()> {
        self.data_offset += bin.name.len() + OPERATION_HEADER_SIZE as usize;
        self.data_offset += try!(bin.value.estimate_size());
        Ok(())
    }

    fn estimate_operation_size_for_bin_name(&mut self, bin_name: &str) -> Result<()> {
        self.data_offset += bin_name.len() + OPERATION_HEADER_SIZE as usize;
        Ok(())
    }

    fn estimate_operation_size(&mut self) -> Result<()> {
        self.data_offset += OPERATION_HEADER_SIZE as usize;
        Ok(())
    }

    fn write_header(
        &mut self,
        policy: &ReadPolicy,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) -> Result<()> {
        let mut read_attr = read_attr;

        if policy.consistency_level == ConsistencyLevel::ConsistencyAll {
            read_attr |= INFO1_CONSISTENCY_ALL;
        }

        // Write all header data except total size which must be written last.
        self.data_buffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        self.data_buffer[9] = read_attr;
        self.data_buffer[10] = write_attr;

        for i in 11..26 {
            self.data_buffer[i] = 0;
        }

        self.data_offset = 26;
        try!(self.write_u16(field_count as u16));
        try!(self.write_u16(operation_count as u16));

        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        Ok(())
    }

    // Header write for write operations.
    fn write_header_with_policy(
        &mut self,
        policy: &WritePolicy,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) -> Result<()> {
        // Set flags.
        let mut generation: u32 = 0;
        let mut info_attr: u8 = 0;
        let mut read_attr = read_attr;
        let mut write_attr = write_attr;

        match policy.record_exists_action {
            RecordExistsAction::Update => (),
            RecordExistsAction::UpdateOnly => info_attr |= INFO3_UPDATE_ONLY,
            RecordExistsAction::Replace => info_attr |= INFO3_CREATE_OR_REPLACE,
            RecordExistsAction::ReplaceOnly => info_attr |= INFO3_REPLACE_ONLY,
            RecordExistsAction::CreateOnly => write_attr |= INFO2_CREATE_ONLY,
        }

        match policy.generation_policy {
            GenerationPolicy::None => (),
            GenerationPolicy::ExpectGenEqual => {
                generation = policy.generation;
                write_attr |= INFO2_GENERATION;
            }
            GenerationPolicy::ExpectGenGreater => {
                generation = policy.generation;
                write_attr |= INFO2_GENERATION_GT;
            }
        }

        if policy.commit_level == CommitLevel::CommitMaster {
            info_attr |= INFO3_COMMIT_MASTER
        }

        if policy.base_policy.consistency_level == ConsistencyLevel::ConsistencyAll {
            read_attr |= INFO1_CONSISTENCY_ALL
        }

        if policy.durable_delete {
            write_attr |= INFO2_DURABLE_DELETE
        }

        // Write all header data except total size which must be written last.
        self.data_offset = 8;
        try!(self.write_u8(MSG_REMAINING_HEADER_SIZE)); // Message header length.
        try!(self.write_u8(read_attr));
        try!(self.write_u8(write_attr));
        try!(self.write_u8(info_attr));
        try!(self.write_u8(0)); // unused
        try!(self.write_u8(0)); // clear the result code

        try!(self.write_u32(generation));
        try!(self.write_u32(policy.expiration.into()));

        // Initialize timeout. It will be written later.
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_u8(0));

        try!(self.write_u16(field_count));
        try!(self.write_u16(operation_count));
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        Ok(())
    }

    fn write_key(&mut self, key: &Key, send_key: bool) -> Result<()> {
        // Write key into buffer.
        if key.namespace != "" {
            try!(self.write_field_string(&key.namespace, FieldType::Namespace));
        }

        if key.set_name != "" {
            try!(self.write_field_string(&key.set_name, FieldType::Table));
        }

        try!(self.write_field_bytes(&key.digest, FieldType::DigestRipe));

        if send_key {
            if let Some(ref user_key) = key.user_key {
                try!(self.write_field_value(user_key, FieldType::Key));
            }
        }

        Ok(())
    }

    fn write_field_header(&mut self, size: usize, ftype: FieldType) -> Result<()> {
        try!(self.write_i32(size as i32 + 1));
        try!(self.write_u8(ftype as u8));

        Ok(())
    }

    fn write_field_string(&mut self, field: &str, ftype: FieldType) -> Result<()> {
        try!(self.write_field_header(field.len(), ftype));
        try!(self.write_str(field));

        Ok(())
    }

    fn write_field_bytes(&mut self, bytes: &[u8], ftype: FieldType) -> Result<()> {
        try!(self.write_field_header(bytes.len(), ftype));
        try!(self.write_bytes(bytes));

        Ok(())
    }

    fn write_field_value(&mut self, value: &Value, ftype: FieldType) -> Result<()> {
        try!(self.write_field_header(try!(value.estimate_size()) + 1, ftype));
        try!(self.write_u8(value.particle_type() as u8));
        try!(value.write_to(self));

        Ok(())
    }

    fn write_args(&mut self, args: Option<&[Value]>, ftype: FieldType) -> Result<()> {
        if let Some(args) = args {
            try!(self.write_field_header(try!(encoder::pack_array(&mut None, args)), ftype));
            try!(encoder::pack_array(&mut Some(self), args));
        } else {
            try!(self.write_field_header(try!(encoder::pack_empty_args_array(&mut None)), ftype));
            try!(encoder::pack_empty_args_array(&mut Some(self)));
        }

        Ok(())
    }

    fn write_operation_for_bin(&mut self, bin: &Bin, op_type: OperationType) -> Result<()> {
        let name_length = bin.name.len();
        let value_length = try!(bin.value.estimate_size());

        try!(self.write_i32((name_length + value_length + 4) as i32));
        try!(self.write_u8(op_type as u8));
        try!(self.write_u8(bin.value.particle_type() as u8));
        try!(self.write_u8(0));
        try!(self.write_u8(name_length as u8));
        try!(self.write_str(bin.name));
        try!(bin.value.write_to(self));

        Ok(())
    }

    fn write_operation_for_bin_name(&mut self, name: &str, op_type: OperationType) -> Result<()> {
        try!(self.write_i32(name.len() as i32 + 4));
        try!(self.write_u8(op_type as u8));
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_u8(name.len() as u8));
        try!(self.write_str(name));

        Ok(())
    }

    fn write_operation_for_operation_type(&mut self, op_type: OperationType) -> Result<()> {
        try!(self.write_i32(4));
        try!(self.write_u8(op_type as u8));
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_u8(0));

        Ok(())
    }

    // Data buffer implementations

    pub fn data_offset(&self) -> usize {
        self.data_offset
    }

    pub fn skip_bytes(&mut self, count: usize) {
        self.data_offset += count;
    }

    pub fn skip(&mut self, count: usize) -> Result<()> {
        self.data_offset += count;
        Ok(())
    }

    pub fn peek(&self) -> u8 {
        self.data_buffer[self.data_offset]
    }

    pub fn read_u8(&mut self, pos: Option<usize>) -> Result<u8> {
        match pos {
            Some(pos) => Ok(self.data_buffer[pos]),
            None => {
                let res = self.data_buffer[self.data_offset];
                self.data_offset += 1;
                Ok(res)
            }
        }
    }

    pub fn read_i8(&mut self, pos: Option<usize>) -> Result<i8> {
        match pos {
            Some(pos) => Ok(self.data_buffer[pos] as i8),
            None => {
                let res = self.data_buffer[self.data_offset] as i8;
                self.data_offset += 1;
                Ok(res)
            }
        }
    }

    pub fn read_u16(&mut self, pos: Option<usize>) -> Result<u16> {
        let len = 2;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u16(&self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u16(
                    &self.data_buffer[self.data_offset..self.data_offset + len],
                );
                self.data_offset += len;
                Ok(res)
            }
        }
    }

    pub fn read_i16(&mut self, pos: Option<usize>) -> Result<i16> {
        let val = try!(self.read_u16(pos));
        Ok(val as i16)
    }

    pub fn read_u32(&mut self, pos: Option<usize>) -> Result<u32> {
        let len = 4;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u32(&self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u32(
                    &self.data_buffer[self.data_offset..self.data_offset + len],
                );
                self.data_offset += len;
                Ok(res)
            }
        }
    }

    pub fn read_i32(&mut self, pos: Option<usize>) -> Result<i32> {
        let val = try!(self.read_u32(pos));
        Ok(val as i32)
    }

    pub fn read_u64(&mut self, pos: Option<usize>) -> Result<u64> {
        let len = 8;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u64(&self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u64(
                    &self.data_buffer[self.data_offset..self.data_offset + len],
                );
                self.data_offset += len;
                Ok(res)
            }
        }
    }

    pub fn read_i64(&mut self, pos: Option<usize>) -> Result<i64> {
        let val = try!(self.read_u64(pos));
        Ok(val as i64)
    }

    pub fn read_msg_size(&mut self, pos: Option<usize>) -> Result<usize> {
        let size = try!(self.read_i64(pos));
        let size = size & 0xFFFFFFFFFFFF;
        Ok(size as usize)
    }

    pub fn read_f32(&mut self, pos: Option<usize>) -> Result<f32> {
        let len = 4;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_f32(&self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_f32(
                    &self.data_buffer[self.data_offset..self.data_offset + len],
                );
                self.data_offset += len;
                Ok(res)
            }
        }
    }

    pub fn read_f64(&mut self, pos: Option<usize>) -> Result<f64> {
        let len = 8;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_f64(&self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_f64(
                    &self.data_buffer[self.data_offset..self.data_offset + len],
                );
                self.data_offset += len;
                Ok(res)
            }
        }
    }

    pub fn read_str(&mut self, len: usize) -> Result<String> {
        let s = try!(str::from_utf8(
            &self.data_buffer[self.data_offset..self.data_offset + len]
        ));
        self.data_offset += len;
        Ok(s.to_owned())
    }

    pub fn read_bytes(&mut self, pos: usize, count: usize) -> Result<&[u8]> {
        Ok(&self.data_buffer[pos..pos + count])
    }

    pub fn read_slice(&mut self, count: usize) -> Result<&[u8]> {
        Ok(&self.data_buffer[self.data_offset..self.data_offset + count])
    }

    pub fn read_blob(&mut self, len: usize) -> Result<Vec<u8>> {
        let val = self.data_buffer[self.data_offset..self.data_offset + len].to_vec();
        self.data_offset += len;
        Ok(val)
    }

    pub fn write_u8(&mut self, val: u8) -> Result<usize> {
        self.data_buffer[self.data_offset] = val;
        self.data_offset += 1;
        Ok(1)
    }

    pub fn write_i8(&mut self, val: i8) -> Result<usize> {
        self.data_buffer[self.data_offset] = val as u8;
        self.data_offset += 1;
        Ok(1)
    }

    pub fn write_u16(&mut self, val: u16) -> Result<usize> {
        NetworkEndian::write_u16(
            &mut self.data_buffer[self.data_offset..self.data_offset + 2],
            val,
        );
        self.data_offset += 2;
        Ok(2)
    }

    pub fn write_i16(&mut self, val: i16) -> Result<usize> {
        self.write_u16(val as u16)
    }

    pub fn write_u32(&mut self, val: u32) -> Result<usize> {
        NetworkEndian::write_u32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        Ok(4)
    }

    pub fn write_i32(&mut self, val: i32) -> Result<usize> {
        self.write_u32(val as u32)
    }

    pub fn write_u64(&mut self, val: u64) -> Result<usize> {
        NetworkEndian::write_u64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        Ok(8)
    }

    pub fn write_i64(&mut self, val: i64) -> Result<usize> {
        self.write_u64(val as u64)
    }

    pub fn write_bool(&mut self, val: bool) -> Result<usize> {
        let val = if val { 1 } else { 0 };
        self.write_i64(val)
    }

    pub fn write_f32(&mut self, val: f32) -> Result<usize> {
        NetworkEndian::write_f32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        Ok(4)
    }

    pub fn write_f64(&mut self, val: f64) -> Result<usize> {
        NetworkEndian::write_f64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        Ok(8)
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<usize> {
        for b in bytes {
            try!(self.write_u8(*b));
        }
        Ok(bytes.len())
    }

    pub fn write_str(&mut self, val: &str) -> Result<usize> {
        self.write_bytes(val.as_bytes())
    }

    pub fn write_geo(&mut self, val: &str) -> Result<usize> {
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_u8(0));
        try!(self.write_bytes(val.as_bytes()));
        Ok(3 + val.len())
    }

    pub fn write_timeout(&mut self, val: Option<Duration>) {
        if let Some(val) = val {
            let millis: i32 =
                (val.as_secs() * 1_000) as i32 + (val.subsec_nanos() / 1_000_000) as i32;
            NetworkEndian::write_i32(&mut self.data_buffer[22..22 + 4], millis);
        }
    }

    pub fn dump_buffer(&self) {
        println!(">>>>>>>>>>>>>>> {:?}", self.data_buffer.to_vec());
    }
}
