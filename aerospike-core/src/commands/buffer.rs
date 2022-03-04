// Copyright 2015-2020 Aerospike, Inc.
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

use byteorder::{ByteOrder, LittleEndian, NetworkEndian};

use crate::commands::field_type::FieldType;
use crate::errors::Result;
use crate::expressions::FilterExpression;
use crate::msgpack::encoder;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::policy::{
    BatchPolicy, CommitLevel, ConsistencyLevel, GenerationPolicy, QueryPolicy, ReadPolicy,
    RecordExistsAction, ScanPolicy, WritePolicy,
};
use crate::{BatchRead, Bin, Bins, CollectionIndexType, Key, Statement, Value};

// Contains a read operation.
const INFO1_READ: u8 = 1;

// Get all bins.
const INFO1_GET_ALL: u8 = 1 << 1;

// Batch read or exists.
const INFO1_BATCH: u8 = 1 << 3;

// Do not read the bins
const INFO1_NOBINDATA: u8 = 1 << 5;

// Involve all replicas in read operation.
const INFO1_CONSISTENCY_ALL: u8 = 1 << 6;

// Create or update record
const INFO2_WRITE: u8 = 1;

// Fling a record into the belly of Moloch.
const INFO2_DELETE: u8 = 1 << 1;

// Update if expected generation == old.
const INFO2_GENERATION: u8 = 1 << 2;

// Update if new generation >= old, good for restore.
const INFO2_GENERATION_GT: u8 = 1 << 3;

// Transaction resulting in record deletion leaves tombstone (Enterprise only).
const INFO2_DURABLE_DELETE: u8 = 1 << 4;

// Create only. Fail if record already exists.
const INFO2_CREATE_ONLY: u8 = 1 << 5;

// Return a result for every operation.
const INFO2_RESPOND_ALL_OPS: u8 = 1 << 7;

// This is the last of a multi-part message.
pub const INFO3_LAST: u8 = 1;

// Commit to master only before declaring success.
const INFO3_COMMIT_MASTER: u8 = 1 << 1;

// Partition is complete response in scan.
pub const _INFO3_PARTITION_DONE: u8 = 1 << 2;

// Update only. Merge bins.
const INFO3_UPDATE_ONLY: u8 = 1 << 3;

// Create or completely replace record.
const INFO3_CREATE_OR_REPLACE: u8 = 1 << 4;

// Completely replace existing record only.
const INFO3_REPLACE_ONLY: u8 = 1 << 5;

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
    pub reclaim_threshold: usize,
}

impl Buffer {
    pub fn new(reclaim_threshold: usize) -> Self {
        Buffer {
            data_buffer: Vec::with_capacity(1024),
            data_offset: 0,
            reclaim_threshold,
        }
    }

    fn begin(&mut self) {
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
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

        let mem_size = self.data_buffer.capacity();
        self.data_buffer.resize(size, 0);
        if mem_size > self.reclaim_threshold && size < mem_size {
            self.data_buffer.shrink_to_fit();
        }

        Ok(())
    }

    pub fn reset_offset(&mut self) {
        // reset data offset
        self.data_offset = 0;
    }

    pub fn end(&mut self) {
        let size = ((self.data_offset - 8) as i64)
            | ((i64::from(CL_MSG_VERSION) << 56) as i64)
            | (i64::from(AS_MSG_TYPE) << 48);

        // reset data offset
        self.reset_offset();
        self.write_i64(size);
    }

    // Writes the command for write operations
    pub fn set_write<'b, A: AsRef<Bin<'b>>>(
        &mut self,
        policy: &WritePolicy,
        op_type: OperationType,
        key: &Key,
        bins: &[A],
    ) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, policy.send_key);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        for bin in bins {
            self.estimate_operation_size_for_bin(bin.as_ref());
        }

        self.size_buffer()?;
        self.write_header_with_policy(
            policy,
            0,
            INFO2_WRITE,
            field_count as u16,
            bins.len() as u16,
        );
        self.write_key(key, policy.send_key);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }
        for bin in bins {
            self.write_operation_for_bin(bin.as_ref(), op_type);
        }

        self.end();
        Ok(())
    }

    // Writes the command for write operations
    pub fn set_delete(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        self.size_buffer()?;
        self.write_header_with_policy(policy, 0, INFO2_WRITE | INFO2_DELETE, field_count as u16, 0);
        self.write_key(key, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    // Writes the command for touch operations
    pub fn set_touch(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, policy.send_key);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }
        self.estimate_operation_size();
        self.size_buffer()?;
        self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count as u16, 1);
        self.write_key(key, policy.send_key);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_operation_for_operation_type(OperationType::Touch);
        self.end();
        Ok(())
    }

    // Writes the command for exist operations
    pub fn set_exists(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        self.size_buffer()?;
        self.write_header(
            &policy.base_policy,
            INFO1_READ | INFO1_NOBINDATA,
            0,
            field_count,
            0,
        );
        self.write_key(key, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    // Writes the command for get operations
    pub fn set_read(&mut self, policy: &ReadPolicy, key: &Key, bins: &Bins) -> Result<()> {
        match bins {
            Bins::None => self.set_read_header(policy, key),
            Bins::All => self.set_read_for_key_only(policy, key),
            Bins::Some(ref bin_names) => {
                self.begin();
                let mut field_count = self.estimate_key_size(key, false);
                let filter_size = self.estimate_filter_size(policy.filter_expression());
                if filter_size > 0 {
                    field_count += 1;
                }
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name);
                }

                self.size_buffer()?;
                self.write_header(policy, INFO1_READ, 0, field_count, bin_names.len() as u16);
                self.write_key(key, false);

                if let Some(filter) = policy.filter_expression() {
                    self.write_filter_expression(filter, filter_size);
                }

                for bin_name in bin_names {
                    self.write_operation_for_bin_name(bin_name, OperationType::Read);
                }

                self.end();
                Ok(())
            }
        }
    }

    // Writes the command for getting metadata operations
    pub fn set_read_header(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        self.estimate_operation_size_for_bin_name("");
        self.size_buffer()?;
        self.write_header(policy, INFO1_READ | INFO1_NOBINDATA, 0, field_count, 1);
        self.write_key(key, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_operation_for_bin_name("", OperationType::Read);
        self.end();
        Ok(())
    }

    pub fn set_read_for_key_only(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        self.begin();

        let mut field_count = self.estimate_key_size(key, false);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        self.size_buffer()?;
        self.write_header(policy, INFO1_READ | INFO1_GET_ALL, 0, field_count, 0);
        self.write_key(key, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    // Writes the command for batch read operations
    pub fn set_batch_read(
        &mut self,
        policy: &BatchPolicy,
        batch_reads: Vec<BatchRead>,
    ) -> Result<()> {
        let field_count_row = if policy.send_set_name { 2 } else { 1 };

        self.begin();
        let mut field_count = 1;
        self.data_offset += FIELD_HEADER_SIZE as usize + 5;

        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        let mut prev: Option<&BatchRead> = None;
        for batch_read in &batch_reads {
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
                    if let Bins::Some(ref bin_names) = batch_read.bins {
                        for name in bin_names {
                            self.estimate_operation_size_for_bin_name(name);
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
        );

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        let field_size_offset = self.data_offset;
        let field_type = if policy.send_set_name {
            FieldType::BatchIndexWithSet
        } else {
            FieldType::BatchIndex
        };
        self.write_field_header(0, field_type);
        self.write_u32(batch_reads.len() as u32);
        self.write_u8(if policy.allow_inline { 1 } else { 0 });

        prev = None;
        for (idx, batch_read) in batch_reads.iter().enumerate() {
            let key = &batch_read.key;
            self.write_u32(idx as u32);
            self.write_bytes(&key.digest);
            match prev {
                Some(prev) if batch_read.match_header(prev, policy.send_set_name) => {
                    self.write_u8(1);
                }
                _ => {
                    self.write_u8(0);
                    match batch_read.bins {
                        Bins::None => {
                            self.write_u8(INFO1_READ | INFO1_NOBINDATA);
                            self.write_u16(field_count_row);
                            self.write_u16(0);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                        }
                        Bins::All => {
                            self.write_u8(INFO1_READ | INFO1_GET_ALL);
                            self.write_u16(field_count_row);
                            self.write_u16(0);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                        }
                        Bins::Some(ref bin_names) => {
                            self.write_u8(INFO1_READ);
                            self.write_u16(field_count_row);
                            self.write_u16(bin_names.len() as u16);
                            self.write_field_string(&key.namespace, FieldType::Namespace);
                            if policy.send_set_name {
                                self.write_field_string(&key.set_name, FieldType::Table);
                            }
                            for bin in bin_names {
                                self.write_operation_for_bin_name(bin, OperationType::Read);
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

        self.end();
        Ok(())
    }

    // Writes the command for getting metadata operations
    pub fn set_operate<'a>(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &'a [Operation<'a>],
    ) -> Result<()> {
        self.begin();

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
                    op:
                        OperationType::Read
                        | OperationType::CdtRead
                        | OperationType::BitRead
                        | OperationType::HllRead
                        | OperationType::ExpRead,
                    ..
                } => read_attr |= INFO1_READ,
                _ => write_attr |= INFO2_WRITE,
            }

            let each_op = matches!(
                operation.data,
                OperationData::CdtMapOp(_)
                    | OperationData::CdtBitOp(_)
                    | OperationData::HLLOp(_)
                    | OperationData::EXPOp(_)
            );

            if policy.respond_per_each_op || each_op {
                write_attr |= INFO2_RESPOND_ALL_OPS;
            }

            self.data_offset += operation.estimate_size() + OPERATION_HEADER_SIZE as usize;
        }

        let mut field_count = self.estimate_key_size(key, policy.send_key && write_attr != 0);
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }
        self.size_buffer()?;

        if write_attr == 0 {
            self.write_header(
                &policy.base_policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        } else {
            self.write_header_with_policy(
                policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        }
        self.write_key(key, policy.send_key && write_attr != 0);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        for operation in operations {
            operation.write_to(self);
        }
        self.end();
        Ok(())
    }

    pub fn set_udf(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<()> {
        self.begin();

        let mut field_count = self.estimate_key_size(key, policy.send_key);
        field_count += self.estimate_udf_size(package_name, function_name, args) as u16;
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }
        self.size_buffer()?;

        self.write_header(&policy.base_policy, 0, INFO2_WRITE, field_count, 0);
        self.write_key(key, policy.send_key);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_field_string(package_name, FieldType::UdfPackageName);
        self.write_field_string(function_name, FieldType::UdfFunction);
        self.write_args(args, FieldType::UdfArgList);
        self.end();
        Ok(())
    }

    pub fn set_scan(
        &mut self,
        policy: &ScanPolicy,
        namespace: &str,
        set_name: &str,
        bins: &Bins,
        task_id: u64,
        partitions: &[u16],
    ) -> Result<()> {
        self.begin();

        let mut field_count = 0;
        let filter_size = self.estimate_filter_size(policy.filter_expression());
        if filter_size > 0 {
            field_count += 1;
        }

        if !namespace.is_empty() {
            self.data_offset += namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !set_name.is_empty() {
            self.data_offset += set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // // Estimate scan options size.
        // self.data_offset += 2 + FIELD_HEADER_SIZE as usize;
        // field_count += 1;

        // Estimate pid size
        self.data_offset += partitions.len() * 2 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Estimate scan timeout size.
        self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Allocate space for task_id field.
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        let bin_count = match *bins {
            Bins::All | Bins::None => 0,
            Bins::Some(ref bin_names) => {
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name);
                }
                bin_names.len()
            }
        };

        self.size_buffer()?;

        let mut read_attr = INFO1_READ;
        if bins.is_none() {
            read_attr |= INFO1_NOBINDATA;
        }

        self.write_header(
            &policy.base_policy,
            read_attr,
            0,
            field_count,
            bin_count as u16,
        );

        if !namespace.is_empty() {
            self.write_field_string(namespace, FieldType::Namespace);
        }

        if !set_name.is_empty() {
            self.write_field_string(set_name, FieldType::Table);
        }

        self.write_field_header(partitions.len() * 2, FieldType::PIDArray);
        for pid in partitions {
            self.write_u16_little_endian(*pid);
        }

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        // self.write_field_header(2, FieldType::ScanOptions);

        // let mut priority: u8 = policy.base_policy.priority.clone() as u8;
        // priority <<= 4;

        // if policy.fail_on_cluster_change {
        //     priority |= 0x08;
        // }

        // self.write_u8(priority);
        // self.write_u8(policy.scan_percent);

        // Write scan timeout
        self.write_field_header(4, FieldType::ScanTimeout);
        self.write_u32(policy.socket_timeout);

        self.write_field_header(8, FieldType::TranId);
        self.write_u64(task_id);

        if let Bins::Some(ref bin_names) = *bins {
            for bin_name in bin_names {
                self.write_operation_for_bin_name(bin_name, OperationType::Read);
            }
        }

        self.end();
        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    pub fn set_query(
        &mut self,
        policy: &QueryPolicy,
        statement: &Statement,
        write: bool,
        task_id: u64,
        partitions: &[u16],
    ) -> Result<()> {
        let filter = statement.filters.as_ref().map(|filters| &filters[0]);

        self.begin();

        let mut field_count = 0;
        let mut filter_size = 0;
        let mut bin_name_size = 0;

        if !statement.namespace.is_empty() {
            self.data_offset += statement.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !statement.set_name.is_empty() {
            self.data_offset += statement.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if let Some(ref index_name) = statement.index_name {
            if !index_name.is_empty() {
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

            filter_size = 1 + filter.estimate_size();
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
            // self.data_offset += 2 + FIELD_HEADER_SIZE as usize;
            // field_count += 1;
            // Estimate pid size
            self.data_offset += partitions.len() * 2 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }
        let filter_exp_size = self.estimate_filter_size(policy.filter_expression());
        if filter_exp_size > 0 {
            field_count += 1;
        }
        if let Some(ref aggregation) = statement.aggregation {
            self.data_offset += 1 + FIELD_HEADER_SIZE as usize; // udf type
            self.data_offset += aggregation.package_name.len() + FIELD_HEADER_SIZE as usize;
            self.data_offset += aggregation.function_name.len() + FIELD_HEADER_SIZE as usize;

            if let Some(ref args) = aggregation.function_args {
                self.estimate_args_size(Some(args));
            } else {
                self.estimate_args_size(None);
            }
            field_count += 4;
        }

        if statement.is_scan() {
            if let Bins::Some(ref bin_names) = statement.bins {
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name);
                }
            }
        }

        self.size_buffer()?;

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
        );

        if !statement.namespace.is_empty() {
            self.write_field_string(&statement.namespace, FieldType::Namespace);
        }

        if let Some(ref index_name) = statement.index_name {
            if !index_name.is_empty() {
                self.write_field_string(index_name, FieldType::IndexName);
            }
        }

        if !statement.set_name.is_empty() {
            self.write_field_string(&statement.set_name, FieldType::Table);
        }

        self.write_field_header(8, FieldType::TranId);
        self.write_u64(task_id);

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type();

            if idx_type != CollectionIndexType::Default {
                self.write_field_header(1, FieldType::IndexType);
                self.write_u8(idx_type as u8);
            }

            self.write_field_header(filter_size, FieldType::IndexRange);
            self.write_u8(1);

            filter.write(self);

            if let Bins::Some(ref bin_names) = statement.bins {
                if !bin_names.is_empty() {
                    self.write_field_header(bin_name_size, FieldType::QueryBinList);
                    self.write_u8(bin_names.len() as u8);

                    for bin_name in bin_names {
                        self.write_u8(bin_name.len() as u8);
                        self.write_str(bin_name);
                    }
                }
            }
        } else {
            // // Calling query with no filters is more efficiently handled by a primary index scan.
            // self.write_field_header(2, FieldType::ScanOptions);
            // let priority: u8 = (policy.base_policy.priority.clone() as u8) << 4;
            // self.write_u8(priority);
            // self.write_u8(100);
            self.write_field_header(partitions.len() * 2, FieldType::PIDArray);
            for pid in partitions {
                self.write_u16_little_endian(*pid);
            }
        }

        if let Some(filter_exp) = policy.filter_expression() {
            self.write_filter_expression(filter_exp, filter_exp_size);
        }

        if let Some(ref aggregation) = statement.aggregation {
            self.write_field_header(1, FieldType::UdfOp);
            if statement.bins.is_none() {
                self.write_u8(2);
            } else {
                self.write_u8(1);
            }

            self.write_field_string(&aggregation.package_name, FieldType::UdfPackageName);
            self.write_field_string(&aggregation.function_name, FieldType::UdfFunction);
            if let Some(ref args) = aggregation.function_args {
                self.write_args(Some(args), FieldType::UdfArgList);
            } else {
                self.write_args(None, FieldType::UdfArgList);
            }
        }

        // scan binNames come last
        if statement.is_scan() {
            if let Bins::Some(ref bin_names) = statement.bins {
                for bin_name in bin_names {
                    self.write_operation_for_bin_name(bin_name, OperationType::Read);
                }
            }
        }

        self.end();
        Ok(())
    }

    fn estimate_filter_size(&mut self, filter: &Option<FilterExpression>) -> usize {
        filter.clone().map_or(0, |filter| {
            let filter_size = filter.pack(&mut None);
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            filter_size
        })
    }

    fn estimate_key_size(&mut self, key: &Key, send_key: bool) -> u16 {
        let mut field_count: u16 = 0;

        if !key.namespace.is_empty() {
            self.data_offset += key.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !key.set_name.is_empty() {
            self.data_offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        self.data_offset += (DIGEST_SIZE + FIELD_HEADER_SIZE) as usize;
        field_count += 1;

        if send_key {
            if let Some(ref user_key) = key.user_key {
                // field header size + key size
                self.data_offset += user_key.estimate_size() + FIELD_HEADER_SIZE as usize + 1;
                field_count += 1;
            }
        }

        field_count
    }

    fn estimate_args_size(&mut self, args: Option<&[Value]>) {
        if let Some(args) = args {
            self.data_offset += encoder::pack_array(&mut None, args) + FIELD_HEADER_SIZE as usize;
        } else {
            self.data_offset +=
                encoder::pack_empty_args_array(&mut None) + FIELD_HEADER_SIZE as usize;
        }
    }

    fn estimate_udf_size(
        &mut self,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> usize {
        self.data_offset += package_name.len() + FIELD_HEADER_SIZE as usize;
        self.data_offset += function_name.len() + FIELD_HEADER_SIZE as usize;
        self.estimate_args_size(args);
        3
    }

    fn estimate_operation_size_for_bin(&mut self, bin: &Bin) {
        self.data_offset += bin.name.len() + OPERATION_HEADER_SIZE as usize;
        self.data_offset += bin.value.estimate_size();
    }

    fn estimate_operation_size_for_bin_name(&mut self, bin_name: &str) {
        self.data_offset += bin_name.len() + OPERATION_HEADER_SIZE as usize;
    }

    fn estimate_operation_size(&mut self) {
        self.data_offset += OPERATION_HEADER_SIZE as usize;
    }

    fn write_header(
        &mut self,
        policy: &ReadPolicy,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) {
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
        self.write_u16(field_count as u16);
        self.write_u16(operation_count as u16);

        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
    }

    // Header write for write operations.
    fn write_header_with_policy(
        &mut self,
        policy: &WritePolicy,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) {
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
            info_attr |= INFO3_COMMIT_MASTER;
        }

        if policy.base_policy.consistency_level == ConsistencyLevel::ConsistencyAll {
            read_attr |= INFO1_CONSISTENCY_ALL;
        }

        if policy.durable_delete {
            write_attr |= INFO2_DURABLE_DELETE;
        }

        // Write all header data except total size which must be written last.
        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE); // Message header length.
        self.write_u8(read_attr);
        self.write_u8(write_attr);
        self.write_u8(info_attr);
        self.write_u8(0); // unused
        self.write_u8(0); // clear the result code

        self.write_u32(generation);
        self.write_u32(policy.expiration.into());

        // Initialize timeout. It will be written later.
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);

        self.write_u16(field_count);
        self.write_u16(operation_count);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
    }

    fn write_key(&mut self, key: &Key, send_key: bool) {
        // Write key into buffer.
        if !key.namespace.is_empty() {
            self.write_field_string(&key.namespace, FieldType::Namespace);
        }

        if !key.set_name.is_empty() {
            self.write_field_string(&key.set_name, FieldType::Table);
        }

        self.write_field_bytes(&key.digest, FieldType::DigestRipe);

        if send_key {
            if let Some(ref user_key) = key.user_key {
                self.write_field_value(user_key, FieldType::Key);
            }
        }
    }

    fn write_filter_expression(&mut self, filter: &FilterExpression, size: usize) {
        self.write_field_header(size, FieldType::FilterExp);
        filter.pack(&mut Some(self));
    }

    fn write_field_header(&mut self, size: usize, ftype: FieldType) {
        self.write_i32(size as i32 + 1);
        self.write_u8(ftype as u8);
    }

    fn write_field_string(&mut self, field: &str, ftype: FieldType) {
        self.write_field_header(field.len(), ftype);
        self.write_str(field);
    }

    fn write_field_bytes(&mut self, bytes: &[u8], ftype: FieldType) {
        self.write_field_header(bytes.len(), ftype);
        self.write_bytes(bytes);
    }

    fn write_field_value(&mut self, value: &Value, ftype: FieldType) {
        self.write_field_header(value.estimate_size() + 1, ftype);
        self.write_u8(value.particle_type() as u8);
        value.write_to(self);
    }

    fn write_args(&mut self, args: Option<&[Value]>, ftype: FieldType) {
        if let Some(args) = args {
            self.write_field_header(encoder::pack_array(&mut None, args), ftype);
            encoder::pack_array(&mut Some(self), args);
        } else {
            self.write_field_header(encoder::pack_empty_args_array(&mut None), ftype);
            encoder::pack_empty_args_array(&mut Some(self));
        }
    }

    fn write_operation_for_bin(&mut self, bin: &Bin, op_type: OperationType) {
        let name_length = bin.name.len();
        let value_length = bin.value.estimate_size();

        self.write_i32((name_length + value_length + 4) as i32);
        self.write_u8(op_type as u8);
        self.write_u8(bin.value.particle_type() as u8);
        self.write_u8(0);
        self.write_u8(name_length as u8);
        self.write_str(bin.name);
        bin.value.write_to(self);
    }

    fn write_operation_for_bin_name(&mut self, name: &str, op_type: OperationType) {
        self.write_i32(name.len() as i32 + 4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(name.len() as u8);
        self.write_str(name);
    }

    fn write_operation_for_operation_type(&mut self, op_type: OperationType) {
        self.write_i32(4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
    }

    // Data buffer implementations

    pub const fn data_offset(&self) -> usize {
        self.data_offset
    }

    pub fn skip_bytes(&mut self, count: usize) {
        self.data_offset += count;
    }

    pub fn skip(&mut self, count: usize) {
        self.data_offset += count;
    }

    pub fn peek(&self) -> u8 {
        self.data_buffer[self.data_offset]
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u8(&mut self, pos: Option<usize>) -> u8 {
        if let Some(pos) = pos {
            self.data_buffer[pos]
        } else {
            let res = self.data_buffer[self.data_offset];
            self.data_offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_i8(&mut self, pos: Option<usize>) -> i8 {
        if let Some(pos) = pos {
            self.data_buffer[pos] as i8
        } else {
            let res = self.data_buffer[self.data_offset] as i8;
            self.data_offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u16(&mut self, pos: Option<usize>) -> u16 {
        let len = 2;
        if let Some(pos) = pos {
            NetworkEndian::read_u16(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_u16(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        }
    }

    pub fn read_i16(&mut self, pos: Option<usize>) -> i16 {
        let val = self.read_u16(pos);
        val as i16
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u32(&mut self, pos: Option<usize>) -> u32 {
        let len = 4;
        if let Some(pos) = pos {
            NetworkEndian::read_u32(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_u32(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        }
    }

    pub fn read_i32(&mut self, pos: Option<usize>) -> i32 {
        let val = self.read_u32(pos);
        val as i32
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_u64(&mut self, pos: Option<usize>) -> u64 {
        let len = 8;
        if let Some(pos) = pos {
            NetworkEndian::read_u64(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_u64(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        }
    }

    pub fn read_i64(&mut self, pos: Option<usize>) -> i64 {
        let val = self.read_u64(pos);
        val as i64
    }

    pub fn read_msg_size(&mut self, pos: Option<usize>) -> usize {
        let size = self.read_i64(pos);
        let size = size & 0xFFFF_FFFF_FFFF;
        size as usize
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_f32(&mut self, pos: Option<usize>) -> f32 {
        let len = 4;
        if let Some(pos) = pos {
            NetworkEndian::read_f32(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_f32(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub fn read_f64(&mut self, pos: Option<usize>) -> f64 {
        let len = 8;
        if let Some(pos) = pos {
            NetworkEndian::read_f64(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_f64(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        }
    }

    pub fn read_str(&mut self, len: usize) -> Result<String> {
        let s = str::from_utf8(&self.data_buffer[self.data_offset..self.data_offset + len])?;
        self.data_offset += len;
        Ok(s.to_owned())
    }

    pub fn read_bytes(&mut self, pos: usize, count: usize) -> &[u8] {
        &self.data_buffer[pos..pos + count]
    }

    pub fn read_slice(&mut self, count: usize) -> &[u8] {
        &self.data_buffer[self.data_offset..self.data_offset + count]
    }

    pub fn read_blob(&mut self, len: usize) -> Vec<u8> {
        let val = self.data_buffer[self.data_offset..self.data_offset + len].to_vec();
        self.data_offset += len;
        val
    }

    pub fn write_u8(&mut self, val: u8) -> usize {
        self.data_buffer[self.data_offset] = val;
        self.data_offset += 1;
        1
    }

    pub fn write_i8(&mut self, val: i8) -> usize {
        self.data_buffer[self.data_offset] = val as u8;
        self.data_offset += 1;
        1
    }

    pub fn write_u16(&mut self, val: u16) -> usize {
        NetworkEndian::write_u16(
            &mut self.data_buffer[self.data_offset..self.data_offset + 2],
            val,
        );
        self.data_offset += 2;
        2
    }

    pub fn write_u16_little_endian(&mut self, val: u16) -> usize {
        LittleEndian::write_u16(
            &mut self.data_buffer[self.data_offset..self.data_offset + 2],
            val,
        );
        self.data_offset += 2;
        2
    }

    pub fn write_i16(&mut self, val: i16) -> usize {
        self.write_u16(val as u16)
    }

    pub fn write_u32(&mut self, val: u32) -> usize {
        NetworkEndian::write_u32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        4
    }

    pub fn write_i32(&mut self, val: i32) -> usize {
        self.write_u32(val as u32)
    }

    pub fn write_u64(&mut self, val: u64) -> usize {
        NetworkEndian::write_u64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub fn write_i64(&mut self, val: i64) -> usize {
        self.write_u64(val as u64)
    }

    pub fn write_bool(&mut self, val: bool) -> usize {
        let val = if val { 1 } else { 0 };
        self.write_i64(val)
    }

    pub fn write_f32(&mut self, val: f32) -> usize {
        NetworkEndian::write_f32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        4
    }

    pub fn write_f64(&mut self, val: f64) -> usize {
        NetworkEndian::write_f64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> usize {
        for b in bytes {
            self.write_u8(*b);
        }
        bytes.len()
    }

    pub fn write_str(&mut self, val: &str) -> usize {
        self.write_bytes(val.as_bytes())
    }

    pub fn write_geo(&mut self, value: &str) -> usize {
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_bytes(value.as_bytes());
        3 + value.len()
    }

    pub fn write_timeout(&mut self, val: Option<Duration>) {
        if let Some(val) = val {
            let millis: i32 = (val.as_secs() * 1_000) as i32 + val.subsec_millis() as i32;
            NetworkEndian::write_i32(&mut self.data_buffer[22..22 + 4], millis);
        }
    }

    pub fn dump_buffer(&self) {
        println!(">>>>>>>>>>>>>>> {:?}", self.data_buffer.clone());
    }
}
