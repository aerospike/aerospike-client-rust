// Copyright 2013-2016 Aerospike, Inc.
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

use std::sync::Arc;
use std::io::Write;

use std::thread;
use std::time::{Instant, Duration};

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

use net::Connection;
use error::{AerospikeError, ResultCode, AerospikeResult};
use value::{Value};

use net::Host;
use cluster::node_validator::NodeValidator;
use cluster::partition_tokenizer::PartitionTokenizer;
use cluster::partition::Partition;
use cluster::{Node, Cluster};
use common::{Key, Record, OperationType, FieldType, ParticleType, Bin};
use policy::{ClientPolicy, WritePolicy, ReadPolicy, Policy, ConsistencyLevel, CommitLevel, GenerationPolicy, RecordExistsAction};
use common::operation::{Operation};
use common::operation;
use command::command::{Command};

// Flags commented out are not supported by cmd client.
// Contains a read operation.
const INFO1_READ: u8 = (1 << 0);

// Get all bins.
const INFO1_GET_ALL: u8 = (1 << 1);

// Do not read the bins
const INFO1_NOBINDATA: u8 = (1 << 5);

// Involve all replicas in read operation.
const INFO1_CONSISTENCY_ALL: u8 = (1 << 6);

// Create or update record
const INFO2_WRITE: u8 = (1 << 0);

// Fling a record into the belly of Moloch.
const INFO2_DELETE: u8 = (1 << 1);

// Update if expected generation == old.
const INFO2_GENERATION: u8 = (1 << 2);

// Update if new generation >= old, good for restore.
const INFO2_GENERATION_GT: u8 = (1 << 3);

// Create a duplicate on a generation collision.
const INFO2_GENERATION_DUP: u8 = (1 << 4);

// Create only. Fail if record already exists.
const INFO2_CREATE_ONLY: u8 = (1 << 5);

// Return a result for every operation.
const INFO2_RESPOND_ALL_OPS: u8 = (1 << 7);

// This is the last of a multi-part message.
const INFO3_LAST: u8 = (1 << 0);

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
const MSG_REMAINING_HEADER_SIZE: u8 = 22;
const DIGEST_SIZE: u8 = 20;
const CL_MSG_VERSION: u8 = 2;
const AS_MSG_TYPE: u8 = 3;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers. Tweak this number if you are returning a lot of
// LDT elements in your queries.
const MAX_BUFFER_SIZE: usize = 1024 * 1024 + 8; // 1 MB + header

// Holds data buffer for the command
#[derive(Debug)]
pub struct Buffer {
    // pub node: Arc<Node>,
    // conn: Option<Arc<Connection>>,

    pub data_buffer: Vec<u8>,
    pub data_offset: usize,
}

impl Buffer {

    pub fn new() -> Self {
        Buffer {
            // node: node,

            data_buffer: vec![],
            data_offset: 0,
        }
    }

    fn begin(&mut self) -> AerospikeResult<()> {
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
        Ok(())
    }

    fn size_buffer(&mut self) -> AerospikeResult<()> {
        let offset = self.data_offset;
        self.resize_buffer(offset)
    }

    pub fn resize_buffer(&mut self, size: usize) -> AerospikeResult<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(AerospikeError::new(ResultCode::PARSE_ERROR,
                                           Some(format!("Invalid size for buffer: {}",
                                                        size))));
        }

        self.data_buffer.resize(size, 0);

        Ok(())
    }

    pub fn reset_offset(&mut self) -> AerospikeResult<()> {
        // reset data offset
        self.data_offset = 0;
        Ok(())
    }

    fn end(&mut self) -> AerospikeResult<()> {
        let size = ((self.data_offset - 8) as i64) | (((CL_MSG_VERSION as i64) << 56) as i64) |
                   ((AS_MSG_TYPE as i64) << 48);

        // reset data offset
        try!(self.reset_offset());
        self.write_i64(size);

        Ok(())
    }

    // Writes the command for write operations
    pub fn set_write<'a>(&mut self, policy: &WritePolicy, operation: &OperationType, key: &Key<'a>, bins: &[&Bin<'a>]) -> AerospikeResult<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, policy.send_key));

        for bin in bins {
            try!(self.estimate_operation_size_for_bin(bin));
        }

        try!(self.size_buffer());
        try!(self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count as u16, bins.len() as u16));
        try!(self.write_key(key, policy.send_key));

        for bin in bins {
            try!(self.write_operation_for_bin(bin, operation));
        }

        self.end()
    }

    // Writes the command for write operations
    pub fn set_delete<'a>(&mut self, policy: &WritePolicy, key: &Key<'a>) -> AerospikeResult<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header_with_policy(policy, 0, INFO2_WRITE|INFO2_DELETE, field_count as u16, 0));
        try!(self.write_key(key, false));
        self.end()
    }

    // Writes the command for touch operations
    pub fn set_touch<'a>(&mut self, policy: &WritePolicy, key: &Key<'a>) -> AerospikeResult<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, policy.send_key));

        try!(self.estimate_operation_size());
        try!(self.size_buffer());
        try!(self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count as u16, 1));
        try!(self.write_key(key, policy.send_key));
        try!(self.write_operation_for_operation_type(&operation::TOUCH));
        self.end()
    }

    // Writes the command for exist operations
    pub fn set_exists<'a>(&mut self, policy: &WritePolicy, key: &Key<'a>) -> AerospikeResult<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header(&policy.base_policy, INFO1_READ|INFO1_NOBINDATA, 0, field_count, 0));
        try!(self.write_key(key, false));
        self.end()
    }

    pub fn set_read_for_key_only<'a>(&mut self,
                                    policy: &ReadPolicy,
                                    key: &Key<'a>)
                                    -> AerospikeResult<()> {
        try!(self.begin());

        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.size_buffer());
        try!(self.write_header(policy, INFO1_READ | INFO1_GET_ALL, 0, field_count, 0));
        try!(self.write_key(key, false));

        self.end()
    }

    // Writes the command for get operations (specified bins)
    pub fn set_read<'a>(&mut self,
                       policy: &ReadPolicy,
                       key: &Key<'a>,
                       bin_names: Option<&[&str]>)
                       -> AerospikeResult<()> {
        match bin_names {
            None => {
                try!(self.set_read_for_key_only(policy, key));
            }
            Some(bin_names) => {
                try!(self.begin());
                let field_count = try!(self.estimate_key_size(key, false));
                for bin_name in bin_names {
                    try!(self.estimate_operation_size_for_bin_name(bin_name));
                }

                try!(self.size_buffer());
                try!(self.write_header(policy, INFO1_READ, 0, field_count, bin_names.len() as u16));
                try!(self.write_key(key, false));
                for bin_name in bin_names {
                    try!(self.write_operation_for_bin_name(bin_name, &operation::READ));
                }
                try!(self.end());
            }
        }

        Ok(())
    }

    // Writes the command for getting metadata operations
    pub fn set_read_header<'a>(&mut self,
                       policy: &ReadPolicy,
                       key: &Key<'a>)
                       -> AerospikeResult<()> {
        try!(self.begin());
        let field_count = try!(self.estimate_key_size(key, false));
        try!(self.estimate_operation_size_for_bin_name(""));
        try!(self.size_buffer());
        try!(self.write_header(policy, INFO1_READ|INFO1_NOBINDATA, 0, field_count, 1));
        try!(self.write_key(key, false));
        try!(self.write_operation_for_bin_name("", &operation::READ));
        self.end()
    }

    // Writes the command for getting metadata operations
    pub fn set_operate<'a>(&mut self,
                       policy: &WritePolicy,
                       key: &Key<'a>,
                       operations: &'a [Operation<'a>])
                       -> AerospikeResult<()> {
        try!(self.begin());

        let mut read_attr = 0;
        let mut write_attr = 0;
        let mut read_bin = false;
        let mut read_header = false;
        let mut respond_per_each_op = policy.respond_per_each_op;

        for operation in operations {
            if operation.op == operation::MAP_READ || operation.op == operation::MAP_MODIFY {
                respond_per_each_op = true;
            }

            match operation.op {
                operation::READ /*| operation::CDT_READ*/ => {
                    if!operation.header_only {
                        read_attr |= INFO1_READ;

                        // Read all bins if no bin is specified.
                        if operation.bin_name == None {
                            read_attr |= INFO1_GET_ALL;
                        }
                        read_bin = true;
                    } else {
                        read_attr |= INFO1_READ;
                        read_header = true;
                    }
                },
                _ => write_attr |= INFO2_WRITE,
            }

            try!(self.estimate_operation_size_for_operation(operation));
        }

        let field_count = try!(self.estimate_key_size(key, policy.send_key && write_attr != 0));

        try!(self.size_buffer());

        if read_header && !read_bin {
            read_attr |= INFO1_NOBINDATA;
        }

        if respond_per_each_op {
            write_attr |= INFO2_RESPOND_ALL_OPS;
        }

        if write_attr != 0 {
            try!(self.write_header_with_policy(policy, read_attr, write_attr, field_count, operations.len() as u16));
        } else {
            try!(self.write_header(&policy.base_policy, read_attr, write_attr, field_count, operations.len() as u16));
        }
        try!(self.write_key(key, policy.send_key && write_attr != 0));

        for operation in operations {
            try!(self.write_operation_for_operation(operation));
        }

        self.end()
    }

    fn estimate_key_size<'a>(&mut self, key: &Key<'a>, send_key: bool) -> AerospikeResult<u16> {
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
                self.data_offset += user_key.estimate_size() + FIELD_HEADER_SIZE as usize + 1;
                field_count += 1;
            }
        }

        Ok(field_count)
    }

    fn estimate_operation_size_for_operation(&mut self, operation: &Operation) -> AerospikeResult<()> {
        self.data_offset += OPERATION_HEADER_SIZE as usize;
        if let Some(bin_name) = operation.bin_name {
            self.data_offset += bin_name.len();
        }

        if let Some(ref bv) = *operation.bin_value {
            self.data_offset += bv.estimate_size();
        }

        Ok(())
    }

    fn estimate_operation_size_for_bin(&mut self, bin: &Bin) -> AerospikeResult<()> {
        self.data_offset += bin.name.len() + OPERATION_HEADER_SIZE as usize;
        if let Some(ref value) = bin.value {
            self.data_offset += value.estimate_size();
        }
        Ok(())
    }

    fn estimate_operation_size_for_bin_name(&mut self, bin_name: &str) -> AerospikeResult<()> {
        self.data_offset += bin_name.len() + OPERATION_HEADER_SIZE as usize;
        Ok(())
    }

    fn estimate_operation_size(&mut self) -> AerospikeResult<()> {
        self.data_offset += OPERATION_HEADER_SIZE as usize;
        Ok(())
    }

    fn write_header(&mut self,
                    policy: &ReadPolicy,
                    read_attr: u8,
                    write_attr: u8,
                    field_count: u16,
                    operation_count: u16)
                    -> AerospikeResult<()> {
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

        Ok(())
    }

    // Header write for write operations.
    fn write_header_with_policy<'a>(&mut self, policy: &WritePolicy, read_attr: u8, write_attr: u8, field_count: u16, operation_count: u16)  -> AerospikeResult<()> {
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
                },
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

        // Write all header data except total size which must be written last.
        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE); // Message header length.
        self.write_u8(read_attr);
        self.write_u8(write_attr);
        self.write_u8(info_attr);
        self.write_u8(0); // unused
        self.write_u8(0); // clear the result code

        self.write_u32(generation);
        self.write_u32(policy.expiration.expiration());

        // Initialize timeout. It will be written later.
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);

        self.write_u16(field_count);
        self.write_u16(operation_count);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        Ok(())
    }

    fn write_key<'a>(&mut self, key: &Key<'a>, send_key: bool) -> AerospikeResult<()> {
        // Write key into buffer.
        if key.namespace != "" {
            try!(self.write_field_string(key.namespace, FieldType::Namespace));
        }

        if key.set_name != "" {
            try!(self.write_field_string(key.set_name, FieldType::Table));
        }

        try!(self.write_field_bytes(&key.digest, FieldType::DigestRipe));

        if send_key {
            if let Some(ref user_key) = key.user_key {
                try!(self.write_field_value(user_key, FieldType::Key));
            }
        }

        Ok(())
    }

    fn write_field_header(&mut self, size: usize, ftype: FieldType) -> AerospikeResult<()> {
        self.write_i32(size as i32 + 1);
        self.write_u8(ftype as u8);

        Ok(())
    }

    fn write_field_string(&mut self, field: &str, ftype: FieldType) -> AerospikeResult<()> {
        try!(self.write_field_header(field.len(), ftype));
        self.write_str(field);

        Ok(())
    }

    fn write_field_bytes(&mut self, bytes: &[u8], ftype: FieldType) -> AerospikeResult<()> {
        try!(self.write_field_header(bytes.len(), ftype));
        self.write_bytes(bytes);

        Ok(())
    }

    fn write_field_value(&mut self,
                                value: &Value,
                                ftype: FieldType)
                                -> AerospikeResult<()> {
        try!(self.write_field_header(value.estimate_size() + 1, ftype));
        self.write_u8(value.particle_type() as u8);
        try!(value.write_to(self));

        Ok(())
    }

    fn write_operation_for_operation(&mut self,
                                    operation: &Operation)
                                    -> AerospikeResult<()> {

        let bin_name = match operation.bin_name {
            Some(bn) => bn,
            _ => "",
        };

        let name_length = bin_name.len();
        let value_length = if let Some(ref bv) = *operation.bin_value { bv.estimate_size() } else { 0 };

        self.write_i32((name_length+value_length+4) as i32);
        self.write_u8(operation.op.op);
        if let Some(ref bv) = *operation.bin_value {
            self.write_u8(bv.particle_type() as u8);
        } else {
            self.write_u8(ParticleType::NULL as u8);
        }
        self.write_u8(0);
        self.write_u8(name_length as u8);
        self.write_str(bin_name);
        if let Some(ref bv) = *operation.bin_value {
            try!(bv.write_to(self));
        }

        Ok(())
    }

    fn write_operation_for_bin(&mut self,
                                    bin: &Bin,
                                    operation: &OperationType)
                                    -> AerospikeResult<()> {

        let name_length = bin.name.len() as usize;
        let value_length = if let Some(ref value) = bin.value { value.estimate_size() } else { 0 };

        self.write_i32((name_length+value_length+4) as i32);
        self.write_u8(operation.op);
        if let Some(ref value) = bin.value {
            self.write_u8(value.particle_type() as u8);
        } else {
            self.write_u8(ParticleType::NULL as u8);
        }
        self.write_u8(0);
        self.write_u8(name_length as u8);
        self.write_str(bin.name);

        if let Some(ref value) = bin.value {
            try!(value.write_to(self));
        }

        Ok(())
    }

    fn write_operation_for_bin_name(&mut self,
                                    name: &str,
                                    operation: &OperationType)
                                    -> AerospikeResult<()> {
        self.write_i32(name.len() as i32 + 4);
        self.write_u8(operation.op);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(name.len() as u8);
        self.write_str(name);

        Ok(())
    }

    fn write_operation_for_operation_type(&mut self,
                                    operation: &OperationType)
                                    -> AerospikeResult<()> {
        self.write_i32(4);
        self.write_u8(operation.op);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);

        Ok(())
    }


    // fn check_server_compatibility(&mut self, val: Box<Value>) -> AerospikeResult<()> {
    //     match val.particle_type() {
    //         ParticleType::FLOAT if !self.node.supports_float() => {
    //             Err(AerospikeError::new(ResultCode::TYPE_NOT_SUPPORTED,
    //                                     Some("This cluster node doesn't support double \
    //                                           precision floating-point values."
    //                                              .to_string())))
    //         }
    //         ParticleType::GEOJSON if !self.node.supports_geo() => {
    //             Err(AerospikeError::new(ResultCode::TYPE_NOT_SUPPORTED,
    //                                     Some("This cluster node doesn't support geo-spatial \
    //                                           features."
    //                                              .to_string())))
    //         }
    //         _ => Ok(()),
    //     }
    // }

    //
    // Data buffer implementations
    //

    #[inline(always)]
    pub fn read_u8(&mut self, pos: Option<usize>) -> AerospikeResult<u8> {
        match pos {
            Some(pos) => Ok(self.data_buffer[pos]),
            None => {
                let res = self.data_buffer[self.data_offset];
                self.data_offset += 1;
                Ok(res)
            },
        }
    }

    #[inline(always)]
    pub fn read_i8(&mut self, pos: Option<usize>) -> AerospikeResult<i8> {
        match pos {
            Some(pos) => Ok(self.data_buffer[pos] as i8),
            None => {
                let res = self.data_buffer[self.data_offset] as i8;
                self.data_offset += 1;
                Ok(res)
            },
        }
    }

    #[inline(always)]
    pub fn read_u16(&mut self, pos: Option<usize>) -> AerospikeResult<u16> {
        let len = 2;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u16(&mut self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u16(&mut self.data_buffer[self.data_offset..self.data_offset + len]);
                self.data_offset += len;
                Ok(res)
            },
        }
    }

    #[inline(always)]
    pub fn read_i16(&mut self, pos: Option<usize>) -> AerospikeResult<i16> {
        let val = try!(self.read_u16(pos));
        Ok(val as i16)
    }

    #[inline(always)]
    pub fn read_u32(&mut self, pos: Option<usize>) -> AerospikeResult<u32> {
        let len = 4;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u32(&mut self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u32(&mut self.data_buffer[self.data_offset..self.data_offset + len]);
                self.data_offset += len;
                Ok(res)
            },
        }
    }

    #[inline(always)]
    pub fn read_i32(&mut self, pos: Option<usize>) -> AerospikeResult<i32> {
        let val = try!(self.read_u32(pos));
        Ok(val as i32)
    }

    #[inline(always)]
    pub fn read_u64(&mut self, pos: Option<usize>) -> AerospikeResult<u64> {
        let len = 8;
        match pos {
            Some(pos) => Ok(NetworkEndian::read_u64(&mut self.data_buffer[pos..pos + len])),
            None => {
                let res = NetworkEndian::read_u64(&mut self.data_buffer[self.data_offset..self.data_offset + len]);
                self.data_offset += len;
                Ok(res)
            },
        }
    }

    #[inline(always)]
    pub fn read_i64(&mut self, pos: Option<usize>) -> AerospikeResult<i64> {
        let val = try!(self.read_u64(pos));
        Ok(val as i64)
    }

    #[inline(always)]
    pub fn read_bytes(&mut self, pos: usize, count: usize) -> AerospikeResult<&[u8]> {
        Ok(&self.data_buffer[pos..pos+count])
    }

    // #[inline(always)]
    // pub fn read_str(&mut self, pos: usize) -> AerospikeResult<&str> {
    //     let res = ;
    // }

    #[inline(always)]
    pub fn write_u8(&mut self, val: u8) {
        self.data_buffer[self.data_offset] = val;
        self.data_offset += 1;
    }

    #[inline(always)]
    pub fn write_i8(&mut self, val: i8) {
        self.data_buffer[self.data_offset] = val as u8;
        self.data_offset += 1;
    }

    #[inline(always)]
    pub fn write_u16(&mut self, val: u16) {
        NetworkEndian::write_u16(&mut self.data_buffer[self.data_offset..self.data_offset + 2],
                                 val);
        self.data_offset += 2;
    }

    #[inline(always)]
    pub fn write_i16(&mut self, val: i16) {
        self.write_u16(val as u16);
    }

    #[inline(always)]
    pub fn write_u32(&mut self, val: u32) {
        NetworkEndian::write_u32(&mut self.data_buffer[self.data_offset..self.data_offset + 4],
                                 val);
        self.data_offset += 4;
    }

    #[inline(always)]
    pub fn write_i32(&mut self, val: i32) {
        self.write_u32(val as u32);
    }

    #[inline(always)]
    pub fn write_u64(&mut self, val: u64) {
        NetworkEndian::write_u64(&mut self.data_buffer[self.data_offset..self.data_offset + 8],
                                 val);
        self.data_offset += 8;
    }

    #[inline(always)]
    pub fn write_i64(&mut self, val: i64) {
        self.write_u64(val as u64);
    }

    #[inline(always)]
    pub fn write_f64(&mut self, val: f64) {
        NetworkEndian::write_f64(&mut self.data_buffer[self.data_offset..self.data_offset + 8],
                                 val);
        self.data_offset += 8;
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.write_u8(*b);
        }
    }

    #[inline(always)]
    pub fn write_str(&mut self, val: &str) {
        self.write_bytes(val.as_bytes());
    }

    #[inline(always)]
    pub fn write_timeout(&mut self, val: Option<Duration>) {
        if let Some(val) = val {
            let millis: i32 = (val.as_secs() * 1_000) as i32 + (val.subsec_nanos() / 1_000_000) as i32;
            NetworkEndian::write_i32(&mut self.data_buffer[22..22 + 4], millis);
        }
    }
}


