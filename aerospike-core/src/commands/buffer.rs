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

use std::io::Write;
use std::str;

use byteorder::{ByteOrder, LittleEndian, NetworkEndian};
use flate2::write::ZlibEncoder;
use flate2::Compression;

use std::sync::Arc;

use crate::batch::BatchOperation;
use crate::commands::field_type::FieldType;
use crate::commands::BatchAttr;
use crate::errors::{Error, Result};
use crate::expressions::Expression;
use crate::msgpack::encoder;
use crate::operations::{Operation, OperationBin, OperationData, OperationType};
use crate::policy::{
    BasePolicy, BatchPolicy, CommitLevel, GenerationPolicy, Policy, QueryDuration, QueryPolicy,
    ReadModeAP, ReadModeSC, ReadPolicy, RecordExistsAction, WritePolicy,
};
use crate::query::NodePartitions;
use crate::txn::Txn;
use crate::{Bin, Bins, CollectionIndexType, Key, Statement, Value};

// Contains a read operation.
pub const INFO1_READ: u8 = 1;

// Get all bins.
pub const INFO1_GET_ALL: u8 = 1 << 1;

// Short query.
pub const INFO1_SHORT_QUERY: u8 = 1 << 2;

// Batch read or exists.
pub const INFO1_BATCH: u8 = 1 << 3;

// Do not read the bins
pub const INFO1_NOBINDATA: u8 = 1 << 5;

// Involve all replicas in read operation.
pub const INFO1_READ_MODE_AP_ALL: u8 = 1 << 6;

// Tell server to compress its response.
pub const INFO1_COMPRESS_RESPONSE: u8 = 1 << 7;

// Create or update record
pub const INFO2_WRITE: u8 = 1;

// Fling a record into the belly of Moloch.
pub const INFO2_DELETE: u8 = 1 << 1;

// Update if expected generation == old.
pub const INFO2_GENERATION: u8 = 1 << 2;

// Update if new generation >= old, good for restore.
pub const INFO2_GENERATION_GT: u8 = 1 << 3;

// Transaction resulting in record deletion leaves tombstone (Enterprise only).
pub const INFO2_DURABLE_DELETE: u8 = 1 << 4;

// Create only. Fail if record already exists.
pub const INFO2_CREATE_ONLY: u8 = 1 << 5;

// Create only. Fail if record already exists.
pub const INFO2_RELAX_AP_LONG_QUERY: u8 = 1 << 6;

// Return a result for every operation.
pub const INFO2_RESPOND_ALL_OPS: u8 = 1 << 7;

// This is the last of a multi-part message.
pub const INFO3_LAST: u8 = 1;

// Commit to master only before declaring success.
pub const INFO3_COMMIT_MASTER: u8 = 1 << 1;

// Partition is complete response in scan.
pub const INFO3_PARTITION_DONE: u8 = 1 << 2;

// Update only. Merge bins.
pub const INFO3_UPDATE_ONLY: u8 = 1 << 3;

// Create or completely replace record.
pub const INFO3_CREATE_OR_REPLACE: u8 = 1 << 4;

// Completely replace existing record only.
pub const INFO3_REPLACE_ONLY: u8 = 1 << 5;
// SC read type bit.
pub const INFO3_SC_READ_TYPE: u8 = 1 << 6;
// SC read relax bit.
pub const INFO3_SC_READ_RELAX: u8 = 1 << 7;

// pub(crate) const BATCH_MSG_READ: u8 = 0x0;
pub const BATCH_MSG_REPEAT: u8 = 0x1;
pub const BATCH_MSG_INFO: u8 = 0x2;
pub const BATCH_MSG_GEN: u8 = 0x4;
pub const BATCH_MSG_TTL: u8 = 0x8;
pub const BATCH_MSG_INFO4: u8 = 0x10;

// INFO4 flags for Multi-Record Transactions (MRT).
/// Verify read operation for MRT commit.
pub const INFO4_MRT_VERIFY_READ: u8 = 1;
/// Roll forward (commit) MRT.
pub const INFO4_MRT_ROLL_FORWARD: u8 = 1 << 1;
/// Roll back (abort) MRT.
pub const INFO4_MRT_ROLL_BACK: u8 = 1 << 2;
/// Apply operation on locking only (no data modification).
pub const INFO4_MRT_ON_LOCKING_ONLY: u8 = 1 << 4;

pub const MSG_TOTAL_HEADER_SIZE: u8 = 30;
pub const FIELD_HEADER_SIZE: u8 = 5;
pub const OPERATION_HEADER_SIZE: u8 = 8;
pub const MSG_REMAINING_HEADER_SIZE: u8 = 22;
const DIGEST_SIZE: u8 = 20;
const CL_MSG_VERSION: u8 = 2;
const AS_MSG_TYPE: u8 = 3;
pub const AS_MSG_TYPE_COMPRESSED: u8 = 4;

/// Default minimum message size to trigger compression — used when no
/// per-policy threshold has been set on a `Buffer`. Java's hardcoded
/// `COMPRESS_THRESHOLD` matches this value.
pub const DEFAULT_COMPRESS_THRESHOLD: usize = 128;

// MAX_BUFFER_SIZE protects against allocating massive memory blocks
// for buffers. Tweak this number if you are returning a lot of
// LDT elements in your queries.
pub const MAX_BUFFER_SIZE: usize = 120 * 1024 * 1024 + 8; // 120 MB + header

// Holds data buffer for the command
#[derive(Debug, Default)]
#[allow(clippy::struct_field_names)]
pub struct Buffer {
    pub data_buffer: Vec<u8>,
    pub data_offset: usize,
    // pub estimated_data_offset: usize,
    pub reclaim_threshold: usize,
    /// When compression is enabled, all command data is written starting at this
    /// offset (16), reserving space for the compressed proto header (8 bytes) and
    /// uncompressed size (8 bytes). This mirrors the Go client's pre-padding
    /// approach, allowing `compress()` to work in-place without copying.
    compress_offset: usize,
    /// Minimum command-buffer size before [`compress`](Self::compress)
    /// actually invokes zlib. Mirrors Java's hard-coded
    /// `COMPRESS_THRESHOLD = 128` (the default), but is configurable via
    /// `BasePolicy.compression_threshold`.
    compress_threshold: usize,
}

impl Buffer {
    pub(crate) fn new(reclaim_threshold: usize) -> Self {
        Buffer {
            data_buffer: Vec::with_capacity(1024),
            data_offset: 0,
            // estimated_data_offset: 0,
            reclaim_threshold,
            compress_offset: 0,
            compress_threshold: DEFAULT_COMPRESS_THRESHOLD,
        }
    }

    /// Enable or disable compression pre-padding and configure the size
    /// gate. Must be called before `begin()`. When enabled, reserves 16
    /// bytes at the start of the buffer for the compressed message header
    /// and remembers `threshold` so a later [`compress`](Self::compress)
    /// call can short-circuit on small payloads exactly like Java's
    /// `if (policy.compress && dataOffset > COMPRESS_THRESHOLD)`.
    pub(crate) const fn set_compress(&mut self, enabled: bool, threshold: usize) {
        self.compress_offset = if enabled { 16 } else { 0 };
        self.compress_threshold = threshold;
    }

    const fn begin(&mut self) {
        self.data_offset = self.compress_offset + MSG_TOTAL_HEADER_SIZE as usize;
    }

    pub(crate) fn size_buffer(&mut self) -> Result<()> {
        let offset = self.data_offset;
        // self.estimated_data_offset = offset;
        self.resize_buffer(offset)
    }

    pub(crate) fn resize_buffer(&mut self, size: usize) -> Result<()> {
        // Corrupted data streams can result in a huge length.
        // Do a sanity check here.
        if size > MAX_BUFFER_SIZE {
            return Err(Error::InvalidArgument(format!(
                "Invalid size for buffer: {size}"
            )));
        }

        let mem_size = self.data_buffer.capacity();
        self.data_buffer.resize(size, 0);
        if mem_size > self.reclaim_threshold && size < mem_size {
            self.data_buffer.shrink_to_fit();
        }

        Ok(())
    }

    pub(crate) const fn reset_offset(&mut self) {
        // reset data offset
        self.data_offset = 0;
    }

    pub(crate) fn end(&mut self) {
        let proto_offset = self.compress_offset;
        let size = ((self.data_offset - proto_offset - 8) as i64)
            | (i64::from(CL_MSG_VERSION) << 56)
            | (i64::from(AS_MSG_TYPE) << 48);

        // Write proto header at the start of the command data (after any compression pad).
        self.data_offset = proto_offset;
        self.write_u64(size as u64);
    }

    /// Compress the command buffer in-place using zlib, matching the Go client's approach.
    ///
    /// Because `begin()` wrote command data starting at `compress_offset` (16),
    /// the first 16 bytes of `data_buffer` are already reserved for the compressed
    /// header. We split the buffer at the command-data boundary and compress from
    /// one region into the other, avoiding any copy or separate allocation.
    ///
    /// Buffer layout (when `compress_offset` == 16):
    ///   `[0 ..16]`                 → space for compressed proto header + uncompressed size
    ///   `[16 .. data_offset]`      → uncompressed command data (source)
    ///
    /// After compression:
    ///   `[0 .. 8]`                 → compressed proto header
    ///   `[8 .. 16]`               → original uncompressed size
    ///   `[16 .. 16+compressed]`   → compressed data
    pub(crate) fn compress(&mut self) -> Result<()> {
        let cmd_start = self.compress_offset;
        // After end() is called, data_offset no longer reflects the command length.
        // Use the buffer length instead.
        let cmd_end = self.data_buffer.len();
        let uncompressed_size = cmd_end - cmd_start;

        if uncompressed_size <= self.compress_threshold {
            // Below threshold: shift data to position 0 and skip compression.
            if cmd_start > 0 {
                self.data_buffer.copy_within(cmd_start..cmd_end, 0);
                self.data_offset = uncompressed_size;
                self.data_buffer.truncate(self.data_offset);
            }
            return Ok(());
        }

        // Extend the buffer so there's room for the compressed output after the
        // source data. zlib worst-case expansion is ~0.1% + 13 bytes.
        let extra = uncompressed_size / 100 + 64;
        let output_start = cmd_end; // right after the source data
        let output_len = uncompressed_size + extra;
        self.data_buffer.resize(output_start + output_len, 0);

        // Split into non-overlapping source and output regions.
        let compressed_size;
        {
            let (src_region, output_region) = self.data_buffer.split_at_mut(output_start);
            let src = &src_region[cmd_start..cmd_end]; // the uncompressed command data

            let cursor = std::io::Cursor::new(&mut output_region[..output_len]);
            let mut encoder = ZlibEncoder::new(cursor, Compression::default());

            // Write in 64KB chunks to work around zlib issues (matching Go client behavior).
            let mut pos = 0;
            const STEP: usize = 64 * 1024;
            while pos + STEP < uncompressed_size {
                encoder
                    .write_all(&src[pos..pos + STEP])
                    .map_err(|e| Error::ClientError(format!("Compression error: {e}")))?;
                pos += STEP;
            }
            if pos < uncompressed_size {
                encoder
                    .write_all(&src[pos..uncompressed_size])
                    .map_err(|e| Error::ClientError(format!("Compression error: {e}")))?;
            }

            let cursor = encoder
                .finish()
                .map_err(|e| Error::ClientError(format!("Compression error: {e}")))?;
            compressed_size = cursor.position() as usize;
        }

        // Copy compressed data from the tail into the pre-reserved space at [16..].
        self.data_buffer
            .copy_within(output_start..output_start + compressed_size, 16);

        // Write compressed proto header at [0..8].
        let proto = ((compressed_size + 8) as u64)
            | (u64::from(CL_MSG_VERSION) << 56)
            | (u64::from(AS_MSG_TYPE_COMPRESSED) << 48);
        NetworkEndian::write_u64(&mut self.data_buffer[0..8], proto);

        // Write original uncompressed size at [8..16].
        NetworkEndian::write_u64(&mut self.data_buffer[8..16], uncompressed_size as u64);

        // Truncate to actual compressed message size.
        self.data_offset = 16 + compressed_size;
        self.data_buffer.truncate(self.data_offset);

        Ok(())
    }

    // Writes the command for write operations
    pub(crate) fn set_write(
        &mut self,
        policy: &WritePolicy,
        op_type: OperationType,
        key: &Key,
        bins: &[Bin],
    ) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, policy.send_key)?;
        let (txn_field_count, version) = self.size_txn(key, policy.base_policy.txn.as_ref(), true);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }

        for bin in bins {
            self.estimate_operation_size_for_bin(bin.as_ref())?;
        }

        self.size_buffer()?;
        self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count, bins.len() as u16);
        self.write_key(key, policy.send_key)?;
        self.write_txn(policy.base_policy.txn.as_ref(), version, true);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }
        for bin in bins {
            self.write_operation_for_bin(bin.as_ref(), op_type)?;
        }

        self.end();
        Ok(())
    }

    // Writes the command for delete operations
    pub(crate) fn set_delete(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false)?;
        let (txn_field_count, version) = self.size_txn(key, policy.base_policy.txn.as_ref(), true);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }

        self.size_buffer()?;
        self.write_header_with_policy(policy, 0, INFO2_WRITE | INFO2_DELETE, field_count, 0);
        self.write_key(key, false)?;
        self.write_txn(policy.base_policy.txn.as_ref(), version, true);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    // Writes the command for touch operations
    pub(crate) fn set_touch(&mut self, policy: &WritePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, policy.send_key)?;
        let (txn_field_count, version) = self.size_txn(key, policy.base_policy.txn.as_ref(), true);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }
        self.estimate_operation_size();
        self.size_buffer()?;
        self.write_header_with_policy(policy, 0, INFO2_WRITE, field_count, 1);
        self.write_key(key, policy.send_key)?;
        self.write_txn(policy.base_policy.txn.as_ref(), version, true);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_operation_for_operation_type(OperationType::Touch);
        self.end();
        Ok(())
    }

    // Writes the command for exist operations
    pub(crate) fn set_exists(&mut self, policy: &ReadPolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false)?;
        let (txn_field_count, version) = self.size_txn(key, policy.base_policy.txn.as_ref(), false);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.base_policy.filter_expression())?;
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
        self.write_key(key, false)?;
        self.write_txn(policy.base_policy.txn.as_ref(), version, false);

        if let Some(filter) = policy.base_policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    // Writes the command for get operations
    pub(crate) fn set_read(&mut self, policy: &BasePolicy, key: &Key, bins: &Bins) -> Result<()> {
        match bins {
            Bins::None => self.set_read_header(policy, key),
            Bins::All => self.set_read_for_key_only(policy, key),
            Bins::Some(ref bin_names) => {
                self.begin();
                let mut field_count = self.estimate_key_size(key, false)?;
                let (txn_field_count, version) = self.size_txn(key, policy.txn.as_ref(), false);
                field_count += txn_field_count;
                let filter_size = self.estimate_filter_size(policy.filter_expression())?;
                if filter_size > 0 {
                    field_count += 1;
                }
                for bin_name in bin_names {
                    self.estimate_operation_size_for_bin_name(bin_name)?;
                }

                self.size_buffer()?;
                self.write_header(policy, INFO1_READ, 0, field_count, bin_names.len() as u16);
                self.write_key(key, false)?;
                self.write_txn(policy.txn.as_ref(), version, false);

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
    pub(crate) fn set_read_header(&mut self, policy: &BasePolicy, key: &Key) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_key_size(key, false)?;
        let (txn_field_count, version) = self.size_txn(key, policy.txn.as_ref(), false);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }

        self.estimate_operation_size_for_bin_name("")?;
        self.size_buffer()?;
        self.write_header(policy, INFO1_READ | INFO1_NOBINDATA, 0, field_count, 1);
        self.write_key(key, false)?;
        self.write_txn(policy.txn.as_ref(), version, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_operation_for_bin_name("", OperationType::Read);
        self.end();
        Ok(())
    }

    pub(crate) fn set_read_for_key_only(&mut self, policy: &BasePolicy, key: &Key) -> Result<()> {
        self.begin();

        let mut field_count = self.estimate_key_size(key, false)?;
        let (txn_field_count, version) = self.size_txn(key, policy.txn.as_ref(), false);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }

        self.size_buffer()?;
        self.write_header(policy, INFO1_READ | INFO1_GET_ALL, 0, field_count, 0);
        self.write_key(key, false)?;
        self.write_txn(policy.txn.as_ref(), version, false);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.end();
        Ok(())
    }

    #[allow(clippy::ref_option)]
    fn write_batch_fields_with_filter(
        &mut self,
        key: &Key,
        filter: &Option<Expression>,
        field_count: usize,
        op_count: usize,
    ) -> Result<()> {
        if let Some(filter) = filter {
            let field_count = field_count + 1;
            self.write_batch_fields(key, field_count, op_count);
            let exp_size = filter.size()?;
            self.write_filter_expression(filter, exp_size);
        } else {
            self.write_batch_fields(key, field_count, op_count);
        }
        Ok(())
    }

    #[allow(clippy::ref_option)]
    fn write_batch_fields_reg(
        &mut self,
        key: &Key,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        mut field_count: usize,
        op_count: usize,
    ) -> Result<()> {
        if filter.is_some() {
            field_count += 1;
        }

        if attr.send_key && key.has_value_to_send() {
            field_count += 1;
        }

        self.write_batch_fields(key, field_count, op_count);

        if let Some(filter) = filter {
            let exp_size = filter.size()?;
            self.write_filter_expression(filter, exp_size);
        }

        if attr.send_key && key.has_value_to_send() {
            self.write_field_value(&key.user_key.clone().unwrap(), FieldType::Key)?;
        }
        Ok(())
    }

    fn write_batch_fields(&mut self, key: &Key, field_count: usize, op_count: usize) {
        let field_count = field_count + 2;
        self.write_u16(field_count as u16);
        self.write_u16(op_count as u16);
        self.write_field_string(&key.namespace, FieldType::Namespace);
        self.write_field_string(&key.set_name, FieldType::Table);
    }

    #[allow(clippy::ref_option)]
    fn write_batch_bin_names(
        &mut self,
        key: &Key,
        bin_names: &Vec<String>,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
    ) -> Result<()> {
        self.write_batch_read(key, attr, filter, bin_names.len(), txn, ver)?;

        for bin in bin_names {
            self.write_operation_for_bin_name(bin, OperationType::Read);
        }
        Ok(())
    }

    #[allow(clippy::ref_option)]
    fn write_batch_operations(
        &mut self,
        key: &Key,
        ops: &Vec<Operation>,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
    ) -> Result<()> {
        if attr.has_write {
            self.write_batch_write(key, attr, filter, 0, ops.len(), txn, ver)?;
        } else {
            self.write_batch_read(key, attr, filter, ops.len(), txn, ver)?;
        }

        for op in ops {
            self.write_operation_for_operation(op)?;
        }
        Ok(())
    }

    #[allow(clippy::ref_option)]
    fn write_batch_read(
        &mut self,
        key: &Key,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        op_count: usize,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
    ) -> Result<()> {
        if txn.is_some() {
            self.write_u8(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_TTL);
            self.write_u8(attr.read_attr);
            self.write_u8(attr.write_attr);
            self.write_u8(attr.info_attr);
            self.write_u8(attr.txn_attr);
            self.write_u32(attr.expiration);
            self.write_batch_fields_txn(key, txn, ver, attr, filter, 0, op_count)
        } else {
            self.write_u8(BATCH_MSG_INFO | BATCH_MSG_TTL);
            self.write_u8(attr.read_attr);
            self.write_u8(attr.write_attr);
            self.write_u8(attr.info_attr);
            self.write_u32(attr.expiration);
            self.write_batch_fields_with_filter(key, filter, 0, op_count)
        }
    }

    #[allow(clippy::ref_option)]
    fn write_batch_write(
        &mut self,
        key: &Key,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        field_count: usize,
        op_count: usize,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
    ) -> Result<()> {
        if txn.is_some() {
            self.write_u8(BATCH_MSG_INFO | BATCH_MSG_INFO4 | BATCH_MSG_GEN | BATCH_MSG_TTL);
            self.write_u8(attr.read_attr);
            self.write_u8(attr.write_attr);
            self.write_u8(attr.info_attr);
            self.write_u8(attr.txn_attr);
            self.write_u16(attr.generation as u16);
            self.write_u32(attr.expiration);
            self.write_batch_fields_txn(key, txn, ver, attr, filter, field_count, op_count)
        } else {
            self.write_u8(BATCH_MSG_INFO | BATCH_MSG_GEN | BATCH_MSG_TTL);
            self.write_u8(attr.read_attr);
            self.write_u8(attr.write_attr);
            self.write_u8(attr.info_attr);
            self.write_u16(attr.generation as u16);
            self.write_u32(attr.expiration);
            self.write_batch_fields_reg(key, attr, filter, field_count, op_count)
        }
    }

    /// Write batch fields including transaction fields (`MRT_ID`, `RECORD_VERSION`, `MRT_DEADLINE`).
    fn write_batch_fields_txn(
        &mut self,
        key: &Key,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
        attr: &BatchAttr,
        filter: &Option<Expression>,
        mut field_count: usize,
        op_count: usize,
    ) -> Result<()> {
        // Account for txn fields
        field_count += 1; // MRT_ID always present

        if ver.is_some() {
            field_count += 1; // RECORD_VERSION
        }

        if let Some(txn) = txn {
            if attr.has_write && txn.monitor_exists() {
                field_count += 1; // MRT_DEADLINE
            }
        }

        if filter.is_some() {
            field_count += 1;
        }

        if attr.send_key && key.has_value_to_send() {
            field_count += 1;
        }

        self.write_batch_fields(key, field_count, op_count);

        // Write txn fields
        if let Some(txn) = txn {
            self.write_field_le64(txn.id(), FieldType::MrtId);

            if let Some(ver) = ver {
                self.write_field_version(ver);
            }

            if attr.has_write && txn.monitor_exists() {
                self.write_field_le32(txn.get_deadline(), FieldType::MrtDeadline);
            }
        }

        // Write filter expression
        if let Some(filter) = filter {
            let exp_size = filter.size()?;
            self.write_filter_expression(filter, exp_size);
        }

        // Write user key
        if attr.send_key && key.has_value_to_send() {
            self.write_field_value(&key.user_key.clone().unwrap(), FieldType::Key)?;
        }
        Ok(())
    }

    pub(crate) const fn get_batch_flags(policy: &BatchPolicy) -> u8 {
        let mut flags: u8 = if policy.allow_inline { 1 } else { 0 };

        if policy.allow_inline_ssd {
            flags |= 0x2;
        }

        if policy.respond_all_keys {
            flags |= 0x4;
        }

        flags
    }

    // Writes the command for batch read operations
    pub(crate) fn set_batch_operate(
        &mut self,
        policy: &BatchPolicy,
        batch_ops: &[(BatchOperation, usize)],
    ) -> Result<()> {
        self.begin();
        let mut field_count = 1;
        self.data_offset += FIELD_HEADER_SIZE as usize + 5;

        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }

        let txn = policy.base_policy.txn.as_ref();

        // Build versions array from txn reads
        let versions: Vec<Option<u64>> = if let Some(txn) = txn {
            batch_ops
                .iter()
                .map(|(batch_op, _)| txn.get_read_version(&batch_op.key()))
                .collect()
        } else {
            Vec::new()
        };

        let mut prev: Option<&BatchOperation> = None;
        let mut ver_prev: Option<u64> = None;
        for (i, (batch_op, _)) in batch_ops.iter().enumerate() {
            let ver = versions.get(i).copied().flatten();
            self.data_offset += batch_op.key().digest.len() + 4;
            if batch_op.match_header(prev, ver, ver_prev) {
                self.data_offset += 1;
            } else {
                // Must write full header and namespace/set/bin names.
                let key = &batch_op.key();
                self.data_offset += 12; // header(4) + ttl(4) + field_count(2) + op_count(2) = 12
                self.data_offset += key.namespace.len() + FIELD_HEADER_SIZE as usize;
                self.data_offset += key.set_name.len() + FIELD_HEADER_SIZE as usize;
                self.data_offset += batch_op.size(&policy.filter_expression)?; // + HEADER

                // Add txn field sizes
                self.size_txn_batch(txn, ver, batch_op.has_write());
            }
            prev = Some(batch_op);
            ver_prev = ver;
        }

        self.size_buffer()?;
        self.write_header(&policy.base_policy, INFO1_BATCH, 0, field_count, 0);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        let field_size_offset = self.data_offset;
        let field_type = FieldType::BatchIndex;
        self.write_field_header(0, field_type);
        self.write_u32(batch_ops.len() as u32);
        self.write_u8(Buffer::get_batch_flags(policy));

        let mut attr = BatchAttr::default();
        prev = None;
        ver_prev = None;
        for (idx, (batch_op, _)) in batch_ops.iter().enumerate() {
            let key = &batch_op.key();
            let ver = versions.get(idx).copied().flatten();
            self.write_u32(idx as u32);
            self.write_bytes(&key.digest);
            if batch_op.match_header(prev, ver, ver_prev) {
                self.write_u8(BATCH_MSG_REPEAT);
            } else {
                match batch_op {
                    BatchOperation::Read {
                        br: _,
                        policy: brpolicy,
                        bins,
                        ops,
                    } => {
                        attr.set_batch_read(brpolicy, policy);
                        match (bins, ops) {
                            (Bins::Some(bin_names), Some(ops))
                                if !bin_names.is_empty() && !ops.is_empty() =>
                            {
                                return Err(Error::ClientError(
                                    "Can't pass both bin names and operations to BatchReads".into(),
                                ))
                            }
                            (Bins::Some(bin_names), _) if !bin_names.is_empty() => {
                                self.write_batch_bin_names(
                                    key,
                                    bin_names,
                                    &attr,
                                    &attr.filter_expression,
                                    txn,
                                    ver,
                                )?;
                            }
                            (_, Some(ops)) if !ops.is_empty() => {
                                attr.adjust_read(ops);
                                self.write_batch_operations(
                                    key,
                                    ops,
                                    &attr,
                                    &attr.filter_expression,
                                    txn,
                                    ver,
                                )?;
                            }
                            _ => {
                                attr.adjust_read_for_all_bins(matches!(bins, Bins::All));
                                self.write_batch_read(
                                    key,
                                    &attr,
                                    &attr.filter_expression,
                                    0,
                                    txn,
                                    ver,
                                )?;
                            }
                        }
                    }
                    BatchOperation::Write {
                        br: _,
                        policy: bwpolicy,
                        ops,
                    } => {
                        attr.set_batch_write(bwpolicy, policy);
                        attr.adjust_write(ops);
                        self.write_batch_operations(
                            key,
                            ops,
                            &attr,
                            &attr.filter_expression,
                            txn,
                            ver,
                        )?;
                    }
                    BatchOperation::Delete {
                        br: _,
                        policy: bdpolicy,
                    } => {
                        attr.set_batch_delete(bdpolicy, policy);
                        self.write_batch_write(
                            key,
                            &attr,
                            &attr.filter_expression,
                            0,
                            0,
                            txn,
                            ver,
                        )?;
                    }
                    BatchOperation::UDF {
                        br: _,
                        policy: bupolicy,
                        udf_name,
                        function_name,
                        args,
                    } => {
                        attr.set_batch_udf(bupolicy, policy);
                        self.write_batch_write(
                            key,
                            &attr,
                            &attr.filter_expression,
                            3,
                            0,
                            txn,
                            ver,
                        )?;
                        self.write_field_string(udf_name, FieldType::UdfPackageName);
                        self.write_field_string(function_name, FieldType::UdfFunction);
                        self.write_args(args.as_deref(), FieldType::UdfArgList)?;
                    }
                }
            }
            prev = Some(batch_op);
            ver_prev = ver;
        }

        let field_size =
            self.data_offset - self.compress_offset - MSG_TOTAL_HEADER_SIZE as usize - 4;
        NetworkEndian::write_u32(
            &mut self.data_buffer[field_size_offset..field_size_offset + 4],
            field_size as u32,
        );

        self.end();
        Ok(())
    }

    // Writes the command for getting metadata operations
    pub(crate) fn set_operate(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &[Operation],
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

            self.data_offset += operation.estimate_size()? + OPERATION_HEADER_SIZE as usize;
        }

        let has_write = write_attr != 0;
        let mut field_count = self.estimate_key_size(key, policy.send_key && has_write)?;
        let (txn_field_count, version) =
            self.size_txn(key, policy.base_policy.txn.as_ref(), has_write);
        field_count += txn_field_count;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }
        self.size_buffer()?;

        if has_write {
            self.write_header_with_policy(
                policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        } else {
            self.write_header(
                &policy.base_policy,
                read_attr,
                write_attr,
                field_count,
                operations.len() as u16,
            );
        }
        self.write_key(key, policy.send_key && has_write)?;
        self.write_txn(policy.base_policy.txn.as_ref(), version, has_write);

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        for operation in operations {
            operation.write_to(self)?;
        }
        self.end();
        Ok(())
    }

    pub(crate) fn set_udf(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        package_name: &str,
        function_name: &str,
        args: Option<&[Value]>,
    ) -> Result<()> {
        self.begin();

        let mut field_count = self.estimate_key_size(key, policy.send_key)?;
        field_count += self.estimate_udf_size(package_name, function_name, args)? as u16;
        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_size > 0 {
            field_count += 1;
        }
        self.size_buffer()?;

        self.write_header(&policy.base_policy, 0, INFO2_WRITE, field_count, 0);
        self.write_key(key, policy.send_key)?;

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        self.write_field_string(package_name, FieldType::UdfPackageName);
        self.write_field_string(function_name, FieldType::UdfFunction);
        self.write_args(args, FieldType::UdfArgList)?;
        self.end();
        Ok(())
    }

    pub(crate) async fn set_scan(
        &mut self,
        policy: &QueryPolicy,
        namespace: &str,
        set_name: &str,
        bins: &Bins,
        task_id: u64,
        node_partitions: &NodePartitions,
    ) -> Result<()> {
        self.begin();

        let mut field_count = 0;

        let parts_full_size = node_partitions.parts_full.len() * 2;
        let parts_partial_size = node_partitions.parts_partial.len() * 20;
        let max_records = node_partitions.record_max;

        let filter_size = self.estimate_filter_size(policy.filter_expression())?;
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

        if parts_full_size > 0 {
            self.data_offset += parts_full_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if parts_partial_size > 0 {
            self.data_offset += parts_partial_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if max_records > 0 {
            self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if policy.records_per_second > 0 {
            self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

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
                    self.estimate_operation_size_for_bin_name(bin_name)?;
                }
                bin_names.len()
            }
        };

        self.size_buffer()?;

        let mut read_attr = INFO1_READ;
        if bins.is_none() {
            read_attr |= INFO1_NOBINDATA;
        }

        let mut write_attr = 0;
        match policy.expected_duration {
            QueryDuration::Short => read_attr |= INFO1_SHORT_QUERY,
            QueryDuration::LongRelaxAP => write_attr |= INFO2_RELAX_AP_LONG_QUERY,
            QueryDuration::Long => (),
        }

        self.write_header_read(
            &policy.base_policy,
            read_attr,
            write_attr,
            INFO3_PARTITION_DONE,
            field_count,
            bin_count as u16,
        );

        if !namespace.is_empty() {
            self.write_field_string(namespace, FieldType::Namespace);
        }

        if !set_name.is_empty() {
            self.write_field_string(set_name, FieldType::Table);
        }

        if parts_full_size > 0 {
            self.write_field_header(parts_full_size, FieldType::PIDArray);
            for part in &node_partitions.parts_full {
                let part = part.lock().await;
                self.write_u16_little_endian(part.id);
            }
        }

        if parts_partial_size > 0 {
            self.write_field_header(parts_partial_size, FieldType::DigestArray);
            for part in &node_partitions.parts_partial {
                let part = part.lock().await;
                part.digest.map(|digest| self.write_bytes(&digest));
            }
        }

        if let Some(filter) = policy.filter_expression() {
            self.write_filter_expression(filter, filter_size);
        }

        if max_records > 0 {
            self.write_field_u64(max_records, FieldType::MaxRecords);
        }

        if policy.records_per_second > 0 {
            self.write_field_u32(policy.records_per_second, FieldType::RecordsPerSecond);
        }

        // Write scan timeout
        self.write_field_header(4, FieldType::SocketTimeout);
        self.write_u32(policy.socket_timeout());

        self.write_field_header(8, FieldType::QueryId);
        self.write_u64(task_id);

        if let Bins::Some(ref bin_names) = *bins {
            for bin_name in bin_names {
                self.write_operation_for_bin_name(bin_name, OperationType::Read);
            }
        }

        self.end();
        // self.dump_buffer();

        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
    pub(crate) async fn set_query(
        &mut self,
        policy: &QueryPolicy,
        statement: &Statement,
        write: bool,
        task_id: u64,
        node_partitions: &NodePartitions,
    ) -> Result<()> {
        let filter = statement.filters.as_ref().map(|filters| &filters[0]);

        self.begin();

        let mut field_count = 0;
        let mut filter_size = 0;

        if !statement.namespace.is_empty() {
            self.data_offset += statement.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !statement.set_name.is_empty() {
            self.data_offset += statement.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if policy.records_per_second > 0 {
            self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // Estimate socket timeout field size. This field is used in new servers and not used
        // (but harmless to add) in old servers.
        self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // Allocate space for TaskId field.
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();
            if idx_type != CollectionIndexType::Default {
                self.data_offset += 1 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            filter_size = 1 + filter.estimate_size()?;
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.data_offset += ctx_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.data_offset += index_name.len() + FIELD_HEADER_SIZE as usize;
                    field_count += 1;
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.data_offset += exp_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }
        }

        let parts_full_size = node_partitions.parts_full.len() * 2;
        let parts_partial_size = node_partitions.parts_partial.len() * 20;
        let parts_partial_bval_size = match filter {
            Some(_) => node_partitions.parts_partial.len() * 8,
            None => 0,
        };
        let max_records = node_partitions.record_max;

        if parts_full_size > 0 {
            self.data_offset += parts_full_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if parts_partial_size > 0 {
            self.data_offset += parts_partial_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if parts_partial_bval_size > 0 {
            self.data_offset += parts_partial_bval_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if max_records > 0 {
            self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        let filter_exp_size = self.estimate_filter_size(policy.filter_expression())?;
        if filter_exp_size > 0 {
            field_count += 1;
        }

        if let Some(ref aggregation) = statement.aggregation {
            self.data_offset += 1 + FIELD_HEADER_SIZE as usize; // udf type
            self.data_offset += aggregation.package_name.len() + FIELD_HEADER_SIZE as usize;
            self.data_offset += aggregation.function_name.len() + FIELD_HEADER_SIZE as usize;

            if let Some(ref args) = aggregation.function_args {
                self.estimate_args_size(Some(args))?;
            } else {
                self.estimate_args_size(None)?;
            }
            field_count += 4;
        }

        let operation_count = if let Bins::Some(ref bin_names) = statement.bins {
            for bin_name in bin_names {
                self.estimate_operation_size_for_bin_name(bin_name)?;
            }
            bin_names.len()
        } else {
            0
        };

        self.size_buffer()?;

        let mut info1 = INFO1_READ;
        if !policy.include_bin_data || statement.bins.is_none() {
            info1 |= INFO1_NOBINDATA;
        }

        let mut info2 = if write { INFO2_WRITE } else { 0 };

        match policy.expected_duration {
            QueryDuration::Short => info1 |= INFO1_SHORT_QUERY,
            QueryDuration::LongRelaxAP => info2 |= INFO2_RELAX_AP_LONG_QUERY,
            QueryDuration::Long => (),
        }

        self.write_header_read(
            &policy.base_policy,
            info1,
            info2,
            INFO3_PARTITION_DONE,
            field_count,
            operation_count as u16,
        );

        if !statement.namespace.is_empty() {
            self.write_field_string(&statement.namespace, FieldType::Namespace);
        }

        if !statement.set_name.is_empty() {
            self.write_field_string(&statement.set_name, FieldType::Table);
        }

        self.write_field_header(8, FieldType::QueryId);
        self.write_u64(task_id);

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();

            if idx_type != CollectionIndexType::Default {
                self.write_field_header(1, FieldType::IndexType);
                self.write_u8(idx_type as u8);
            }

            self.write_field_header(filter_size, FieldType::IndexRange);
            self.write_u8(1);

            filter.write(self)?;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.write_field_header(ctx_size, FieldType::IndexContext);
                encoder::pack_ctx_for_index(&mut Some(self), ctx)?;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.write_field_string(index_name, FieldType::IndexName);
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.write_field_header(exp_size, FieldType::IndexExpression);
                exp.pack(&mut Some(self))?;
            }
        }

        if parts_full_size > 0 {
            self.write_field_header(parts_full_size, FieldType::PIDArray);
            for part in &node_partitions.parts_full {
                let part = part.lock().await;
                self.write_u16_little_endian(part.id);
            }
        }

        if parts_partial_size > 0 {
            self.write_field_header(parts_partial_size, FieldType::DigestArray);
            for part in &node_partitions.parts_partial {
                let part = part.lock().await;
                part.digest.map(|digest| self.write_bytes(&digest));
            }
        }

        if parts_partial_bval_size > 0 {
            self.write_field_header(parts_partial_bval_size, FieldType::BValArray);
            for part in &node_partitions.parts_partial {
                let part = part.lock().await;
                if let Some(bval) = part.bval {
                    self.write_u64_little_endian(bval);
                } else {
                    self.write_u64_little_endian(0);
                }
            }
        }

        if max_records > 0 {
            self.write_field_u64(max_records, FieldType::MaxRecords);
        }

        if policy.records_per_second > 0 {
            self.write_field_u32(policy.records_per_second, FieldType::RecordsPerSecond);
        }

        // Write scan timeout
        self.write_field_header(4, FieldType::SocketTimeout);
        self.write_u32(policy.socket_timeout());

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
                self.write_args(Some(args), FieldType::UdfArgList)?;
            } else {
                self.write_args(None, FieldType::UdfArgList)?;
            }
        }

        // Bin names are sent as READ operations (new server / partition query path).
        if let Bins::Some(ref bin_names) = statement.bins {
            for bin_name in bin_names {
                self.write_operation_for_bin_name(bin_name, OperationType::Read);
            }
        }

        self.end();
        Ok(())
    }

    /// Builds a background query/scan command buffer that applies write operations
    /// to matching records on the server without returning data to the client.
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn set_query_operate(
        &mut self,
        write_policy: &WritePolicy,
        statement: &Statement,
        task_id: u64,
        operations: &[Operation],
    ) -> Result<()> {
        let filter = statement.filters.as_ref().map(|filters| &filters[0]);

        self.begin();

        let mut field_count: u16 = 0;
        let mut filter_size = 0;

        if !statement.namespace.is_empty() {
            self.data_offset += statement.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !statement.set_name.is_empty() {
            self.data_offset += statement.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // Socket timeout field
        self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // TaskId field
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();
            if idx_type != CollectionIndexType::Default {
                self.data_offset += 1 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            filter_size = 1 + filter.estimate_size()?;
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.data_offset += ctx_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.data_offset += index_name.len() + FIELD_HEADER_SIZE as usize;
                    field_count += 1;
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.data_offset += exp_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }
        }

        let filter_exp_size = self.estimate_filter_size(write_policy.filter_expression())?;
        if filter_exp_size > 0 {
            field_count += 1;
        }

        // Estimate operation sizes
        for op in operations {
            if !op.is_write() {
                return Err(Error::InvalidArgument(
                    "Read operations not allowed in background query".into(),
                ));
            }
            self.data_offset += op.estimate_size()? + OPERATION_HEADER_SIZE as usize;
        }
        let operation_count = operations.len() as u16;

        self.size_buffer()?;

        // Background queries use the write header
        self.write_header_with_policy(write_policy, 0, INFO2_WRITE, field_count, operation_count);

        if !statement.namespace.is_empty() {
            self.write_field_string(&statement.namespace, FieldType::Namespace);
        }

        if !statement.set_name.is_empty() {
            self.write_field_string(&statement.set_name, FieldType::Table);
        }

        self.write_field_header(8, FieldType::QueryId);
        self.write_u64(task_id);

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();

            if idx_type != CollectionIndexType::Default {
                self.write_field_header(1, FieldType::IndexType);
                self.write_u8(idx_type as u8);
            }

            self.write_field_header(filter_size, FieldType::IndexRange);
            self.write_u8(1);

            filter.write(self)?;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.write_field_header(ctx_size, FieldType::IndexContext);
                encoder::pack_ctx_for_index(&mut Some(self), ctx)?;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.write_field_string(index_name, FieldType::IndexName);
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.write_field_header(exp_size, FieldType::IndexExpression);
                exp.pack(&mut Some(self))?;
            }
        }

        if let Some(filter_exp) = write_policy.filter_expression() {
            self.write_filter_expression(filter_exp, filter_exp_size);
        }

        // Write socket timeout
        self.write_field_header(4, FieldType::SocketTimeout);
        self.write_u32(write_policy.socket_timeout());

        // Write operations
        for op in operations {
            self.write_operation_for_operation(op)?;
        }

        self.end();
        Ok(())
    }

    /// Builds a background query/scan command buffer that applies a UDF
    /// to matching records on the server without returning data to the client.
    #[allow(clippy::cognitive_complexity)]
    pub(crate) fn set_query_udf_execute(
        &mut self,
        write_policy: &WritePolicy,
        statement: &Statement,
        task_id: u64,
    ) -> Result<()> {
        let filter = statement.filters.as_ref().map(|filters| &filters[0]);
        let aggregation = statement
            .aggregation
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("Statement must have aggregation set".into()))?;

        self.begin();

        let mut field_count: u16 = 0;
        let mut filter_size = 0;

        if !statement.namespace.is_empty() {
            self.data_offset += statement.namespace.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        if !statement.set_name.is_empty() {
            self.data_offset += statement.set_name.len() + FIELD_HEADER_SIZE as usize;
            field_count += 1;
        }

        // Socket timeout field
        self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        // TaskId field
        self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();
            if idx_type != CollectionIndexType::Default {
                self.data_offset += 1 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            filter_size = 1 + filter.estimate_size()?;
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            field_count += 1;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.data_offset += ctx_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.data_offset += index_name.len() + FIELD_HEADER_SIZE as usize;
                    field_count += 1;
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.data_offset += exp_size + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }
        }

        let filter_exp_size = self.estimate_filter_size(write_policy.filter_expression())?;
        if filter_exp_size > 0 {
            field_count += 1;
        }

        // UDF fields: UdfOp, UdfPackageName, UdfFunction, UdfArgList
        self.data_offset += 1 + FIELD_HEADER_SIZE as usize; // UdfOp
        self.data_offset += aggregation.package_name.len() + FIELD_HEADER_SIZE as usize;
        self.data_offset += aggregation.function_name.len() + FIELD_HEADER_SIZE as usize;
        self.estimate_args_size(aggregation.function_args.as_deref())?;
        field_count += 4;

        self.size_buffer()?;

        // Use query protocol header (write_header_read), not the key-value write header.
        // UDF background execution does not set INFO2_WRITE.
        self.write_header_read(&write_policy.base_policy, INFO1_READ, 0, 0, field_count, 0);

        if !statement.namespace.is_empty() {
            self.write_field_string(&statement.namespace, FieldType::Namespace);
        }

        if !statement.set_name.is_empty() {
            self.write_field_string(&statement.set_name, FieldType::Table);
        }

        self.write_field_header(8, FieldType::QueryId);
        self.write_u64(task_id);

        if let Some(filter) = filter {
            let idx_type = filter.collection_index_type.clone();

            if idx_type != CollectionIndexType::Default {
                self.write_field_header(1, FieldType::IndexType);
                self.write_u8(idx_type as u8);
            }

            self.write_field_header(filter_size, FieldType::IndexRange);
            self.write_u8(1);

            filter.write(self)?;

            if let Some(ref ctx) = filter.context {
                let ctx_size = encoder::pack_ctx_for_index(&mut None, ctx)?;
                self.write_field_header(ctx_size, FieldType::IndexContext);
                encoder::pack_ctx_for_index(&mut Some(self), ctx)?;
            }

            if let Some(ref index_name) = filter.index_name {
                if !index_name.is_empty() {
                    self.write_field_string(index_name, FieldType::IndexName);
                }
            }

            if let Some(ref exp) = filter.expression {
                let exp_size = exp.pack(&mut None)?;
                self.write_field_header(exp_size, FieldType::IndexExpression);
                exp.pack(&mut Some(self))?;
            }
        }

        if let Some(filter_exp) = write_policy.filter_expression() {
            self.write_filter_expression(filter_exp, filter_exp_size);
        }

        // Write socket timeout
        self.write_field_header(4, FieldType::SocketTimeout);
        self.write_u32(write_policy.socket_timeout());

        // Write UDF fields
        self.write_field_header(1, FieldType::UdfOp);
        self.write_u8(2); // 2 = no return data

        self.write_field_string(&aggregation.package_name, FieldType::UdfPackageName);
        self.write_field_string(&aggregation.function_name, FieldType::UdfFunction);
        self.write_args(aggregation.function_args.as_deref(), FieldType::UdfArgList)?;

        self.end();
        Ok(())
    }

    #[allow(clippy::ref_option)]
    fn estimate_filter_size(&mut self, filter: &Option<Expression>) -> Result<usize> {
        filter.clone().map_or(Ok(0), |filter| {
            let filter_size = filter.pack(&mut None)?;
            self.data_offset += filter_size + FIELD_HEADER_SIZE as usize;
            // filter_size + FIELD_HEADER_SIZE as usize
            Ok(filter_size)
        })
    }

    fn estimate_key_size(&mut self, key: &Key, send_key: bool) -> Result<u16> {
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
                self.data_offset += user_key.estimate_size()? + FIELD_HEADER_SIZE as usize + 1;
                field_count += 1;
            }
        }

        Ok(field_count)
    }

    fn estimate_args_size(&mut self, args: Option<&[Value]>) -> Result<()> {
        if let Some(args) = args {
            self.data_offset += encoder::pack_array(&mut None, args)? + FIELD_HEADER_SIZE as usize;
        } else {
            self.data_offset +=
                encoder::pack_empty_args_array(&mut None) + FIELD_HEADER_SIZE as usize;
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
        self.estimate_args_size(args)?;
        Ok(3)
    }

    fn estimate_operation_size_for_bin(&mut self, bin: &Bin) -> Result<()> {
        self.estimate_operation_size_for_bin_name(&bin.name)?;
        self.data_offset += bin.value.estimate_size()?;
        Ok(())
    }

    fn estimate_operation_size_for_bin_name(&mut self, bin_name: &str) -> Result<()> {
        if bin_name.len() > 15 {
            return Err(Error::InvalidArgument(
                "Bin name is longer than 15 bytes".into(),
            ));
        }
        self.data_offset += bin_name.len() + OPERATION_HEADER_SIZE as usize;
        Ok(())
    }

    const fn estimate_operation_size(&mut self) {
        self.data_offset += OPERATION_HEADER_SIZE as usize;
    }

    fn write_header(
        &mut self,
        policy: &BasePolicy,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) {
        let mut read_attr = read_attr;
        let b = self.compress_offset;
        let mut info_attr: u8 = 0;

        match policy.read_mode_sc {
            ReadModeSC::Session => {}
            ReadModeSC::Linearize => info_attr |= INFO3_SC_READ_TYPE,
            ReadModeSC::AllowReplica => info_attr |= INFO3_SC_READ_RELAX,
            ReadModeSC::AllowUnavailable => {
                info_attr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
            }
        }

        if policy.read_mode_ap == ReadModeAP::All {
            read_attr |= INFO1_READ_MODE_AP_ALL;
        }

        if policy.use_compression {
            read_attr |= INFO1_COMPRESS_RESPONSE;
        }

        // Write all header data except total size which must be written last.
        self.data_buffer[8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        self.data_buffer[9] = read_attr;
        self.data_buffer[10] = write_attr;
        self.data_buffer[11] = info_attr;

        for i in 12..26 {
            self.data_buffer[i] = 0;
        }

        // Write all header data except total size which must be written last.
        self.data_buffer[b + 8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        self.data_buffer[b + 9] = read_attr;
        self.data_buffer[b + 10] = write_attr;

        for i in 11..26 {
            self.data_buffer[b + i] = 0;
        }

        self.data_offset = b + 18;
        self.write_u32(policy.read_touch_ttl.into());

        self.data_offset = b + 26;
        self.write_u16(field_count);
        self.write_u16(operation_count);

        self.data_offset = b + MSG_TOTAL_HEADER_SIZE as usize;
    }

    fn write_header_read(
        &mut self,
        policy: &BasePolicy,
        read_attr: u8,
        write_attr: u8,
        info_attr: u8,
        field_count: u16,
        operation_count: u16,
    ) {
        let mut read_attr = read_attr;
        let b = self.compress_offset;
        let mut info_attr = info_attr;

        match policy.read_mode_sc {
            ReadModeSC::Session => {}
            ReadModeSC::Linearize => info_attr |= INFO3_SC_READ_TYPE,
            ReadModeSC::AllowReplica => info_attr |= INFO3_SC_READ_RELAX,
            ReadModeSC::AllowUnavailable => {
                info_attr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
            }
        }

        if policy.read_mode_ap == ReadModeAP::All {
            read_attr |= INFO1_READ_MODE_AP_ALL;
        }

        if policy.use_compression {
            read_attr |= INFO1_COMPRESS_RESPONSE;
        }

        // Write all header data except total size which must be written last.
        self.data_buffer[b + 8] = MSG_REMAINING_HEADER_SIZE; // Message header length.
        self.data_buffer[b + 9] = read_attr;
        self.data_buffer[b + 10] = write_attr;
        self.data_buffer[b + 11] = info_attr;

        for i in 12..26 {
            self.data_buffer[b + i] = 0;
        }

        self.data_offset = b + 18;
        self.write_u32(policy.read_touch_ttl.into());

        self.data_offset = b + 26;
        self.write_u16(field_count);
        self.write_u16(operation_count);

        self.data_offset = b + MSG_TOTAL_HEADER_SIZE as usize;
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

        if policy.durable_delete {
            write_attr |= INFO2_DURABLE_DELETE;
        }

        if policy.base_policy.use_compression {
            read_attr |= INFO1_COMPRESS_RESPONSE;
        }

        let mut txn_attr: u8 = 0;
        if policy.on_locking_only {
            txn_attr |= INFO4_MRT_ON_LOCKING_ONLY;
        }

        match policy.base_policy.read_mode_sc {
            ReadModeSC::Session => {}
            ReadModeSC::Linearize => info_attr |= INFO3_SC_READ_TYPE,
            ReadModeSC::AllowReplica => info_attr |= INFO3_SC_READ_RELAX,
            ReadModeSC::AllowUnavailable => {
                info_attr |= INFO3_SC_READ_TYPE | INFO3_SC_READ_RELAX;
            }
        }

        if policy.base_policy.read_mode_ap == ReadModeAP::All {
            read_attr |= INFO1_READ_MODE_AP_ALL;
        }

        // Write all header data except total size which must be written last.
        let b = self.compress_offset;
        self.data_offset = b + 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE); // Message header length.
        self.write_u8(read_attr);
        self.write_u8(write_attr);
        self.write_u8(info_attr);
        self.write_u8(txn_attr); // INFO4 byte for MRT attributes
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
        self.data_offset = b + MSG_TOTAL_HEADER_SIZE as usize;
    }

    fn write_key(&mut self, key: &Key, send_key: bool) -> Result<()> {
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
                self.write_field_value(user_key, FieldType::Key)?;
            }
        }

        Ok(())
    }

    fn write_filter_expression(&mut self, filter: &Expression, size: usize) {
        self.write_field_header(size, FieldType::FilterExp);
        let _ = filter.pack(&mut Some(self));
    }

    fn write_field_header(&mut self, size: usize, ftype: FieldType) {
        self.write_i32(size as i32 + 1);
        self.write_u8(ftype as u8);
    }

    fn write_field_u64(&mut self, field: u64, ftype: FieldType) {
        self.write_field_header(8, ftype);
        self.write_u64(field);
    }

    fn write_field_u32(&mut self, field: u32, ftype: FieldType) {
        self.write_field_header(4, ftype);
        self.write_u32(field);
    }

    fn write_field_string(&mut self, field: &str, ftype: FieldType) {
        self.write_field_header(field.len(), ftype);
        self.write_str(field);
    }

    fn write_field_bytes(&mut self, bytes: &[u8], ftype: FieldType) {
        self.write_field_header(bytes.len(), ftype);
        self.write_bytes(bytes);
    }

    fn write_field_value(&mut self, value: &Value, ftype: FieldType) -> Result<()> {
        self.write_field_header(value.estimate_size()? + 1, ftype);
        self.write_u8(value.particle_type() as u8);
        value.write_to(self)?;
        Ok(())
    }

    fn write_args(&mut self, args: Option<&[Value]>, ftype: FieldType) -> Result<()> {
        if let Some(args) = args {
            self.write_field_header(encoder::pack_array(&mut None, args)?, ftype);
            encoder::pack_array(&mut Some(self), args)?;
        } else {
            self.write_field_header(encoder::pack_empty_args_array(&mut None), ftype);
            encoder::pack_empty_args_array(&mut Some(self));
        }
        Ok(())
    }

    fn write_operation_for_bin(&mut self, bin: &Bin, op_type: OperationType) -> Result<()> {
        let name_length = bin.name.len();
        let value_length = bin.value.estimate_size()?;

        self.write_i32((name_length + value_length + 4) as i32);
        self.write_u8(op_type as u8);
        self.write_u8(bin.value.particle_type() as u8);
        self.write_u8(0);
        self.write_u8(name_length as u8);
        self.write_str(&bin.name);
        bin.value.write_to(self)?;
        Ok(())
    }

    fn write_operation_for_bin_name(&mut self, name: &str, op_type: OperationType) {
        self.write_i32(name.len() as i32 + 4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(name.len() as u8);
        self.write_str(name);
    }

    fn write_operation_for_operation(&mut self, op: &Operation) -> Result<usize> {
        op.write_to(self)
    }

    fn write_operation_for_operation_type(&mut self, op_type: OperationType) {
        self.write_i32(4);
        self.write_u8(op_type as u8);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
    }

    // Data buffer implementations

    pub(crate) const fn data_offset(&self) -> usize {
        self.data_offset
    }

    pub(crate) const fn skip_bytes(&mut self, count: usize) {
        self.data_offset += count;
    }

    pub(crate) const fn skip(&mut self, count: usize) {
        self.data_offset += count;
    }

    pub(crate) fn peek(&self) -> u8 {
        self.data_buffer[self.data_offset]
    }

    pub(crate) fn peek_n(&self, n: usize) -> u8 {
        self.data_buffer[self.data_offset + n]
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u8(&mut self, pos: Option<usize>) -> u8 {
        if let Some(pos) = pos {
            self.data_buffer[pos]
        } else {
            let res = self.data_buffer[self.data_offset];
            self.data_offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_i8(&mut self, pos: Option<usize>) -> i8 {
        if let Some(pos) = pos {
            self.data_buffer[pos] as i8
        } else {
            let res = self.data_buffer[self.data_offset] as i8;
            self.data_offset += 1;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u16(&mut self, pos: Option<usize>) -> u16 {
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

    pub(crate) fn read_i16(&mut self, pos: Option<usize>) -> i16 {
        let val = self.read_u16(pos);
        val as i16
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u32(&mut self, pos: Option<usize>) -> u32 {
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

    pub(crate) fn read_i32(&mut self, pos: Option<usize>) -> i32 {
        let val = self.read_u32(pos);
        val as i32
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u32_little_endian(&mut self, pos: Option<usize>) -> u32 {
        let len = 4;
        if let Some(pos) = pos {
            LittleEndian::read_u32(&self.data_buffer[pos..pos + len])
        } else {
            let res =
                LittleEndian::read_u32(&self.data_buffer[self.data_offset..self.data_offset + len]);
            self.data_offset += len;
            res
        }
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u64(&mut self, pos: Option<usize>) -> u64 {
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

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_u64_value(&mut self, pos: Option<usize>) -> Value {
        let len = 8;
        let val = if let Some(pos) = pos {
            NetworkEndian::read_u64(&self.data_buffer[pos..pos + len])
        } else {
            let res = NetworkEndian::read_u64(
                &self.data_buffer[self.data_offset..self.data_offset + len],
            );
            self.data_offset += len;
            res
        };

        Value::Int(val as i64)
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_le_u64(&mut self, pos: Option<usize>) -> u64 {
        let len = 8;
        if let Some(pos) = pos {
            LittleEndian::read_u64(&self.data_buffer[pos..pos + len])
        } else {
            let res =
                LittleEndian::read_u64(&self.data_buffer[self.data_offset..self.data_offset + len]);
            self.data_offset += len;
            res
        }
    }

    pub(crate) fn read_i64(&mut self, pos: Option<usize>) -> i64 {
        let val = self.read_u64(pos);
        val as i64
    }

    #[allow(clippy::option_if_let_else)]
    pub(crate) fn read_f32(&mut self, pos: Option<usize>) -> f32 {
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
    pub(crate) fn read_f64(&mut self, pos: Option<usize>) -> f64 {
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

    pub(crate) fn read_str(&mut self, len: usize) -> Result<String> {
        let s = str::from_utf8(&self.data_buffer[self.data_offset..self.data_offset + len])?;
        self.data_offset += len;
        Ok(s.to_owned())
    }

    pub(crate) fn read_str_until(&mut self, sep: u8, max_len: usize) -> Result<String> {
        let mut len = 0;
        for i in 0..max_len {
            len += 1;
            if self.data_buffer[self.data_offset + i] == sep {
                break;
            }
        }

        let s = str::from_utf8(&self.data_buffer[self.data_offset..self.data_offset + len])?;
        self.data_offset += len;
        Ok(s.to_owned())
    }

    // pub(crate) fn read_bytes(&mut self, pos: usize, count: usize) -> &[u8] {
    //     &self.data_buffer[pos..pos + count]
    // }

    pub(crate) fn read_slice(&self, count: usize) -> &[u8] {
        &self.data_buffer[self.data_offset..self.data_offset + count]
    }

    pub(crate) fn read_blob(&mut self, len: usize) -> Vec<u8> {
        let val = self.data_buffer[self.data_offset..self.data_offset + len].to_vec();
        self.data_offset += len;
        val
    }

    pub(crate) fn read_bool(&mut self, len: usize) -> bool {
        if len == 0 {
            false
        } else {
            let val = self.data_buffer[self.data_offset];
            self.data_offset += len;
            val != 0
        }
    }

    pub(crate) fn write_u8(&mut self, val: u8) -> usize {
        self.data_buffer[self.data_offset] = val;
        self.data_offset += 1;
        1
    }

    pub(crate) fn write_i8(&mut self, val: i8) -> usize {
        self.data_buffer[self.data_offset] = val as u8;
        self.data_offset += 1;
        1
    }

    pub(crate) fn write_u16(&mut self, val: u16) -> usize {
        NetworkEndian::write_u16(
            &mut self.data_buffer[self.data_offset..self.data_offset + 2],
            val,
        );
        self.data_offset += 2;
        2
    }

    pub(crate) fn write_u64_little_endian(&mut self, val: u64) -> usize {
        LittleEndian::write_u64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub(crate) fn write_u16_little_endian(&mut self, val: u16) -> usize {
        LittleEndian::write_u16(
            &mut self.data_buffer[self.data_offset..self.data_offset + 2],
            val,
        );
        self.data_offset += 2;
        2
    }

    pub(crate) fn write_i16(&mut self, val: i16) -> usize {
        self.write_u16(val as u16)
    }

    pub(crate) fn write_u32(&mut self, val: u32) -> usize {
        NetworkEndian::write_u32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        4
    }

    pub(crate) fn write_i32(&mut self, val: i32) -> usize {
        self.write_u32(val as u32)
    }

    pub(crate) fn write_u64(&mut self, val: u64) -> usize {
        NetworkEndian::write_u64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub(crate) fn write_i64(&mut self, val: i64) -> usize {
        NetworkEndian::write_i64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub(crate) fn write_bool(&mut self, val: bool) -> usize {
        let val = i8::from(val);
        self.write_i8(val)
    }

    pub(crate) fn write_f32(&mut self, val: f32) -> usize {
        NetworkEndian::write_f32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        4
    }

    pub(crate) fn write_f64(&mut self, val: f64) -> usize {
        NetworkEndian::write_f64(
            &mut self.data_buffer[self.data_offset..self.data_offset + 8],
            val,
        );
        self.data_offset += 8;
        8
    }

    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> usize {
        for b in bytes {
            self.write_u8(*b);
        }
        bytes.len()
    }

    pub(crate) fn write_str(&mut self, val: &str) -> usize {
        self.write_bytes(val.as_bytes())
    }

    pub(crate) fn write_geo(&mut self, value: &str) -> usize {
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_bytes(value.as_bytes());
        3 + value.len()
    }

    pub(crate) fn write_timeout(&mut self, millis: u32) {
        let b = self.compress_offset;
        NetworkEndian::write_u32(&mut self.data_buffer[b + 22..b + 22 + 4], millis);
    }

    pub(crate) fn write_u32_little_endian(&mut self, val: u32) -> usize {
        LittleEndian::write_u32(
            &mut self.data_buffer[self.data_offset..self.data_offset + 4],
            val,
        );
        self.data_offset += 4;
        4
    }

    // --- MRT (Multi-Record Transaction) helpers ---

    /// Write a little-endian 64-bit field (used for `MRT_ID`).
    fn write_field_le64(&mut self, val: i64, ftype: FieldType) {
        self.write_field_header(8, ftype);
        self.write_u64_little_endian(val as u64);
    }

    /// Write a little-endian 32-bit field (used for `MRT_DEADLINE`).
    fn write_field_le32(&mut self, val: i32, ftype: FieldType) {
        self.write_field_header(4, ftype);
        self.write_u32_little_endian(val as u32);
    }

    /// Write a 7-byte record version field (little-endian).
    fn write_field_version(&mut self, ver: u64) {
        self.write_field_header(7, FieldType::RecordVersion);
        let offset = self.data_offset;
        self.data_buffer[offset] = ver as u8;
        self.data_buffer[offset + 1] = (ver >> 8) as u8;
        self.data_buffer[offset + 2] = (ver >> 16) as u8;
        self.data_buffer[offset + 3] = (ver >> 24) as u8;
        self.data_buffer[offset + 4] = (ver >> 32) as u8;
        self.data_buffer[offset + 5] = (ver >> 40) as u8;
        self.data_buffer[offset + 6] = (ver >> 48) as u8;
        self.data_offset += 7;
    }

    /// Read a 7-byte record version from a byte slice (little-endian) into a u64.
    pub(crate) fn version_bytes_to_u64(buf: &[u8], offset: usize) -> u64 {
        (u64::from(buf[offset]))
            | (u64::from(buf[offset + 1]) << 8)
            | (u64::from(buf[offset + 2]) << 16)
            | (u64::from(buf[offset + 3]) << 24)
            | (u64::from(buf[offset + 4]) << 32)
            | (u64::from(buf[offset + 5]) << 40)
            | (u64::from(buf[offset + 6]) << 48)
    }

    /// Parse response fields and extract the record version (if present).
    /// Advances `data_offset` past all fields.
    pub(crate) fn parse_fields_for_version(&mut self, field_count: usize) -> Option<u64> {
        let mut version = None;
        for _ in 0..field_count {
            let field_len = self.read_u32(None) as usize;
            let field_type = self.read_u8(None);
            let data_size = field_len - 1;

            if field_type == FieldType::RecordVersion as u8 && data_size == 7 {
                version = Some(Self::version_bytes_to_u64(
                    &self.data_buffer,
                    self.data_offset,
                ));
            }
            self.data_offset += data_size;
        }
        version
    }

    /// Estimate the size of transaction fields and return the number of extra fields added.
    /// `version` is the cached read version for the key, looked up from the txn.
    pub(crate) fn size_txn(
        &mut self,
        key: &Key,
        txn: Option<&Arc<Txn>>,
        has_write: bool,
    ) -> (u16, Option<u64>) {
        let mut field_count: u16 = 0;
        let mut version: Option<u64> = None;

        if let Some(txn) = txn {
            // MRT_ID field: 8 bytes + field header
            self.data_offset += 8 + FIELD_HEADER_SIZE as usize;
            field_count += 1;

            version = txn.get_read_version(key);

            if version.is_some() {
                // RECORD_VERSION field: 7 bytes + field header
                self.data_offset += 7 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }

            if has_write && txn.monitor_exists() {
                // MRT_DEADLINE field: 4 bytes + field header
                self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
                field_count += 1;
            }
        }
        (field_count, version)
    }

    /// Estimate the size of transaction fields for batch commands.
    pub(crate) fn size_txn_batch(
        &mut self,
        txn: Option<&Arc<Txn>>,
        ver: Option<u64>,
        has_write: bool,
    ) {
        if let Some(txn) = txn {
            self.data_offset += 1; // info4 byte
            self.data_offset += 8 + FIELD_HEADER_SIZE as usize;

            if ver.is_some() {
                self.data_offset += 7 + FIELD_HEADER_SIZE as usize;
            }

            if has_write && txn.monitor_exists() {
                self.data_offset += 4 + FIELD_HEADER_SIZE as usize;
            }
        }
    }

    /// Write transaction fields (`MRT_ID`, `RECORD_VERSION`, `MRT_DEADLINE`).
    pub(crate) fn write_txn(
        &mut self,
        txn: Option<&Arc<Txn>>,
        version: Option<u64>,
        send_deadline: bool,
    ) {
        if let Some(txn) = txn {
            self.write_field_le64(txn.id(), FieldType::MrtId);

            if let Some(ver) = version {
                self.write_field_version(ver);
            }

            if send_deadline && txn.monitor_exists() {
                self.write_field_le32(txn.get_deadline(), FieldType::MrtDeadline);
            }
        }
    }

    /// Estimate key size without `send_key` (for txn monitor commands).
    const fn estimate_raw_key_size(&mut self, key: &Key) -> u16 {
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

        field_count
    }

    /// Write the txn monitor record header (simple header without policy-driven flags).
    fn write_txn_monitor(
        &mut self,
        key: &Key,
        read_attr: u8,
        write_attr: u8,
        field_count: u16,
        op_count: u16,
    ) -> Result<()> {
        self.size_buffer()?;
        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE);
        self.write_u8(read_attr);
        self.write_u8(write_attr);
        self.write_u8(0); // info3
        self.write_u8(0); // info4
        self.write_u8(0); // unused
        self.write_u32(0); // generation
        self.write_u32(0); // expiration
        self.write_u32(0); // timeout
        self.write_u16(field_count);
        self.write_u16(op_count);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;
        self.write_key(key, false)?;
        Ok(())
    }

    /// Build the command to verify a record version during MRT commit.
    pub(crate) fn set_txn_verify(
        &mut self,
        _policy: &BasePolicy,
        key: &Key,
        ver: u64,
    ) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_raw_key_size(key);

        // Version field: 7 bytes + field header
        self.data_offset += 7 + FIELD_HEADER_SIZE as usize;
        field_count += 1;

        self.size_buffer()?;

        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE);
        self.write_u8(INFO1_READ | INFO1_NOBINDATA);
        self.write_u8(0);
        self.write_u8(INFO3_SC_READ_TYPE);
        self.write_u8(INFO4_MRT_VERIFY_READ);
        self.write_u8(0);
        self.write_u32(0);
        self.write_u32(0);
        self.write_u32(0);
        self.write_u16(field_count);
        self.write_u16(0);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        self.write_key(key, false)?;
        self.write_field_version(ver);
        self.end();
        Ok(())
    }

    /// Build the command to roll forward or roll back a record during MRT commit/abort.
    pub(crate) fn set_txn_roll(&mut self, key: &Key, txn: &Arc<Txn>, txn_attr: u8) -> Result<()> {
        self.begin();
        let mut field_count = self.estimate_raw_key_size(key);

        let (extra_fields, version) = self.size_txn(key, Some(txn), false);
        field_count += extra_fields;

        self.size_buffer()?;

        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE);
        self.write_u8(0);
        self.write_u8(INFO2_WRITE | INFO2_DURABLE_DELETE);
        self.write_u8(0);
        self.write_u8(txn_attr);
        self.write_u8(0);
        self.write_u32(0);
        self.write_u32(0);
        self.write_u32(0);
        self.write_u16(field_count);
        self.write_u16(0);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        self.write_key(key, false)?;
        self.write_txn(Some(txn), version, false);
        self.end();
        Ok(())
    }

    /// Build the command to mark a transaction monitor record as roll-forward.
    pub(crate) fn set_txn_mark_roll_forward(&mut self, key: &Key) -> Result<()> {
        let bin = Bin::new("fwd".to_string(), Value::Bool(true));

        self.begin();
        let field_count = self.estimate_raw_key_size(key);
        self.estimate_operation_size_for_bin(&bin)?;
        self.write_txn_monitor(key, 0, INFO2_WRITE, field_count, 1)?;
        self.write_operation_for_bin(&bin, OperationType::Write)?;
        self.end();
        Ok(())
    }

    /// Build the command to close (delete) a transaction monitor record.
    pub(crate) fn set_txn_close(&mut self, key: &Key) -> Result<()> {
        self.begin();
        let field_count = self.estimate_raw_key_size(key);
        self.write_txn_monitor(
            key,
            0,
            INFO2_WRITE | INFO2_DELETE | INFO2_DURABLE_DELETE,
            field_count,
            0,
        )?;
        self.end();
        Ok(())
    }

    /// Build the command to add keys to a transaction monitor record.
    pub(crate) fn set_txn_add_keys(
        &mut self,
        policy: &WritePolicy,
        key: &Key,
        operations: &[Operation],
    ) -> Result<()> {
        self.begin();
        let field_count = self.estimate_raw_key_size(key);

        for op in operations {
            self.data_offset += op.estimate_size()? + OPERATION_HEADER_SIZE as usize;
        }

        self.size_buffer()?;

        self.data_offset = 8;
        self.write_u8(MSG_REMAINING_HEADER_SIZE);
        self.write_u8(0); // read_attr from args
        self.write_u8(INFO2_WRITE | INFO2_RESPOND_ALL_OPS); // write_attr from args
        self.write_u8(0);
        self.write_u8(0);
        self.write_u8(0);
        self.write_u32(0);
        self.write_u32(policy.expiration.into());
        self.write_u32(0);
        self.write_u16(field_count);
        self.write_u16(operations.len() as u16);
        self.data_offset = MSG_TOTAL_HEADER_SIZE as usize;

        self.write_key(key, false)?;

        for op in operations {
            self.write_operation_for_operation(op)?;
        }

        self.end();
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn dump_buffer(&self) {
        rhexdump!(&self.data_buffer);
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    /// Helper: build a Buffer that looks like a real command was written.
    /// Returns (buffer, uncompressed_payload) where uncompressed_payload is
    /// the command bytes at [compress_offset .. data_offset] before end() is called.
    fn make_command_buffer(payload_size: usize, use_compress: bool) -> Buffer {
        let mut buf = Buffer::new(0);
        buf.set_compress(use_compress, DEFAULT_COMPRESS_THRESHOLD);
        buf.begin();

        // Simulate writing a header via write_header (read-only command)
        let b = buf.compress_offset;
        buf.size_buffer().unwrap();
        // Manually fill the header area
        buf.data_buffer[b + 8] = MSG_REMAINING_HEADER_SIZE;
        buf.data_buffer[b + 9] = INFO1_READ;
        for i in 10..26 {
            buf.data_buffer[b + i] = 0;
        }
        buf.data_offset = b + MSG_TOTAL_HEADER_SIZE as usize;

        // Append payload bytes to reach the desired size
        let total = b + MSG_TOTAL_HEADER_SIZE as usize + payload_size;
        buf.data_buffer.resize(total, 0);
        for i in (b + MSG_TOTAL_HEADER_SIZE as usize)..total {
            buf.data_buffer[i] = (i % 251) as u8; // deterministic pattern
        }
        buf.data_offset = total;

        buf.end();
        buf
    }

    #[test]
    fn set_compress_sets_offset() {
        let mut buf = Buffer::new(0);
        assert_eq!(buf.compress_offset, 0);

        buf.set_compress(true, DEFAULT_COMPRESS_THRESHOLD);
        assert_eq!(buf.compress_offset, 16);
        assert_eq!(buf.compress_threshold, DEFAULT_COMPRESS_THRESHOLD);

        buf.set_compress(false, DEFAULT_COMPRESS_THRESHOLD);
        assert_eq!(buf.compress_offset, 0);
    }

    #[test]
    fn set_compress_records_threshold() {
        let mut buf = Buffer::new(0);
        buf.set_compress(true, 4096);
        assert_eq!(buf.compress_threshold, 4096);
    }

    #[test]
    fn begin_accounts_for_compress_offset() {
        let mut buf = Buffer::new(0);

        buf.set_compress(false, DEFAULT_COMPRESS_THRESHOLD);
        buf.begin();
        assert_eq!(buf.data_offset, MSG_TOTAL_HEADER_SIZE as usize);

        buf.set_compress(true, DEFAULT_COMPRESS_THRESHOLD);
        buf.begin();
        assert_eq!(buf.data_offset, 16 + MSG_TOTAL_HEADER_SIZE as usize);
    }

    #[test]
    fn end_writes_proto_at_compress_offset() {
        // Without compression: proto at [0..8]
        let buf_no = make_command_buffer(100, false);
        let proto_no = NetworkEndian::read_u64(&buf_no.data_buffer[0..8]);
        let msg_type_no = ((proto_no >> 48) & 0xFF) as u8;
        assert_eq!(msg_type_no, AS_MSG_TYPE);

        // With compression: proto at [16..24]
        let buf_yes = make_command_buffer(100, true);
        let proto_yes = NetworkEndian::read_u64(&buf_yes.data_buffer[16..24]);
        let msg_type_yes = ((proto_yes >> 48) & 0xFF) as u8;
        assert_eq!(msg_type_yes, AS_MSG_TYPE);

        // The first 16 bytes should be zeroed (reserved for compress header)
        assert!(buf_yes.data_buffer[0..16].iter().all(|&b| b == 0));
    }

    #[test]
    fn compress_below_threshold_returns_uncompressed() {
        // A small command (header + 10 bytes payload = 40 bytes total, well below 128)
        let mut buf = make_command_buffer(10, true);
        let original_buf_len = buf.data_buffer.len();

        buf.compress().unwrap();

        // Should have shifted data to position 0 (removed the 16-byte pad)
        assert_eq!(buf.data_buffer.len(), original_buf_len - 16);
        assert_eq!(buf.data_offset, buf.data_buffer.len());

        // Proto header should be at [0..8] with type AS_MSG_TYPE (3), not compressed
        let proto = NetworkEndian::read_u64(&buf.data_buffer[0..8]);
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        assert_eq!(msg_type, AS_MSG_TYPE);
    }

    #[test]
    fn compress_threshold_is_configurable() {
        // Payload (=200 bytes total command) sits above the default 128
        // gate but below a custom 4096-byte gate. With the higher gate
        // configured, `compress()` must short-circuit and leave the
        // buffer uncompressed (the proto header should still be the
        // plain `AS_MSG_TYPE`, not `AS_MSG_TYPE_COMPRESSED`).
        let payload_size = 200;
        let mut buf = make_command_buffer(payload_size, true);
        // Override the default gate baked in by `make_command_buffer`
        // *after* the command was built — `compress()` only consults
        // `compress_threshold` at compress-time, so a late override is
        // sufficient (and matches how a per-policy threshold would be
        // wired through `set_compress`).
        buf.compress_threshold = 4096;

        buf.compress().unwrap();

        let proto = NetworkEndian::read_u64(&buf.data_buffer[0..8]);
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        assert_eq!(
            msg_type, AS_MSG_TYPE,
            "configured gate should suppress compression below threshold"
        );
    }

    #[test]
    fn compress_round_trip() {
        // Build a command large enough to trigger compression (> 128 bytes payload)
        let payload_size = 500;
        let mut buf = make_command_buffer(payload_size, true);

        // Save the uncompressed command bytes (at [16..buf_len])
        let uncompressed = buf.data_buffer[16..].to_vec();

        buf.compress().unwrap();

        // Verify compressed message structure
        let proto = NetworkEndian::read_u64(&buf.data_buffer[0..8]);
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        let compressed_payload_size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;
        assert_eq!(msg_type, AS_MSG_TYPE_COMPRESSED);

        // compressed_payload_size includes the 8-byte uncompressed size field
        assert!(compressed_payload_size >= 8);

        let stored_uncompressed_size = NetworkEndian::read_u64(&buf.data_buffer[8..16]) as usize;
        assert_eq!(stored_uncompressed_size, uncompressed.len());

        // Decompress and verify round-trip
        let compressed_data = &buf.data_buffer[16..16 + compressed_payload_size - 8];
        let mut decoder = ZlibDecoder::new(compressed_data);
        let mut decompressed = vec![0u8; stored_uncompressed_size];
        decoder.read_exact(&mut decompressed).unwrap();

        assert_eq!(decompressed, uncompressed);
    }

    #[test]
    fn compress_without_pad_is_noop() {
        // compress_offset == 0: compress() should detect nothing above threshold
        // since the command itself is only 30 + 10 = 40 bytes.
        let mut buf = make_command_buffer(10, false);
        let before = buf.data_buffer.clone();

        buf.compress().unwrap();

        assert_eq!(buf.data_buffer, before);
    }

    #[test]
    fn compress_large_payload_round_trip() {
        // Test with a payload larger than the 64KB chunk step
        let payload_size = 128 * 1024;
        let mut buf = make_command_buffer(payload_size, true);

        let uncompressed = buf.data_buffer[16..].to_vec();
        buf.compress().unwrap();

        // Verify message type
        let proto = NetworkEndian::read_u64(&buf.data_buffer[0..8]);
        let msg_type = ((proto >> 48) & 0xFF) as u8;
        assert_eq!(msg_type, AS_MSG_TYPE_COMPRESSED);

        // Compressed should be smaller than uncompressed for patterned data
        assert!(buf.data_buffer.len() < 16 + uncompressed.len());

        // Round-trip
        let compressed_payload_size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;
        let stored_uncompressed_size = NetworkEndian::read_u64(&buf.data_buffer[8..16]) as usize;
        let compressed_data = &buf.data_buffer[16..16 + compressed_payload_size - 8];

        let mut decoder = ZlibDecoder::new(compressed_data);
        let mut decompressed = vec![0u8; stored_uncompressed_size];
        decoder.read_exact(&mut decompressed).unwrap();

        assert_eq!(decompressed, uncompressed);
    }

    #[test]
    fn write_timeout_respects_compress_offset() {
        let mut buf = make_command_buffer(100, false);
        buf.write_timeout(42_000);
        let val_no_pad = NetworkEndian::read_u32(&buf.data_buffer[22..26]);
        assert_eq!(val_no_pad, 42_000);

        let mut buf = make_command_buffer(100, true);
        buf.write_timeout(42_000);
        let val_with_pad = NetworkEndian::read_u32(&buf.data_buffer[16 + 22..16 + 26]);
        assert_eq!(val_with_pad, 42_000);
    }

    #[test]
    fn connection_inflate_round_trip() {
        // Simulate what Connection::inflate does: given compressed data and
        // uncompressed size, decompress and verify the inner proto header.
        let payload_size = 300;
        let mut buf = make_command_buffer(payload_size, true);
        let uncompressed = buf.data_buffer[16..].to_vec();

        buf.compress().unwrap();

        // Extract compressed data (what the server would send)
        let proto = NetworkEndian::read_u64(&buf.data_buffer[0..8]);
        let compressed_payload_size = (proto & 0x0000_FFFF_FFFF_FFFF) as usize;
        let stored_uncompressed_size = NetworkEndian::read_u64(&buf.data_buffer[8..16]) as usize;
        let compressed_data = &buf.data_buffer[16..16 + compressed_payload_size - 8];

        // Decompress like Connection::inflate would
        let mut decoder = ZlibDecoder::new(compressed_data);
        let mut decompressed = vec![0u8; stored_uncompressed_size];
        decoder.read_exact(&mut decompressed).unwrap();

        // The decompressed data should start with a valid proto header
        let inner_proto = NetworkEndian::read_u64(&decompressed[0..8]);
        let inner_msg_type = ((inner_proto >> 48) & 0xFF) as u8;
        let inner_version = ((inner_proto >> 56) & 0xFF) as u8;
        assert_eq!(inner_version, CL_MSG_VERSION);
        assert_eq!(inner_msg_type, AS_MSG_TYPE);

        // And the body should match
        assert_eq!(decompressed, uncompressed);
    }
}
