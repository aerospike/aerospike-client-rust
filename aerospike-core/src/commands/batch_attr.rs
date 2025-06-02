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

use crate::batch::BatchDeletePolicy;
use crate::batch::BatchReadPolicy;
use crate::batch::BatchUDFPolicy;
use crate::batch::BatchWritePolicy;
use crate::commands::buffer;
use crate::expressions::FilterExpression;
use crate::operations::{Operation, OperationBin, OperationType};
use crate::policy::BatchPolicy;
use crate::CommitLevel;
use crate::GenerationPolicy;
use crate::RecordExistsAction;

pub(crate) struct BatchAttr {
    pub(crate) filter_expression: Option<FilterExpression>,
    pub(crate) read_attr: u8,
    pub(crate) write_attr: u8,
    pub(crate) info_attr: u8,
    pub(crate) txn_attr: u8,
    pub(crate) expiration: u32,
    pub(crate) generation: u32,
    pub(crate) has_write: bool,
    pub(crate) send_key: bool,
}

impl Default for BatchAttr {
    fn default() -> Self {
        BatchAttr {
            filter_expression: None,
            read_attr: 0,
            write_attr: 0,
            info_attr: 0,
            txn_attr: 0,
            expiration: 0,
            generation: 0,
            has_write: false,
            send_key: false,
        }
    }
}

impl BatchAttr {
    pub(crate) fn set_read(&mut self, rp: &BatchPolicy) {
        self.filter_expression = None;
        self.read_attr = buffer::INFO1_READ;

        // if rp.ReadModeAP == ReadModeAPAll {
        // 	self.read_attr |= buffer::INFO1_READ_MODE_AP_ALL
        // }

        self.write_attr = 0;

        // switch rp.ReadModeSC {
        // default:
        // case ReadModeSCSession:
        // 	self.info_attr = 0
        // case ReadModeSCLinearize:
        // 	self.info_attr = buffer::INFO3_SC_READ_TYPE
        // case ReadModeSCAllowReplica:
        // 	self.info_attr = buffer::INFO3_SC_READ_RELAX
        // case ReadModeSCAllowUnavailable:
        // 	self.info_attr = buffer::INFO3_SC_READ_TYPE | buffer::INFO3_SC_READ_RELAX
        // }
        self.txn_attr = 0;
        self.expiration = rp.base_policy.read_touch_ttl.into();
        self.generation = 0;
        self.has_write = false;
        self.send_key = false;
    }

    pub(crate) fn set_batch_read(&mut self, rp: &BatchReadPolicy) {
        self.filter_expression = rp.filter_expression.clone();
        self.read_attr = buffer::INFO1_READ;

        // if rp.ReadModeAP == ReadModeAPAll {
        // 	self.read_attr |= buffer::INFO1_READ_MODE_AP_ALL
        // }

        self.write_attr = 0;

        // switch rp.ReadModeSC {
        // default:
        // case ReadModeSCSession:
        // 	self.info_attr = 0
        // case ReadModeSCLinearize:
        // 	self.info_attr = buffer::INFO3_SC_READ_TYPE
        // case ReadModeSCAllowReplica:
        // 	self.info_attr = buffer::INFO3_SC_READ_RELAX
        // case ReadModeSCAllowUnavailable:
        // 	self.info_attr = buffer::INFO3_SC_READ_TYPE | buffer::INFO3_SC_READ_RELAX
        // }
        self.txn_attr = 0;
        self.expiration = rp.read_touch_ttl.into();
        self.generation = 0;
        self.has_write = false;
        self.send_key = false;
    }

    pub(crate) fn adjust_read<'a>(&mut self, ops: &Vec<Operation<'a>>) {
        for op in ops {
            match op.op {
                OperationType::Read => match op.bin {
                    OperationBin::All => {
                        self.read_attr |= buffer::INFO1_GET_ALL;
                    }
                    OperationBin::None => {
                        self.read_attr |= buffer::INFO1_NOBINDATA;
                    }
                    _ => (),
                },
                _ => (),
            }
        }
    }

    pub(crate) fn adjust_read_for_all_bins(&mut self, read_all_bins: bool) {
        if read_all_bins {
            self.read_attr |= buffer::INFO1_GET_ALL;
        } else {
            self.read_attr |= buffer::INFO1_NOBINDATA;
        }
    }

    pub(crate) fn set_batch_write(&mut self, wp: &BatchWritePolicy) {
        self.filter_expression = wp.filter_expression.clone();
        self.read_attr = 0;
        self.write_attr = buffer::INFO2_WRITE | buffer::INFO2_RESPOND_ALL_OPS;
        self.info_attr = 0;
        self.txn_attr = 0;
        self.expiration = wp.expiration.into();
        self.has_write = true;
        self.send_key = wp.send_key;

        match wp.generation_policy {
            GenerationPolicy::None => {
                self.generation = 0;
            }

            GenerationPolicy::ExpectGenEqual => {
                self.generation = wp.generation;
                self.write_attr |= buffer::INFO2_GENERATION;
            }

            GenerationPolicy::ExpectGenGreater => {
                self.generation = wp.generation;
                self.write_attr |= buffer::INFO2_GENERATION_GT;
            }
        }

        match wp.record_exists_action {
            RecordExistsAction::Update => (),
            RecordExistsAction::UpdateOnly => self.info_attr |= buffer::INFO3_UPDATE_ONLY,
            RecordExistsAction::Replace => self.info_attr |= buffer::INFO3_CREATE_OR_REPLACE,
            RecordExistsAction::ReplaceOnly => self.info_attr |= buffer::INFO3_REPLACE_ONLY,
            RecordExistsAction::CreateOnly => self.write_attr |= buffer::INFO2_CREATE_ONLY,
        }

        if wp.durable_delete {
            self.write_attr |= buffer::INFO2_DURABLE_DELETE
        }

        // if wp.on_locking_only {
        // 	self.txn_attr |= buffer::INFO4_MRT_ON_LOCKING_ONLY
        // }

        if wp.commit_level == CommitLevel::CommitMaster {
            self.info_attr |= buffer::INFO3_COMMIT_MASTER
        }
    }

    pub(crate) fn adjust_write<'a>(&mut self, ops: &Vec<Operation<'a>>) {
        let mut read_all_bins = false;
        let mut read_header = false;
        let mut has_read = false;

        for op in ops {
            match op.op {
                OperationType::BitRead
                | OperationType::ExpRead
                | OperationType::HllRead
                | OperationType::CdtRead
                | OperationType::Read => {
                    // _Read all bins if no bin is specified.
                    match op.bin {
                        OperationBin::All => {
                            read_all_bins = true;
                        }
                        OperationBin::None => {
                            read_header = true;
                        }
                        _ => (),
                    }
                    has_read = true;
                }
                _ => (),
            }
        }

        if has_read {
            self.read_attr |= buffer::INFO1_READ;

            if read_all_bins {
                self.read_attr |= buffer::INFO1_GET_ALL;
            } else if read_header {
                self.read_attr |= buffer::INFO1_NOBINDATA;
            }
        }
    }

    pub(crate) fn set_batch_udf(&mut self, up: &BatchUDFPolicy) {
        self.filter_expression = up.filter_expression.clone();
        self.read_attr = 0;
        self.write_attr = buffer::INFO2_WRITE;
        self.info_attr = 0;
        self.txn_attr = 0;
        self.expiration = up.expiration.into();
        self.generation = 0;
        self.has_write = true;
        self.send_key = up.send_key;

        if up.durable_delete {
            self.write_attr |= buffer::INFO2_DURABLE_DELETE;
        }

        // if up.on_locking_only {
        // 	self.txn_attr |= buffer::INFO4_MRT_ON_LOCKING_ONLY
        // }

        if up.commit_level == CommitLevel::CommitMaster {
            self.info_attr |= buffer::INFO3_COMMIT_MASTER
        }
    }

    pub(crate) fn set_batch_delete(&mut self, dp: &BatchDeletePolicy) {
        self.filter_expression = dp.filter_expression.clone();
        self.read_attr = 0;
        self.write_attr =
            buffer::INFO2_WRITE | buffer::INFO2_RESPOND_ALL_OPS | buffer::INFO2_DELETE;
        self.info_attr = 0;
        self.txn_attr = 0;
        self.expiration = 0;
        self.has_write = true;
        self.send_key = dp.send_key;

        match dp.generation_policy {
            GenerationPolicy::None => {
                self.generation = 0;
            }
            GenerationPolicy::ExpectGenEqual => {
                self.generation = dp.generation;
                self.write_attr |= buffer::INFO2_GENERATION;
            }
            GenerationPolicy::ExpectGenGreater => {
                self.generation = dp.generation;
                self.write_attr |= buffer::INFO2_GENERATION_GT;
            }
        }

        if dp.durable_delete {
            self.write_attr |= buffer::INFO2_DURABLE_DELETE;
        }

        if dp.commit_level == CommitLevel::CommitMaster {
            self.info_attr |= buffer::INFO3_COMMIT_MASTER;
        }
    }
}
