// Copyright 2015-2024 Aerospike, Inc.
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

//! Multi-Record Transaction (MRT) support.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use crate::errors::{Error, Result};
use crate::Key;
use crate::ResultCode;

static TXN_RANDOM_STATE: AtomicI64 = AtomicI64::new(0);

fn ensure_initialized() {
    // Use compare_exchange to set initial state from system time once.
    // If already set (non-zero), this is a no-op.
    let _ = TXN_RANDOM_STATE.compare_exchange(
        0,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64,
        Ordering::SeqCst,
        Ordering::Relaxed,
    );
}

fn create_txn_id() -> i64 {
    ensure_initialized();
    // xorshift64* doesn't generate zeroes.
    loop {
        let old_state = TXN_RANDOM_STATE.load(Ordering::SeqCst);
        let mut new_state = old_state;
        new_state ^= new_state >> 12;
        new_state ^= new_state << 25;
        new_state ^= new_state >> 27;

        if TXN_RANDOM_STATE
            .compare_exchange(old_state, new_state, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return new_state;
        }
    }
}

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is open and accepting commands.
    Open,
    /// Transaction has been verified (record versions checked).
    Verified,
    /// Transaction has been committed.
    Committed,
    /// Transaction has been aborted.
    Aborted,
}

/// Transaction commit status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitStatus {
    /// Commit succeeded.
    Ok,
    /// Already committed.
    AlreadyCommitted,
    /// Transaction client roll forward abandoned. Server will eventually commit.
    RollForwardAbandoned,
    /// Transaction has been rolled forward, but client close was abandoned.
    CloseAbandoned,
}

/// Transaction abort status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbortStatus {
    /// Abort succeeded.
    Ok,
    /// Already aborted.
    AlreadyAborted,
    /// Transaction client roll back abandoned. Server will eventually abort.
    RollBackAbandoned,
    /// Transaction has been rolled back, but client close was abandoned.
    CloseAbandoned,
}

/// Transaction commit error status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitErrorType {
    /// Transaction verify failed. Transaction aborted.
    VerifyFail,
    /// Transaction verify failed. Transaction aborted. Client close abandoned.
    VerifyFailCloseAbandoned,
    /// Transaction verify failed. Client abort abandoned.
    VerifyFailAbortAbandoned,
    /// Transaction client mark roll forward abandoned.
    MarkRollForwardAbandoned,
}

impl std::fmt::Display for CommitErrorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VerifyFail => write!(f, "Transaction verify failed. Transaction aborted."),
            Self::VerifyFailCloseAbandoned => write!(
                f,
                "Transaction verify failed. Transaction aborted. Transaction client close abandoned. Server will eventually close the Transaction."
            ),
            Self::VerifyFailAbortAbandoned => write!(
                f,
                "Transaction verify failed. Transaction client abort abandoned. Server will eventually abort the Transaction."
            ),
            Self::MarkRollForwardAbandoned => write!(
                f,
                "Transaction client mark roll forward abandoned. Server will eventually abort the Transaction."
            ),
        }
    }
}

/// Multi-Record Transaction.
///
/// Each command in the transaction must use the same namespace.
/// The default client transaction timeout is zero, meaning use the server
/// configuration `transaction-duration` as the timeout (default 10 seconds).
pub struct Txn {
    id: i64,
    reads: RwLock<HashMap<[u8; 20], (Key, Option<u64>)>>,
    writes: RwLock<HashMap<[u8; 20], Key>>,
    state: RwLock<TxnState>,
    namespace: RwLock<Option<String>>,
    timeout: u32,
    pub(crate) deadline: AtomicI32,
    write_in_doubt: AtomicBool,
    in_doubt: AtomicBool,
}

impl std::fmt::Debug for Txn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Txn")
            .field("id", &self.id)
            .field("state", &*self.state.read().unwrap())
            .field("namespace", &*self.namespace.read().unwrap())
            .field("timeout", &self.timeout)
            .field("deadline", &self.deadline.load(Ordering::Relaxed))
            .finish()
    }
}

impl Txn {
    /// Create a new transaction with a random transaction ID.
    pub fn new() -> Self {
        Self {
            id: create_txn_id(),
            reads: RwLock::new(HashMap::with_capacity(16)),
            writes: RwLock::new(HashMap::with_capacity(16)),
            state: RwLock::new(TxnState::Open),
            namespace: RwLock::new(None),
            timeout: 0,
            deadline: AtomicI32::new(0),
            write_in_doubt: AtomicBool::new(false),
            in_doubt: AtomicBool::new(false),
        }
    }

    /// Create a new transaction with specified read/write capacities.
    pub fn with_capacity(reads_capacity: usize, writes_capacity: usize) -> Self {
        let reads_capacity = reads_capacity.max(16);
        let writes_capacity = writes_capacity.max(16);

        Self {
            id: create_txn_id(),
            reads: RwLock::new(HashMap::with_capacity(reads_capacity)),
            writes: RwLock::new(HashMap::with_capacity(writes_capacity)),
            state: RwLock::new(TxnState::Open),
            namespace: RwLock::new(None),
            timeout: 0,
            deadline: AtomicI32::new(0),
            write_in_doubt: AtomicBool::new(false),
            in_doubt: AtomicBool::new(false),
        }
    }

    /// Return the transaction ID.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Return the transaction state.
    pub fn state(&self) -> TxnState {
        *self.state.read().unwrap()
    }

    /// Set the transaction state.
    pub fn set_state(&self, state: TxnState) {
        *self.state.write().unwrap() = state;
    }

    /// Process the results of a record read. For internal use only.
    pub fn on_read(&self, key: &Key, version: Option<u64>) {
        if let Some(ver) = version {
            let mut reads = self.reads.write().unwrap();
            reads.insert(key.digest, (key.clone(), Some(ver)));
        }
    }

    /// Get record version for a given key.
    pub fn get_read_version(&self, key: &Key) -> Option<u64> {
        let reads = self.reads.read().unwrap();
        reads.get(&key.digest).and_then(|(_, v)| *v)
    }

    /// Check if a read exists for the given key.
    pub fn read_exists_for_key(&self, key: &Key) -> bool {
        let reads = self.reads.read().unwrap();
        reads.contains_key(&key.digest)
    }

    /// Get all read keys and their versions.
    pub fn get_reads(&self) -> Vec<(Key, Option<u64>)> {
        let reads = self.reads.read().unwrap();
        reads.values().cloned().collect()
    }

    /// Process the results of a record write. For internal use only.
    pub fn on_write(&self, key: &Key, version: Option<u64>, result_code: ResultCode) {
        if version.is_some() {
            let mut reads = self.reads.write().unwrap();
            reads.insert(key.digest, (key.clone(), version));
        } else if result_code == ResultCode::Ok {
            let mut reads = self.reads.write().unwrap();
            reads.remove(&key.digest);
            let mut writes = self.writes.write().unwrap();
            writes.insert(key.digest, key.clone());
        }
    }

    /// Add key to write hash when write command is in doubt (usually caused by timeout).
    pub fn on_write_in_doubt(&self, key: &Key) {
        self.write_in_doubt.store(true, Ordering::Relaxed);
        self.writes.write().unwrap().insert(key.digest, key.clone());
        self.reads.write().unwrap().remove(&key.digest);
    }

    /// Get all write keys.
    pub fn get_writes(&self) -> Vec<Key> {
        let writes = self.writes.read().unwrap();
        writes.values().cloned().collect()
    }

    /// Check if a write exists for the given key.
    pub fn write_exists_for_key(&self, key: &Key) -> bool {
        let writes = self.writes.read().unwrap();
        writes.contains_key(&key.digest)
    }

    /// Return transaction namespace.
    pub fn namespace(&self) -> Option<String> {
        self.namespace.read().unwrap().clone()
    }

    /// Verify current transaction state and namespace for a future command.
    pub(crate) fn prepare_read(&self, ns: &str) -> Result<()> {
        self.verify_command()?;
        self.set_namespace(ns)
    }

    /// Verify that the transaction state allows future commands.
    pub fn verify_command(&self) -> Result<()> {
        if *self.state.read().unwrap() != TxnState::Open {
            return Err(Error::ClientError(
                "Issuing commands to this transaction is forbidden because it has been ended by a commit or abort".to_string(),
            ));
        }
        Ok(())
    }

    /// Set transaction namespace only if doesn't already exist.
    /// If namespace already exists, verify new namespace is the same.
    pub fn set_namespace(&self, ns: &str) -> Result<()> {
        let mut guard = self.namespace.write().unwrap();
        match &*guard {
            None => {
                *guard = Some(ns.to_string());
                Ok(())
            }
            Some(existing) if existing == ns => Ok(()),
            Some(existing) => Err(Error::ClientError(format!(
                "Namespace must be the same for all commands in the Transaction. orig: {} new: {}",
                existing, ns
            ))),
        }
    }

    /// Get transaction timeout.
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(u64::from(self.timeout))
    }

    /// Set transaction timeout in seconds.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout.as_secs() as u32;
    }

    /// Get raw timeout value in seconds (for protocol encoding).
    pub(crate) fn timeout_secs(&self) -> u32 {
        self.timeout
    }

    /// Get transaction in-doubt status.
    pub fn in_doubt(&self) -> bool {
        self.in_doubt.load(Ordering::Relaxed)
    }

    /// Set transaction in-doubt status.
    pub fn set_in_doubt(&self, in_doubt: bool) {
        self.in_doubt.store(in_doubt, Ordering::Relaxed);
    }

    /// Return whether any write in this transaction is currently in-doubt
    /// (meaning it may or may not have reached the server due to a timeout).
    pub fn write_in_doubt(&self) -> bool {
        self.write_in_doubt.load(Ordering::Relaxed)
    }

    /// Return if the MRT monitor record should be closed/deleted.
    pub fn close_monitor(&self) -> bool {
        self.deadline.load(Ordering::Relaxed) != 0 && !self.write_in_doubt.load(Ordering::Relaxed)
    }

    /// Does transaction monitor record exist.
    pub fn monitor_exists(&self) -> bool {
        self.deadline.load(Ordering::Relaxed) != 0
    }

    /// Get the deadline value (for protocol encoding).
    pub(crate) fn get_deadline(&self) -> i32 {
        self.deadline.load(Ordering::Relaxed)
    }

    /// Set the deadline value from parsed response.
    pub(crate) fn set_deadline(&self, deadline: i32) {
        self.deadline.store(deadline, Ordering::Relaxed);
    }

    /// Clear transaction. Remove all tracked keys and reset transient flags.
    pub fn clear(&self) {
        *self.namespace.write().unwrap() = None;
        self.deadline.store(0, Ordering::Relaxed);
        self.write_in_doubt.store(false, Ordering::Relaxed);
        self.in_doubt.store(false, Ordering::Relaxed);
        self.reads.write().unwrap().clear();
        self.writes.write().unwrap().clear();
    }
}

impl Default for Txn {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate the transaction monitor key.
pub(crate) fn get_txn_monitor_key(txn: &Txn) -> Option<Key> {
    txn.namespace().map(|ns| {
        Key::new(ns, "<ERO~MRT".to_string(), crate::Value::Int(txn.id())).expect("Failed to create transaction monitor key")
    })
}
