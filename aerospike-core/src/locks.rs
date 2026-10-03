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

//! Poison-tolerant access to the crate's `std::sync` locks.
//!
//! Every `Mutex`/`RwLock` in this crate guards plain data — counters, maps,
//! cursors, a path — with no invariant that a panic in the middle of an
//! update could leave half-kept. A task that panics while holding one of
//! them must therefore not wedge every later user of that lock: these
//! helpers take the guard back out of the `PoisonError` instead of
//! unwrapping it, so a poisoned metrics histogram keeps counting and a
//! poisoned transaction keeps tracking its keys.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Locks `m`, recovering the guard if a previous holder panicked.
pub fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Takes a read guard on `l`, recovering it if a previous holder panicked.
pub fn read<T: ?Sized>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(PoisonError::into_inner)
}

/// Takes a write guard on `l`, recovering it if a previous holder panicked.
pub fn write<T: ?Sized>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{lock, read, write};
    use std::sync::{Arc, Mutex, RwLock};

    #[test]
    fn a_poisoned_mutex_is_still_usable() {
        let m = Arc::new(Mutex::new(1));
        let poisoner = Arc::clone(&m);
        let _ = std::thread::spawn(move || {
            let _held = poisoner.lock().unwrap();
            panic!("poison the mutex");
        })
        .join();
        assert!(m.is_poisoned());

        *lock(&m) += 1;
        assert_eq!(*lock(&m), 2);
    }

    #[test]
    fn a_poisoned_rwlock_is_still_usable() {
        let l = Arc::new(RwLock::new(String::from("a")));
        let poisoner = Arc::clone(&l);
        let _ = std::thread::spawn(move || {
            let _held = poisoner.write().unwrap();
            panic!("poison the rwlock");
        })
        .join();
        assert!(l.is_poisoned());

        write(&l).push('b');
        assert_eq!(*read(&l), "ab");
    }
}
