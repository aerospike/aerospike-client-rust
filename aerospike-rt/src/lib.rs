// Copyright 2015-2026 Aerospike, Inc.
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

//! Runtime shim for the Aerospike client. **Not meant to be used directly.**
//!
//! The client is written once against the names re-exported here — `spawn`,
//! `timeout`, `sleep`, `TcpStream`, `Semaphore`, `RwLock` and friends — and this
//! crate binds them to whichever runtime the `rt-tokio` or `rt-async-std`
//! feature selects. Exactly one must be enabled (`rt-tokio` is the default, so
//! the crate builds standalone; the client crates turn the default off and
//! choose); the alternatives differ enough that a few capabilities are
//! runtime-specific, and the `compile_error!`s below reject the combinations
//! that cannot work (notably `tls`, which needs tokio).
//!
//! Nothing here is a stable API: items appear and disappear as the client's
//! needs change, and versions move in lock-step with `aerospike-core`.
#![warn(missing_docs)]

#[cfg(not(any(feature = "rt-tokio", feature = "rt-async-std")))]
compile_error!("Please select a runtime from ['rt-tokio', 'rt-async-std']");

#[cfg(all(feature = "tls", feature = "rt-async-std"))]
compile_error!("TLS support is only available for the tokio runtime ['rt-tokio']");

#[cfg(all(feature = "rt-async-std", feature = "rt-tokio"))]
compile_error!("Please select only one runtime");

#[cfg(feature = "rt-async-std")]
pub use async_lock::Semaphore;
#[cfg(feature = "rt-async-std")]
pub use async_std::{
    self, fs, future::timeout, io, net, sync::Mutex, sync::RwLock, task, task::sleep, task::spawn,
};
#[cfg(feature = "rt-tokio")]
pub use tokio::{
    self, fs, io, net, runtime, spawn, sync::Mutex, sync::RwLock, sync::Semaphore, task, time,
    time::sleep, time::timeout,
};

#[cfg(feature = "rt-async-std")]
pub use std::time;

/// Resolve `host:port` on the selected runtime without blocking a worker
/// thread on the system resolver.
pub async fn lookup_host(host: &str, port: u16) -> std::io::Result<Vec<std::net::SocketAddr>> {
    #[cfg(feature = "rt-tokio")]
    {
        Ok(tokio::net::lookup_host((host, port)).await?.collect())
    }
    #[cfg(feature = "rt-async-std")]
    {
        use async_std::net::ToSocketAddrs;
        Ok((host, port).to_socket_addrs().await?.collect())
    }
}

/// Cancel a spawned task without waiting for it to finish.
pub fn abort<T: Send + 'static>(handle: task::JoinHandle<T>) {
    #[cfg(feature = "rt-tokio")]
    handle.abort();
    #[cfg(feature = "rt-async-std")]
    drop(spawn(async move {
        handle.cancel().await;
    }));
}
