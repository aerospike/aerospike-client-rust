//! Runtime shim for the Aerospike client. **Not meant to be used directly.**
//!
//! The client is written once against the names re-exported here — `spawn`,
//! `timeout`, `sleep`, `TcpStream`, `Semaphore`, `RwLock` and friends — and this
//! crate binds them to whichever runtime the `rt-tokio` or `rt-async-std`
//! feature selects. Exactly one must be enabled; the alternatives differ enough
//! that a few capabilities are runtime-specific, and the `compile_error!`s below
//! reject the combinations that cannot work (notably `tls`, which needs tokio).
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
