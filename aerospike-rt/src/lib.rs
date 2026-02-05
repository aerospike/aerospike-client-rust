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
    self, fs, io, net, spawn, sync::Mutex, sync::RwLock, sync::Semaphore, task, time, time::sleep,
    time::timeout,
};

#[cfg(feature = "rt-async-std")]
pub use std::time;
