#[cfg(not(any(feature = "rt-tokio", feature = "rt-async-std")))]
compile_error!("Please select a runtime from ['rt-tokio', 'rt-async-std']");

#[cfg(all(feature = "tls", feature = "rt-async-std"))]
compile_error!("TLS support is only available for the tokio runtime ['rt-tokio']");

#[cfg(any(all(feature = "rt-async-std", feature = "rt-tokio")))]
compile_error!("Please select only one runtime");

#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
pub use async_lock::Semaphore;
#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
pub use async_std::{
    self, fs, future::timeout, io, net, sync::Mutex, sync::RwLock, task, task::sleep, task::spawn,
};
#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
pub use tokio::{
    self, fs, io, net, runtime, spawn, sync::Mutex, sync::RwLock, sync::Semaphore, task, time,
    time::sleep, time::timeout,
};

#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
pub use std::time;
