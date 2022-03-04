#[cfg(not(any(feature = "rt-tokio", feature = "rt-async-std")))]
compile_error!("Please select a runtime from ['rt-tokio', 'rt-async-std']");

#[cfg(any(all(feature = "rt-async-std", feature = "rt-tokio")))]
compile_error!("Please select only one runtime");

#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
pub use async_std::{
    self, fs, future::timeout, io, net, sync::RwLock, task, task::sleep, task::spawn,
};
#[cfg(all(any(feature = "rt-tokio"), not(feature = "rt-async-std")))]
pub use tokio::{self, fs, io, net, spawn, sync::RwLock, task, time, time::sleep, time::timeout};

#[cfg(all(any(feature = "rt-async-std"), not(feature = "rt-tokio")))]
pub use std::time;
