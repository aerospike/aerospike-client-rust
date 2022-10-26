#[cfg(all(not(feature = "async"), not(feature = "sync")))]
compile_error!("Please select a client version: [async, sync]");

#[cfg(any(all(feature = "async", feature = "sync")))]
compile_error!("Please select only one client version");

#[cfg(any(all(feature = "async", not(feature = "sync"))))]
pub use aerospike_core::*;

#[cfg(any(all(not(feature = "async"), feature = "sync")))]
pub use aerospike_sync::*;
