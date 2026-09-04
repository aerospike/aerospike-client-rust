#[cfg(all(not(feature = "async"), not(feature = "sync")))]
compile_error!("Please select a client version: [async, sync]");

#[cfg(all(feature = "async", feature = "sync"))]
compile_error!(
    "Both the `async` and `sync` clients are enabled. `async` is a default feature, so \
     `--features sync` alone turns both on; build the sync client with \
     `--no-default-features --features sync,serialization,rt-tokio` (or rt-async-std)."
);

#[cfg(all(feature = "async", not(feature = "sync")))]
pub use aerospike_core::*;

#[cfg(all(not(feature = "async"), feature = "sync"))]
pub use aerospike_sync::*;
