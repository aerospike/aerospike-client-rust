//! The Aerospike client for Rust.
//!
//! This crate is a thin facade: it re-exports one of two client
//! implementations, chosen by feature, so application code names a single
//! dependency and one set of types.
//!
//! | feature | client | methods |
//! |---|---|---|
//! | `async` (default) | `aerospike-core` | `async fn`, awaited on your runtime |
//! | `sync` | `aerospike-sync` | blocking, on a runtime the client owns |
//!
//! The two are mutually exclusive — enabling both, or neither, is a compile
//! error rather than a surprise at run time. A runtime feature is also
//! required: `rt-tokio` (which `tls` needs) or `rt-async-std`.
//!
//! ```no_run
//! use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
//!
//! # async fn example() -> aerospike::Result<()> {
//! let client = Client::new(&ClientPolicy::default(), &"127.0.0.1:3000".to_string()).await?;
//! let key = as_key!("test", "demo", "key");
//!
//! client.put(&WritePolicy::default(), &key, &[as_bin!("n", 1)]).await?;
//! let record = client.get(&ReadPolicy::default(), &key, Bins::All).await?;
//! println!("{:?}", record.bins);
//! # Ok(())
//! # }
//! ```
//!
//! Start at [`Client`] for the command surface, [`ClientPolicy`] for cluster
//! and connection settings, and the `operations` and `expressions` modules for
//! server-side work on records.
#![warn(missing_docs)]

#[cfg(all(not(feature = "async"), not(feature = "sync")))]
compile_error!("Please select a client version: [async, sync]");

#[cfg(all(feature = "async", feature = "sync"))]
compile_error!("Please select only one client version");

#[cfg(all(feature = "async", not(feature = "sync")))]
pub use aerospike_core::*;

#[cfg(all(not(feature = "async"), feature = "sync"))]
pub use aerospike_sync::*;
