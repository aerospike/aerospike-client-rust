//! A blocking Aerospike client.
//!
//! Same API as [`aerospike_core`], with the `async` taken off: every method
//! here drives the asynchronous client to completion, so callers need no
//! runtime of their own and no `.await`.
//!
//! The runtime underneath follows the `rt-tokio` / `rt-async-std` feature.
//! With Tokio this crate owns a dedicated runtime and may be called from
//! inside a caller's Tokio runtime as well as from plain threads. With
//! async-std it blocks on async-std's global executor, which must not be
//! done from inside an async-std task. TLS requires `rt-tokio`.
//!
//! ```no_run
//! use aerospike_sync::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
//!
//! # fn main() -> aerospike_sync::Result<()> {
//! let client = Client::new(&ClientPolicy::default(), &"127.0.0.1:3000".to_string())?;
//! let key = as_key!("test", "demo", "key");
//!
//! client.put(&WritePolicy::default(), &key, &[as_bin!("n", 1)])?;
//! let record = client.get(&ReadPolicy::default(), &key, Bins::All)?;
//! println!("{:?}", record.bins);
//! # Ok(())
//! # }
//! ```
//!
//! Everything other than [`Client`] — policies, values, operations,
//! expressions, errors — is re-exported from [`aerospike_core`] unchanged, so
//! the two clients share one set of types and one set of docs.
//!
//! Select this client through the facade crate's `sync` feature; `async` and
//! `sync` are mutually exclusive there.
#![warn(missing_docs)]
// The blocking wrappers mirror the async client's signatures argument for argument.
#![allow(clippy::too_many_arguments)]
// `docsrs` activates the nightly `doc_cfg` feature during docs.rs builds,
// configured via `[package.metadata.docs.rs]` in `Cargo.toml`.
// This automatically adds feature badges to all `#[cfg(feature = "...")]` items,
// so manual `#[doc(cfg(...))]` attributes aren't needed (see `aerospike-core/src/lib.rs`).
#![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod query_handle;
mod blocking_task;

pub use crate::client::Client;
pub use crate::query_handle::QueryHandle;
pub use crate::blocking_task::Task;
pub use aerospike_core::*;
