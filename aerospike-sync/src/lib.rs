//! A blocking Aerospike client.
//!
//! Same API as [`aerospike_core`], with the `async` taken off: every method
//! here drives the asynchronous client to completion on a runtime this crate
//! owns, so callers need no runtime of their own and no `.await`.
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

mod client;

pub use crate::client::{BatchStream, Client};
pub use aerospike_core::*;
