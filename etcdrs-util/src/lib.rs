#![doc = include_str!("../README.md")]

/// Re-export the `etcdrs` crate as itself.
pub use etcdrs;

#[cfg(feature = "cache")]
pub mod cache;
#[cfg(feature = "lease-pool")]
pub mod lease_pool;
