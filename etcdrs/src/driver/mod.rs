#![doc = include_str!("README.md")]

mod auth;
mod cluster;
mod kv;
mod lease;
mod watch;

pub use auth::AuthDriver;
pub use cluster::ClusterDriver;
pub use kv::{KvDriver, ListView};
pub use lease::LeaseDriver;
pub use watch::WatchDriver;

/// Driver for all public etcd operation families.
pub trait Driver: KvDriver + LeaseDriver + WatchDriver + AuthDriver + ClusterDriver {}

impl<T> Driver for T where T: KvDriver + LeaseDriver + WatchDriver + AuthDriver + ClusterDriver {}
