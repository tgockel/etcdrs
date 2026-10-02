#![doc = include_str!("../README.md")]

mod server;
pub use server::{ClusterState, EtcdCluster, EtcdClusterConfig, EtcdServer, EtcdServerConfig};

#[cfg(feature = "rstest")]
pub use server::fixtures::{etcd_cluster, etcd_server};
