#![doc = include_str!("../README.md")]

mod server;
pub use server::{EtcdCluster, EtcdClusterConfig, EtcdServer, EtcdServerConfig};

#[cfg(feature = "rstest")]
mod fixtures {
    use rstest::fixture;

    use crate::*;

    #[fixture]
    pub fn etcd_server() -> EtcdServer {
        EtcdServerConfig::new_single_temporary().start().unwrap()
    }

    #[fixture]
    pub fn etcd_cluster(#[default(3)] peers: usize) -> EtcdCluster {
        EtcdClusterConfig::with_generated_peers(peers).start().unwrap()
    }
}

#[cfg(feature = "rstest")]
pub use fixtures::{etcd_cluster, etcd_server};
