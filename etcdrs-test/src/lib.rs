#![doc = include_str!("../README.md")]

mod server;
pub use server::{ClusterState, EtcdCluster, EtcdClusterConfig, EtcdServer, EtcdServerConfig};

#[cfg(feature = "rstest")]
mod fixtures {
    use std::time::{Duration, Instant};

    use rstest::fixture;

    use crate::*;

    #[fixture]
    pub fn etcd_server() -> EtcdServer {
        let server = EtcdServerConfig::new_single_temporary().start().unwrap();
        wait_until_ready(&server.connect_string(), 1);
        server
    }

    #[fixture]
    pub fn etcd_cluster(#[default(3)] peers: usize) -> EtcdCluster {
        let cluster = EtcdClusterConfig::with_generated_peers(peers).start().unwrap();
        wait_until_ready(&cluster.connect_string(), peers);
        cluster
    }

    /// Block until a linearizable `member_list` against `connect_string` returns
    /// `expected_peers` fully-populated members (or panic after 30s).
    ///
    /// Runs on a dedicated thread with its own current-thread tokio runtime: the
    /// fixture is sync and is called from inside a `#[tokio::test]` runtime, so
    /// we can't build a runtime on the calling thread.
    fn wait_until_ready(connect_string: &str, expected_peers: usize) {
        let connect = connect_string.to_owned();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let client = etcdrs::Client::builder()
                    .connection_string(&connect)
                    .unwrap()
                    .retry_never()
                    .build()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let status = match client.member_list().linearizable().await {
                        Ok(r)
                            if r.members().len() == expected_peers
                                && r.members()
                                    .iter()
                                    .all(|m| !m.name().is_empty() && !m.client_urls().is_empty()) =>
                        {
                            return;
                        }
                        Ok(r) => format!("incomplete members: {:?}", r.members()),
                        Err(e) => format!("member_list error: {e:?}"),
                    };
                    assert!(
                        Instant::now() < deadline,
                        "etcd not ready after 30s; last status: {status}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        })
        .join()
        .unwrap();
    }
}

#[cfg(feature = "rstest")]
pub use fixtures::{etcd_cluster, etcd_server};
