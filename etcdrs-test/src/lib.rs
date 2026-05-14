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
        EtcdServerConfig::new_single_temporary().start().unwrap()
    }

    #[fixture]
    pub fn etcd_cluster(#[default(3)] peers: usize) -> EtcdCluster {
        let cluster = EtcdClusterConfig::with_generated_peers(peers).start().unwrap();
        // Block on a dedicated thread until a linearizable `member_list` returns `peers`
        // fully-populated members (or panic after 30s). The fixture is sync and is called
        // from inside a `#[tokio::test]` runtime, so we can't build a runtime on this thread.
        let connect = cluster.connect_string();
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
                            if r.members().len() == peers
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
                        "etcd cluster not ready after 30s; last status: {status}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
        })
        .join()
        .unwrap();
        cluster
    }
}

#[cfg(feature = "rstest")]
pub use fixtures::{etcd_cluster, etcd_server};
