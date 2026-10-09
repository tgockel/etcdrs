use std::time::{Duration, Instant};

use rstest::fixture;

use super::*;

/// How many times a fixture starts etcd before it gives up.
const ATTEMPTS: usize = 3;

#[fixture]
pub fn etcd_server() -> EtcdServer {
    start_ready(
        || EtcdServerConfig::new_single_temporary().start().unwrap(),
        |server| vec![server],
    )
}

#[fixture]
pub fn etcd_cluster(#[default(3)] peers: usize) -> EtcdCluster {
    start_ready(
        || EtcdClusterConfig::with_generated_peers(peers).start().unwrap(),
        |cluster| cluster.servers.values_mut().collect(),
    )
}

/// Call `start` until the servers it started are ready, starting over when one of their etcd processes exits before
/// they are.
fn start_ready<T>(mut start: impl FnMut() -> T, servers_of: impl Fn(&mut T) -> Vec<&mut EtcdServer>) -> T {
    for attempt in 1..=ATTEMPTS {
        let mut started = start();
        let servers = servers_of(&mut started);
        // The fixtures are called from inside a `#[tokio::test]` runtime, so their own runtime needs another thread.
        let ready = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(wait_until_ready(servers))
                })
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        });
        let Err(exit) = ready else {
            return started;
        };
        // Panic while `started` is alive, so its `Drop` keeps the data directories.
        assert!(
            attempt < ATTEMPTS,
            "etcd fixture attempt {attempt} of {ATTEMPTS} failed: {exit}"
        );
        eprintln!("etcd fixture attempt {attempt} of {ATTEMPTS} failed, starting over on new ports: {exit}");
    }
    unreachable!()
}

/// Poll until a linearizable `member_list` names exactly `servers`, each with a client URL. Return an error as soon as
/// one of their etcd processes exits, and panic after 30s.
async fn wait_until_ready(mut servers: Vec<&mut EtcdServer>) -> Result<(), String> {
    let mut expected: Vec<_> = servers.iter().map(|server| server.config.name.0.clone()).collect();
    expected.sort_unstable();
    let connect = servers
        .iter()
        .map(|server| server.connect_string())
        .collect::<Vec<_>>()
        .join(",");
    let client = etcdrs::Client::builder()
        .connection_string(&connect)
        .unwrap()
        .retry_never()
        .build()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // The client has no timeouts, and a port held by something other than etcd may never answer.
        let status = match tokio::time::timeout(Duration::from_secs(1), client.member_list().linearizable()).await {
            Ok(Ok(r)) => {
                let mut names: Vec<_> = r.members().iter().map(|m| m.name()).collect();
                names.sort_unstable();
                if names == expected && r.members().iter().all(|m| !m.client_urls().is_empty()) {
                    return Ok(());
                }
                format!("expected members {expected:?}, got {:?}", r.members())
            }
            Ok(Err(e)) => format!("member_list error: {e:?}"),
            Err(_) => "member_list timed out after 1s".to_owned(),
        };
        for server in &mut servers {
            if let Some(exit) = server.runner.as_mut().unwrap().proc.try_wait().unwrap() {
                let config = &server.config;
                return Err(format!(
                    "{} exited with {exit} (client port {}, peer port {}); {}",
                    config.name.0,
                    config.client_port,
                    config.peer_port,
                    config.log_tail()
                ));
            }
        }
        assert!(
            Instant::now() < deadline,
            "etcd not ready after 30s; last status: {status}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use super::*;

    /// A port held by a listener that never accepts, so etcd cannot bind it.
    fn taken_port() -> (net::TcpListener, u16) {
        let listener = net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    #[test]
    fn fixture_skips_another_server_on_its_client_port() {
        let other = etcd_server();
        let mut first = Some(EtcdServerConfig {
            client_port: other.config.client_port,
            ..EtcdServerConfig::new_single_temporary()
        });

        let server = start_ready(
            || {
                first
                    .take()
                    .unwrap_or_else(EtcdServerConfig::new_single_temporary)
                    .start()
                    .unwrap()
            },
            |server| vec![server],
        );
        assert_ne!(server.config.client_port, other.config.client_port);
    }

    #[test]
    fn fixture_starts_over_when_its_client_port_never_answers() {
        let (_listener, taken) = taken_port();
        let mut first = Some(EtcdServerConfig {
            client_port: taken,
            ..EtcdServerConfig::new_single_temporary()
        });

        let server = start_ready(
            || {
                first
                    .take()
                    .unwrap_or_else(EtcdServerConfig::new_single_temporary)
                    .start()
                    .unwrap()
            },
            |server| vec![server],
        );
        assert_ne!(server.config.client_port, taken);
    }

    #[test]
    fn fixture_rebuilds_a_cluster_whose_member_exits() {
        let (_listener, taken) = taken_port();
        let mut drawn = Some(EtcdServerConfig {
            peer_port: taken,
            ..EtcdServerConfig::new_single_temporary()
        });
        let colliding = EtcdClusterConfig::with_peers(3, || {
            drawn.take().unwrap_or_else(EtcdServerConfig::new_single_temporary)
        });
        let colliding_token = colliding.cluster_token.clone();
        let mut first = Some(colliding);

        let cluster = start_ready(
            || {
                first
                    .take()
                    .unwrap_or_else(|| EtcdClusterConfig::with_generated_peers(3))
                    .start()
                    .unwrap()
            },
            |cluster| cluster.servers.values_mut().collect(),
        );
        assert_ne!(cluster.cluster_token, colliding_token);
        assert!(cluster.servers.values().all(|server| server.config.peer_port != taken));
    }

    #[test]
    fn fixture_gives_up_after_three_exits() {
        let (_listener, taken) = taken_port();
        let mut attempts = Vec::new();

        let Err(panic) = catch_unwind(AssertUnwindSafe(|| {
            start_ready(
                || {
                    let config = EtcdServerConfig {
                        peer_port: taken,
                        ..EtcdServerConfig::new_single_temporary()
                    };
                    attempts.push(config.clone());
                    config.start().unwrap()
                },
                |server| vec![server],
            )
        })) else {
            panic!("start_ready() should give up when etcd exits on every attempt");
        };
        let message = panic.downcast_ref::<String>().unwrap();
        assert_eq!(attempts.len(), ATTEMPTS, "{message}");
        let last = attempts.last().unwrap();
        assert!(message.contains(&format!("{} exited with", last.name.0)), "{message}");
        assert!(
            message.contains(&format!("client port {}, peer port {taken}", last.client_port)),
            "{message}"
        );
        assert!(message.contains(&format!("{:?}", last.log_path())), "{message}");
        assert!(message.contains(&format!("127.0.0.1:{taken}: bind:")), "{message}");

        let dir = &last.working_dir;
        assert!(dir.exists(), "the last attempt's data dir {dir:?} should be kept");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn restarted_server_appends_to_its_log() {
        let mut server = etcd_server();
        server.stop().unwrap();
        server.start().unwrap();
        wait_until_ready(vec![&mut server]).await.unwrap();

        let log = std::fs::read_to_string(server.config.log_path()).unwrap();
        assert_eq!(log.matches(r#""msg":"Running: ""#).count(), 2, "{log}");
    }

    #[tokio::test]
    async fn single_server_advertises_its_peer_url() {
        let server = etcd_server();
        let client = etcdrs::Client::new(&server.connect_string()).unwrap();
        let listed = client.member_list().await.unwrap();
        let [member] = listed.members() else {
            panic!("expected one member, got {:?}", listed.members());
        };
        assert_eq!(member.peer_urls(), [server.config.peer_url()]);
    }
}
