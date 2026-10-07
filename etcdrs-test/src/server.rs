//! Utilities for running an etcd server.

use std::{borrow::Cow, collections::HashMap, env, ffi, io, net, path, process, sync::Arc};

#[cfg(feature = "rstest")]
pub(crate) mod fixtures;

/// A single instance of an etcd server.
pub struct EtcdServer {
    config: EtcdServerConfig,
    runner: Option<EtcdRunner>,
    created_working_dir: bool,
}

impl EtcdServer {
    pub fn with_config(config: EtcdServerConfig) -> Self {
        Self {
            config,
            runner: None,
            created_working_dir: false,
        }
    }

    /// Start the etcd process.
    ///
    /// The first call creates the data directory, and fails with [`io::ErrorKind::AlreadyExists`] if it already
    /// exists. Later calls reuse it, so data persists across [`stop`][`EtcdServer::stop`].
    ///
    /// This returns once etcd is spawned, without waiting for it to bind its ports, so etcd can still exit right
    /// after, for example when one of them is taken.
    pub fn start(&mut self) -> io::Result<()> {
        if self.runner.is_some() {
            return Err(io::Error::other(
                "server is already running -- you must stop() it first",
            ));
        }

        if !self.created_working_dir {
            create_working_dir(&self.config.working_dir)?;
            self.created_working_dir = true;
        }

        let client_port = self.config.client_port;
        let peer_url = self.config.peer_url();
        let mut command = process::Command::new(get_etcd_program());
        command
            .arg("--name")
            .arg(&self.config.name.0)
            .arg("--data-dir")
            .arg(&self.config.working_dir)
            .arg("--listen-client-urls")
            .arg(format!("http://127.0.0.1:{client_port}"))
            .arg("--advertise-client-urls")
            .arg(format!("http://127.0.0.1:{client_port}"))
            .arg("--listen-peer-urls")
            .arg(&peer_url)
            .arg("--initial-advertise-peer-urls")
            .arg(&peer_url)
            .arg("--initial-cluster")
            .arg(match &self.config.initial_cluster {
                Some(initial_cluster) => initial_cluster.clone(),
                None => format!("{}={peer_url}", self.config.name.0),
            });
        if let Some(cluster_token) = self.config.cluster_token.as_ref() {
            command.arg("--initial-cluster-token").arg(cluster_token);
        }
        if let Some(state) = self.config.cluster_state {
            command.arg("--initial-cluster-state").arg(match state {
                ClusterState::New => "new",
                ClusterState::Existing => "existing",
            });
        }

        let mut etcd_process = command.spawn()?;
        if let Some(rc) = etcd_process.try_wait()? {
            return Err(io::Error::other(format!(
                "etcd immediately exited with {rc} -- check the logs for issues in startup"
            )));
        }

        self.runner = Some(EtcdRunner { proc: etcd_process });

        Ok(())
    }

    pub fn stop(&mut self) -> io::Result<bool> {
        let Some(runner) = self.runner.take() else {
            return Ok(false);
        };

        drop(runner);
        Ok(true)
    }

    /// Get a client connection string for this server.
    pub fn connect_string(&self) -> String {
        format!("http://127.0.0.1:{}", self.config.client_port)
    }
}

/// Whether a server is bootstrapping a new cluster or joining an existing one.
///
/// This corresponds to etcd's `--initial-cluster-state` flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterState {
    /// The server is part of a new cluster being bootstrapped.
    New,
    /// The server is joining an already-running cluster (e.g. via a `MemberAdd` call).
    Existing,
}

#[derive(Clone, Debug)]
pub struct EtcdServerConfig {
    name: ServerName,
    working_dir: path::PathBuf,
    client_port: u16,
    peer_port: u16,
    cluster_token: Option<String>,
    initial_cluster: Option<String>,
    cluster_state: Option<ClusterState>,
    /// While open, these keep the OS from giving out `client_port` and `peer_port` as free ports.
    _reserved_ports: Option<Arc<[socket2::Socket; 2]>>,
}

impl EtcdServerConfig {
    /// Create a configuration that uses random ports and a generated temporary directory.
    ///
    /// This is the quickest way to create an empty etcd server.
    ///
    /// On Linux, both ports stay bound, without listening, until this config, its clones and the servers built from
    /// them are all dropped. Until then, the OS does not give them out as free ports, even while etcd is stopped, and
    /// etcd can still bind them. Where the OS would not let etcd bind a port held this way, as on macOS and Windows,
    /// the ports are released before this returns, so something else can take one before etcd binds it.
    pub fn new_single_temporary() -> Self {
        let name = ServerName::generate();
        let working_dir = env::temp_dir().join(&name.0);
        // Both sockets are open at once, so the ports differ.
        let (client, client_port) = reserve_tcp_port().unwrap();
        let (peer, peer_port) = reserve_tcp_port().unwrap();
        // etcd's listeners set SO_REUSEADDR exactly where std's do, so this binds where etcd could.
        let etcd_can_bind = |port| net::TcpListener::bind((net::Ipv4Addr::LOCALHOST, port)).is_ok();
        let reserved_ports = (etcd_can_bind(client_port) && etcd_can_bind(peer_port)).then(|| Arc::new([client, peer]));
        Self {
            name,
            working_dir,
            client_port,
            peer_port,
            cluster_token: None,
            initial_cluster: None,
            cluster_state: None,
            _reserved_ports: reserved_ports,
        }
    }

    /// The peer URL this server will advertise.
    pub fn peer_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.peer_port)
    }

    /// Build a server, but do not start it.
    pub fn build(self) -> EtcdServer {
        EtcdServer::with_config(self)
    }

    /// Create a server through [`build`][`EtcdServerConfig::build`], then [`start`][`EtcdServer::start`] it.
    pub fn start(self) -> io::Result<EtcdServer> {
        let mut out = self.build();
        out.start()?;
        Ok(out)
    }
}

pub struct EtcdCluster {
    cluster_token: String,
    servers: HashMap<ServerName, EtcdServer>,
}

impl EtcdCluster {
    pub fn with_config(config: EtcdClusterConfig) -> Self {
        let servers = config
            .configs
            .into_iter()
            .map(|(name, config)| (name, config.build()))
            .collect();
        Self {
            cluster_token: config.cluster_token,
            servers,
        }
    }

    pub fn start(&mut self) -> io::Result<()> {
        for server in self.servers.values_mut() {
            server.start()?;
        }
        Ok(())
    }

    pub fn stop(&mut self) -> io::Result<()> {
        for server in self.servers.values_mut() {
            server.stop()?;
        }
        Ok(())
    }

    pub fn connect_string(&self) -> String {
        assert!(!self.servers.is_empty(), "Cluster {:?} is empty", self.cluster_token);
        self.servers
            .values()
            .map(|s| s.connect_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Create a config for a new peer that will join this cluster.
    ///
    /// The returned config shares the cluster token, has fresh ports and a fresh data directory,
    /// and is set up to join an existing cluster (`--initial-cluster-state=existing`). Its
    /// `--initial-cluster` argument lists the existing peers plus this new one — the caller is
    /// responsible for first announcing the new peer to the cluster (via
    /// `Client::member_add` with the value of [`EtcdServerConfig::peer_url`]) and then calling
    /// [`start`][`EtcdServerConfig::start`] on the returned config.
    pub fn new_joining_peer(&self) -> EtcdServerConfig {
        let mut config = EtcdServerConfig::new_single_temporary();
        config.cluster_token = Some(self.cluster_token.clone());
        config.cluster_state = Some(ClusterState::Existing);

        let mut entries: Vec<String> = self
            .servers
            .values()
            .map(|s| format!("{}={}", s.config.name.0, s.config.peer_url()))
            .collect();
        entries.push(format!("{}={}", config.name.0, config.peer_url()));
        config.initial_cluster = Some(entries.join(","));

        config
    }
}

#[derive(Clone, Debug)]
pub struct EtcdClusterConfig {
    cluster_token: String,
    configs: HashMap<ServerName, EtcdServerConfig>,
}

impl EtcdClusterConfig {
    pub fn with_generated_peers(count: usize) -> Self {
        Self::with_peers(count, EtcdServerConfig::new_single_temporary)
    }

    fn with_peers(count: usize, mut new_peer: impl FnMut() -> EtcdServerConfig) -> Self {
        let cluster_token = format!("cluster-{}", get_random_name(4));
        let mut configs = HashMap::new();
        while configs.len() < count {
            let mut config = new_peer();
            config.cluster_token = Some(cluster_token.clone());
            configs.insert(config.name.clone(), config);
        }
        let initial_cluster_string = configs
            .values()
            .map(|config| format!("{}={}", config.name.0, config.peer_url()))
            .collect::<Vec<_>>()
            .join(",");

        for config in configs.values_mut() {
            config.initial_cluster = Some(initial_cluster_string.clone());
        }

        Self { cluster_token, configs }
    }

    pub fn build(self) -> EtcdCluster {
        EtcdCluster::with_config(self)
    }

    pub fn start(self) -> io::Result<EtcdCluster> {
        let mut cluster = self.build();
        cluster.start()?;
        Ok(cluster)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ServerName(String);

impl ServerName {
    pub fn generate() -> Self {
        Self(format!("etcd-srvr-{}", get_random_name(12)))
    }
}

fn get_random_name(length: usize) -> String {
    use rand::{RngExt, distr::slice::Choose};

    let dist = Choose::new(b"abcdefghijklmnopqrstuvwxyz").unwrap();
    rand::rng()
        .sample_iter(&dist)
        .take(length)
        .map(|c| char::from(*c))
        .collect()
}

/// Bind a socket to an OS-assigned TCP port on 127.0.0.1, without listening on it.
fn reserve_tcp_port() -> io::Result<(socket2::Socket, u16)> {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
    socket.bind(&net::SocketAddr::from((net::Ipv4Addr::LOCALHOST, 0)).into())?;
    // Set only after the bind, so the OS picks a port that no other socket holds, whatever SO_REUSEADDR means on this
    // OS. On Linux, a listener that also sets it, as etcd's does, can then bind the port while this socket holds it.
    socket.set_reuse_address(true)?;
    let port = socket.local_addr()?.as_socket().unwrap().port();
    Ok((socket, port))
}

/// Get the path to `etcd`.
///
/// Determining the program happens in the following order:
///
/// * If `ETCD` environment variable is set, it will be used.
/// * Use `etcd-bin-vendored` to find the path of the binary (this should work in almost every case, but there might be
///   some platforms that `etcd` supports but does not offer pre-built binaries for).
/// * Just use `"etcd"` and hope it is in the user's `PATH`.
fn get_etcd_program() -> Cow<'static, ffi::OsStr> {
    env::var_os("ETCD")
        .and_then(|name| if name.is_empty() { None } else { Some(Cow::Owned(name)) })
        .or_else(|| {
            etcd_bin_vendored::etcd_bin_path()
                .ok()
                .map(|path| Cow::Borrowed(path.as_os_str()))
        })
        .unwrap_or(Cow::Borrowed(ffi::OsStr::new("etcd")))
}

fn keep_test_dir() -> bool {
    match std::env::var("ETCDRS_KEEP_TEST_DIR").as_deref() {
        Err(_) | Ok("0") | Ok("false") => false,
        Ok("1") | Ok("true") => true,
        Ok(other) => {
            eprintln!(
                "warning: ETCDRS_KEEP_TEST_DIR={other:?} is not recognized; \
                 use '1' or 'true' to keep test directories, '0' or 'false' to remove them"
            );
            false
        }
    }
}

fn create_working_dir(path: &path::Path) -> io::Result<()> {
    // etcd creates its data dir 0700, and warns on every start about one that is not.
    #[cfg(unix)]
    let created = {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)
    };
    #[cfg(not(unix))]
    let created = std::fs::create_dir(path);

    created.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("failed to create etcd data dir {path:?}: {error}"),
        )
    })
}

struct EtcdRunner {
    proc: process::Child,
}

impl Drop for EtcdRunner {
    fn drop(&mut self) {
        if let Err(error) = self.proc.kill() {
            eprintln!("Failed to kill etcd process: {error}");
        }

        if let Err(error) = self.proc.wait() {
            eprintln!("Failed to wait for etcd process: {error}");
        }
    }
}

impl Drop for EtcdServer {
    fn drop(&mut self) {
        drop(self.runner.take());

        if self.created_working_dir
            && !std::thread::panicking()
            && !keep_test_dir()
            && let Err(error) = std::fs::remove_dir_all(&self.config.working_dir)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!("Failed to remove etcd data dir {:?}: {error}", self.config.working_dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_start_refuses_an_existing_data_dir() {
        let config = EtcdServerConfig::new_single_temporary();
        let dir = config.working_dir.clone();
        std::fs::create_dir(&dir).unwrap();
        let leftover = dir.join("leftover");
        std::fs::write(&leftover, "").unwrap();

        // `EtcdServerConfig::start` drops the server when its start fails, so this also runs its `Drop`.
        let Err(error) = config.start() else {
            panic!("start() should refuse the existing data dir {dir:?}");
        };
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");
        assert!(
            leftover.exists(),
            "the refused server removed a data dir it did not create"
        );

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn generated_peers_skip_a_repeated_name() {
        let repeated = EtcdServerConfig::new_single_temporary();
        let mut drawn = [repeated.clone(), repeated].into_iter();
        let cluster = EtcdClusterConfig::with_peers(2, || {
            drawn.next().unwrap_or_else(EtcdServerConfig::new_single_temporary)
        });

        let mut expected: Vec<_> = cluster
            .configs
            .values()
            .map(|config| format!("{}={}", config.name.0, config.peer_url()))
            .collect();
        expected.sort_unstable();
        assert_eq!(expected.len(), 2, "{expected:?}");
        for config in cluster.configs.values() {
            let mut members: Vec<_> = config.initial_cluster.as_deref().unwrap().split(',').collect();
            members.sort_unstable();
            assert_eq!(members, expected);
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn temporary_ports_differ_and_stay_reserved_for_etcd() {
        let config = EtcdServerConfig::new_single_temporary();
        assert_ne!(config.client_port, config.peer_port);
        for port in [config.client_port, config.peer_port] {
            let address = net::SocketAddr::from((net::Ipv4Addr::LOCALHOST, port));
            let plain = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
            let error = plain.bind(&address.into()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::AddrInUse, "port {port}: {error}");
            net::TcpListener::bind(address).unwrap();
        }
    }

    #[test]
    fn public_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EtcdServerConfig>();
        assert_send_sync::<EtcdServer>();
        assert_send_sync::<EtcdClusterConfig>();
        assert_send_sync::<EtcdCluster>();
    }
}
