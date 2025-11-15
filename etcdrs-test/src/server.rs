//! Utilities for running an etcd server.

use std::{borrow::Cow, collections::HashMap, env, ffi, io, net, path, process};

/// A single instance of an etcd server.
pub struct EtcdServer {
    config: EtcdServerConfig,
    runner: Option<EtcdRunner>,
}

impl EtcdServer {
    pub fn with_config(config: EtcdServerConfig) -> Self {
        Self { config, runner: None }
    }

    pub fn start(&mut self) -> io::Result<()> {
        if self.runner.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "server is already running -- you must stop() it first",
            ));
        }

        let client_port = self.config.client_port;
        let peer_port = self.config.peer_port;
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
            .arg(format!("http://127.0.0.1:{peer_port}"));
        if let Some(cluster_token) = self.config.cluster_token.as_ref() {
            command.arg("--initial-cluster-token").arg(cluster_token);
        }
        if let Some(initial_cluster) = self.config.initial_cluster.as_ref() {
            command.arg("--initial-cluster").arg(initial_cluster);
            // if we're part of a cluster, advertise our peer URL
            command
                .arg("--initial-advertise-peer-urls")
                .arg(format!("http://127.0.0.1:{peer_port}"));
        }

        let mut etcd_process = command.spawn()?;
        if let Some(rc) = etcd_process.try_wait()? {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("etcd immediately exited with {rc} -- check the logs for issues in startup"),
            ));
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

#[derive(Clone, Debug)]
pub struct EtcdServerConfig {
    name: ServerName,
    working_dir: path::PathBuf,
    client_port: u16,
    peer_port: u16,
    cluster_token: Option<String>,
    initial_cluster: Option<String>,
}

impl EtcdServerConfig {
    /// Create a configuration that uses random ports and a generated temporary directory.
    ///
    /// This is the quickest way to create an empty etcd server.
    pub fn new_single_temporary() -> Self {
        let name = ServerName::generate();
        let working_dir = env::temp_dir().join(&name.0);
        Self {
            name,
            working_dir,
            client_port: get_random_unused_tcp_port().unwrap(),
            peer_port: get_random_unused_tcp_port().unwrap(),
            cluster_token: None,
            initial_cluster: None,
        }
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
        let Some(server) = self.servers.values().next() else {
            panic!("Cluster {:?} is empty", self.cluster_token);
        };
        // TODO: more than one server
        server.connect_string()
    }
}

#[derive(Clone, Debug)]
pub struct EtcdClusterConfig {
    cluster_token: String,
    configs: HashMap<ServerName, EtcdServerConfig>,
}

impl EtcdClusterConfig {
    pub fn with_generated_peers(count: usize) -> Self {
        let cluster_token = format!("cluster-{}", get_random_name(4));
        let mut configs: Vec<_> = (0..count)
            .into_iter()
            .map(|_| {
                let mut config = EtcdServerConfig::new_single_temporary();
                config.cluster_token = Some(cluster_token.clone());
                config
            })
            .collect();
        let initial_cluster_string = configs
            .iter()
            .map(|config| format!("{}=http://127.0.0.1:{}", config.name.0, config.peer_port))
            .collect::<Vec<_>>()
            .join(",");

        for config in configs.iter_mut() {
            config.initial_cluster = Some(initial_cluster_string.clone());
        }

        Self {
            cluster_token,
            configs: configs
                .into_iter()
                .map(|config| (config.name.clone(), config))
                .collect(),
        }
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
        Self(format!("etcd-srvr-{}", get_random_name(4)))
    }
}

fn get_random_name(length: usize) -> String {
    use rand::{distributions::Slice, Rng};

    let dist = Slice::new(b"abcdefghijklmnopqrstuvwxyz").unwrap();
    rand::thread_rng()
        .sample_iter(&dist)
        .take(length)
        .map(|c| char::from(*c))
        .collect()
}

/// Get an OS-assigned random TCP port.
fn get_random_unused_tcp_port() -> io::Result<u16> {
    let listener = net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr().map(|a| a.port())
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
