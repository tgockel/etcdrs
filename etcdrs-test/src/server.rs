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
    /// The first call creates the server's directory, which holds etcd's data and its log, and fails with
    /// [`io::ErrorKind::AlreadyExists`] if it already exists. Later calls reuse it, so data persists across
    /// [`stop`][`EtcdServer::stop`].
    ///
    /// This returns once etcd is spawned, without waiting for it to bind its ports, so etcd can still exit right
    /// after, for example when one of them is taken.
    ///
    /// etcd is killed when this server is stopped or dropped. On Unix and Windows, it is also killed when this process
    /// exits, even without unwinding, except on Windows when this process exits after this call spawns etcd but before
    /// it puts etcd in a job object.
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
            // etcd warns on every start about any file in its data dir that is not its own, so the log stays outside.
            .arg("--data-dir")
            .arg(self.config.working_dir.join("data"))
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

        let log_path = self.config.log_path();
        let log = std::fs::File::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| io::Error::new(error.kind(), format!("failed to open etcd log {log_path:?}: {error}")))?;
        command.stdout(log.try_clone()?).stderr(log);
        let mut runner = EtcdRunner::spawn(command)?;
        if let Some(rc) = runner.proc.try_wait()? {
            return Err(io::Error::other(format!(
                "etcd immediately exited with {rc}; {}",
                self.config.log_tail()
            )));
        }

        self.runner = Some(runner);

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

    fn log_path(&self) -> path::PathBuf {
        self.working_dir.join("etcd.log")
    }

    /// Name etcd's log and quote its last JSON record, or its last line if it has none, as the log is usually removed
    /// before anyone reads an error naming it.
    fn log_tail(&self) -> String {
        let log = self.log_path();
        let text = std::fs::read(&log).unwrap_or_default();
        let text = String::from_utf8_lossy(&text);
        // Go prints a panic and its stack trace after etcd's record of the panic.
        let line = text
            .lines()
            .rfind(|line| line.starts_with('{'))
            .or_else(|| text.lines().last())
            .unwrap_or_default();
        format!("last record in {log:?}: {line}")
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
    // 0700 keeps etcd's log private in the shared temp dir.
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
            format!("failed to create etcd server dir {path:?}: {error}"),
        )
    })
}

struct EtcdRunner {
    proc: process::Child,
}

impl EtcdRunner {
    /// Spawn etcd so that it is killed when this process exits.
    ///
    /// `PR_SET_PDEATHSIG` fires when the thread that forked etcd exits, not this process, so every etcd is forked by
    /// one thread that never exits.
    #[cfg(target_os = "linux")]
    fn spawn(mut command: process::Command) -> io::Result<Self> {
        use std::{
            os::unix::process::CommandExt,
            sync::{Mutex, mpsc},
        };

        type Spawn = (process::Command, mpsc::Sender<io::Result<process::Child>>);
        // Not a LazyLock, which would turn one failure to start the thread into a panic in every later call.
        static SPAWNER: Mutex<Option<mpsc::Sender<Spawn>>> = Mutex::new(None);

        let parent = process::id() as libc::pid_t;
        // The hook runs in a fork of this multithreaded process, where allocating can deadlock.
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL as libc::c_ulong) != 0 {
                    return Err(io::Error::last_os_error());
                }
                // This process exited before the prctl, so the signal will never come.
                if libc::getppid() != parent {
                    return Err(io::ErrorKind::Other.into());
                }
                Ok(())
            });
        }
        let mut spawner = SPAWNER.lock().unwrap();
        if spawner.is_none() {
            let (sender, spawns) = mpsc::channel::<Spawn>();
            std::thread::Builder::new().spawn(move || {
                for (mut command, spawned) in spawns {
                    let _ = spawned.send(command.spawn());
                }
            })?;
            *spawner = Some(sender);
        }
        let (spawned, child) = mpsc::channel();
        spawner.as_ref().unwrap().send((command, spawned)).unwrap();
        child.recv().unwrap().map(|proc| Self { proc })
    }

    /// Spawn etcd so that it is killed when this process exits.
    ///
    /// etcd joins the process group of a watchdog that kills the group, not etcd's PID: the PID of an etcd that was
    /// reaped can be recycled while the watchdog runs, but not the ID of a group the watchdog is in.
    #[cfg(all(unix, not(target_os = "linux")))]
    fn spawn(mut command: process::Command) -> io::Result<Self> {
        use std::{os::unix::process::CommandExt, sync::Mutex};

        static WATCHDOG: Mutex<Option<process::Child>> = Mutex::new(None);
        let mut watchdog = WATCHDOG.lock().unwrap();
        if watchdog.is_none() {
            *watchdog = Some(spawn_watchdog()?);
        }
        command
            .process_group(watchdog.as_ref().unwrap().id() as i32)
            .spawn()
            .map(|proc| Self { proc })
    }

    /// Spawn etcd so that it is killed when this process exits.
    #[cfg(windows)]
    fn spawn(mut command: process::Command) -> io::Result<Self> {
        use std::{
            os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
            sync::Mutex,
        };
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation, SetInformationJobObject,
        };

        // Windows kills the processes in the job once its last handle closes, which this one does when this process
        // exits.
        static JOB: Mutex<Option<OwnedHandle>> = Mutex::new(None);
        let mut job = JOB.lock().unwrap();
        if job.is_none() {
            unsafe {
                let created = CreateJobObjectW(std::ptr::null(), std::ptr::null());
                if created.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let created = OwnedHandle::from_raw_handle(created);
                let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
                limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                let size = size_of_val(&limits) as u32;
                if SetInformationJobObject(
                    created.as_raw_handle(),
                    JobObjectExtendedLimitInformation,
                    (&raw const limits).cast(),
                    size,
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                *job = Some(created);
            }
        }

        let job = job.as_ref().unwrap();
        // Dropping the runner kills etcd.
        let runner = Self { proc: command.spawn()? };
        if unsafe { AssignProcessToJobObject(job.as_raw_handle(), runner.proc.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(runner)
    }

    #[cfg(not(any(unix, windows)))]
    fn spawn(mut command: process::Command) -> io::Result<Self> {
        command.spawn().map(|proc| Self { proc })
    }
}

/// Spawn a `/bin/sh` that SIGKILLs its process group, itself included, once it reads end-of-file on stdin, a FIFO that
/// only this process can write to, which happens when this process exits.
///
/// Not a pipe from `Stdio::piped`: on macOS, std sets close-on-exec on a pipe's ends only after creating them, so a
/// process that another thread spawns in between can inherit the writer and keep the watchdog from reading end-of-file.
#[cfg(all(unix, any(test, not(target_os = "linux"))))]
fn spawn_watchdog() -> io::Result<process::Child> {
    use std::os::{
        fd::{AsRawFd, OwnedFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt, process::CommandExt},
    };

    let fifo = env::temp_dir().join(format!("etcdrs-watchdog-{}", get_random_name(12)));
    let c_fifo = ffi::CString::new(fifo.as_os_str().as_bytes())?;
    if unsafe { libc::mkfifo(c_fifo.as_ptr(), 0o600) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // Opening either end blocks until the other end is open, but a non-blocking reader opens at once.
    let ends = std::fs::File::options()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(&fifo)
        .and_then(|reader| Ok((reader, std::fs::File::options().write(true).open(&fifo)?)));
    std::fs::remove_file(&fifo)?;
    let (reader, writer) = ends?;
    // A non-blocking stdin would end the watchdog's `read` at once.
    if unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, 0) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let mut watchdog = process::Command::new("/bin/sh")
        .args(["-c", "read _; kill -9 0"])
        .stdin(reader)
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .process_group(0)
        .spawn()?;
    watchdog.stdin = Some(OwnedFd::from(writer).into());
    Ok(watchdog)
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
            eprintln!(
                "Failed to remove etcd server dir {:?}: {error}",
                self.config.working_dir
            );
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
    fn log_tail_quotes_the_last_json_record_or_else_the_last_line() {
        let config = EtcdServerConfig::new_single_temporary();
        std::fs::create_dir(&config.working_dir).unwrap();
        let log = config.log_path();

        let record = r#"{"level":"panic","msg":"failed to create WAL","error":"no space left on device"}"#;
        let panicked = [
            r#"{"level":"info","msg":"bootstrapping storage"}"#,
            record,
            "panic: failed to create WAL",
            "",
            "goroutine 1 [running]:",
            "go.uber.org/zap/zapcore.CheckWriteAction.OnWrite(0x1?, 0x2a7?, {0x0?, 0x0?, 0x36cd976a7060?})",
            "\tgo.uber.org/zap@v1.27.1/zapcore/entry.go:196 +0x54",
        ];
        std::fs::write(&log, panicked.join("\n")).unwrap();
        assert_eq!(config.log_tail(), format!("last record in {log:?}: {record}"));

        std::fs::write(&log, b"stray byte \xff\nlast line\n").unwrap();
        assert_eq!(config.log_tail(), format!("last record in {log:?}: last line"));

        std::fs::remove_dir_all(&config.working_dir).unwrap();
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

    #[cfg(unix)]
    #[test]
    fn watchdog_kills_its_process_group_once_its_pipe_closes() {
        use std::os::unix::process::{CommandExt, ExitStatusExt};

        let mut watchdog = spawn_watchdog().unwrap();
        let mut member = process::Command::new("sleep")
            .arg("60")
            .process_group(watchdog.id() as i32)
            .spawn()
            .unwrap();
        // Long enough for a watchdog whose `read` does not block to kill its group.
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(member.try_wait().unwrap(), None, "killed before the pipe closed");
        // This process exiting closes the pipe the same way.
        drop(watchdog.stdin.take());
        assert_eq!(member.wait().unwrap().signal(), Some(9));
        assert_eq!(watchdog.wait().unwrap().signal(), Some(9));
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
