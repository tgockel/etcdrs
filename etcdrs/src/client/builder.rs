use std::{fmt, sync::Arc};

use super::{AuthState, Client, ClientInner, Credentials, MetricsCollector, RetryPolicy};

#[allow(
    clippy::large_enum_variant,
    reason = "this is only used in a Vec inside ClientBuilder"
)]
enum Remote {
    Unconfigured(tonic::transport::Uri),
    Preconfigured(tonic::transport::Endpoint),
}

type ConfigureEndpointFn =
    dyn Fn(tonic::transport::Endpoint) -> Result<tonic::transport::Endpoint, Box<dyn std::error::Error + Send + Sync>>;

enum AuthConfig {
    Credentials(Credentials),
    Token(tonic::metadata::AsciiMetadataValue),
}

/// etcd sends messages of up to `math.MaxInt32` bytes.
const DEFAULT_MAX_RESPONSE_SIZE: usize = i32::MAX as usize;

#[derive(Default)]
pub struct ClientBuilder {
    remotes: Vec<Remote>,
    configure_endpoint: Option<Box<ConfigureEndpointFn>>,
    metrics: Option<Box<dyn MetricsCollector>>,
    auth: Option<AuthConfig>,
    retry_policy: RetryPolicy,
    max_response_size: Option<usize>,
}

impl ClientBuilder {
    /// Parse a comma-separated connection string and add all endpoints.
    pub fn connection_string(mut self, connection_string: impl AsRef<str>) -> Result<Self, BuildError> {
        for host in connection_string.as_ref().split(',') {
            let host = host.trim();
            if !host.is_empty() {
                self = self.add_connection(host)?;
            }
        }
        Ok(self)
    }

    /// Add a single endpoint by URI.
    pub fn add_connection(mut self, host: impl AsRef<str>) -> Result<Self, BuildError> {
        let uri = host
            .as_ref()
            .parse::<tonic::transport::Uri>()
            .map_err(|err| BuildError::InvalidUri(Box::new(err)))?;
        self.remotes.push(Remote::Unconfigured(uri));
        Ok(self)
    }

    /// Add a pre-configured [`tonic::transport::Endpoint`].
    ///
    /// Pre-configured endpoints bypass the [`configure_endpoint`][Self::configure_endpoint]
    /// function and are connected to as-is.
    pub fn add_endpoint(mut self, endpoint: tonic::transport::Endpoint) -> Self {
        self.remotes.push(Remote::Preconfigured(endpoint));
        self
    }

    /// Set a function to configure endpoints created from URIs.
    ///
    /// This function is applied to each endpoint added via
    /// [`connection_string`][Self::connection_string] or [`add_connection`][Self::add_connection]
    /// during [`build`][Self::build]. It is **not** applied to endpoints added via
    /// [`add_endpoint`][Self::add_endpoint].
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let client = Client::builder()
    ///     .connection_string("http://host1:2379,http://host2:2379")?
    ///     .configure_endpoint(|ep| Ok(ep.connect_timeout(Duration::from_secs(5))))
    ///     .build()?;
    /// ```
    pub fn configure_endpoint(
        mut self,
        f: impl Fn(
            tonic::transport::Endpoint,
        ) -> Result<tonic::transport::Endpoint, Box<dyn std::error::Error + Send + Sync>>
        + 'static,
    ) -> Self {
        self.configure_endpoint = Some(Box::new(f));
        self
    }

    pub fn metrics(mut self, metrics: impl MetricsCollector + 'static) -> Self {
        self.metrics = Some(Box::new(metrics));
        self
    }

    /// Set the retry policy for transient failures.
    ///
    /// Accepts a [`RetryPolicy`] or a [`std::time::Duration`] (which becomes
    /// [`RetryPolicy::WithDeadline`]). Use [`retry_never`][Self::retry_never] or
    /// [`retry_forever`][Self::retry_forever] for the other variants.
    ///
    /// The default is [`RetryPolicy::WithDeadline`] of 5 seconds.
    pub fn retry_policy(mut self, policy: impl Into<RetryPolicy>) -> Self {
        self.retry_policy = policy.into();
        self
    }

    /// Never retry on transient failures; return the first error immediately.
    pub fn retry_never(self) -> Self {
        self.retry_policy(RetryPolicy::Never)
    }

    /// Retry transient failures forever with no deadline.
    pub fn retry_forever(self) -> Self {
        self.retry_policy(RetryPolicy::Forever)
    }

    /// Set the largest response, in bytes, that the client accepts.
    ///
    /// The limit applies to each gRPC message: a list that etcd answers in one response counts that
    /// whole response, and a watch counts each response it receives. A response over the limit fails
    /// the operation with the gRPC status `OutOfRange`, which the error's `grpc_status` returns, and
    /// ends a [`Watcher`][super::Watcher] or [`LeaseKeeper`][super::LeaseKeeper].
    ///
    /// The default is `i32::MAX`, the most etcd sends in one message, so the client accepts every
    /// response etcd can send. A limit of `0` is not special: it refuses every response with content.
    ///
    /// A lower limit has consequences beyond the failed call:
    ///
    /// - etcd has already acted on a request when it sends the response, and the client does not
    ///   replay a refused one, so a refused response to a write leaves the write applied.
    /// - A [list][super::List] answers in one response unless it has a
    ///   [`limit`][super::List::limit]. etcdrs-util's `CacheClient` lists each range it caches in one
    ///   request, so a limit below the size of the records in a range keeps the cache from seeding it.
    /// - A watch that starts from a past revision fails if etcd replays that history in a response
    ///   over the limit, and a watch restarted from the same revision can fail again.
    /// - With [`credentials`][Self::credentials], the client authenticates by itself, and a limit
    ///   below the size of that response keeps it from getting a token. Calls then fail with etcd's
    ///   refusal of a request that carried no token, not with `OutOfRange`.
    ///   [`authenticate`][Client::authenticate] reports the size error.
    pub fn max_response_size(mut self, bytes: usize) -> Self {
        self.max_response_size = Some(bytes);
        self
    }

    /// Set credentials for authenticating with the etcd cluster.
    ///
    /// The token is obtained lazily: the first unary request goes out without one, and when the
    /// server rejects it the client runs the `Authenticate` RPC with these credentials and replays
    /// the request. The token is then cached and reused, and refreshed the same way if the server
    /// later stops accepting it. Each request refreshes at most once.
    ///
    /// A `PERMISSION_DENIED` response is returned to the caller unchanged. It means the
    /// authenticated user's roles do not cover the request, which re-authenticating cannot change.
    ///
    /// [`watch`][Client::watch] and [`lease_keeper`][Client::lease_keeper] send the cached token
    /// once, when their stream opens, authenticating first if no token is cached. etcd checks it
    /// each time the stream creates a watch or keeps a lease alive, so it can stop accepting the
    /// token while the stream is open, for example once the token goes unused for five minutes
    /// (etcd's default) or its user's password changes. A [`LeaseKeeper`][super::LeaseKeeper] then
    /// reopens its stream with a refreshed token and sends again the keep-alives etcd did not
    /// answer. A [`Watcher`][super::Watcher] does the same until etcd has answered one of its
    /// watches; [`Watcher::add`][super::Watcher::add] describes what happens after that.
    ///
    /// [`authenticate`][Client::authenticate] is an exception: it sends one `Authenticate` RPC
    /// directly and hands the token back to you instead of caching it here.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(AuthConfig::Credentials(Credentials {
            username: username.into(),
            password: password.into(),
        }));
        self
    }

    /// Set a pre-obtained auth token for authenticating with the etcd cluster.
    ///
    /// The token is injected into all requests, and sent when a [`watch`][Client::watch] or
    /// [`lease_keeper`][Client::lease_keeper] stream opens. If the token expires, the client cannot
    /// refresh it automatically: requests and keep-alives fail, and etcd refuses the watches created
    /// after that. Use [`credentials`][Self::credentials] instead for automatic token management.
    pub fn auth_token(mut self, token: impl Into<String>) -> Result<Self, BuildError> {
        let value = token
            .into()
            .parse::<tonic::metadata::AsciiMetadataValue>()
            .map_err(|err| BuildError::InvalidAuthToken(Box::new(err)))?;
        self.auth = Some(AuthConfig::Token(value));
        Ok(self)
    }

    /// Create the client from the endpoints and settings configured so far.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a Tokio runtime, which is required to spawn the background task
    /// that drives the connection. A missing runtime is a panic, not a [`BuildError`].
    pub fn build(self) -> Result<Client, BuildError> {
        let Self {
            remotes,
            configure_endpoint,
            metrics,
            auth,
            retry_policy,
            max_response_size,
        } = self;

        if remotes.is_empty() {
            return Err(BuildError::NoEndpoints);
        }

        let endpoints = remotes
            .into_iter()
            .map(|remote| match remote {
                Remote::Unconfigured(uri) => {
                    let ep = tonic::transport::Channel::builder(uri);
                    match &configure_endpoint {
                        Some(f) => f(ep).map_err(BuildError::EndpointConfiguration),
                        None => Ok(ep),
                    }
                }
                Remote::Preconfigured(ep) => Ok(ep),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let channel = if endpoints.len() == 1 {
            endpoints.into_iter().next().unwrap().connect_lazy()
        } else {
            tonic::transport::Channel::balance_list(endpoints.into_iter())
        };

        let auth = auth.map(|config| match config {
            AuthConfig::Credentials(creds) => AuthState {
                credentials: Some(creds),
                token: std::sync::RwLock::new((0, None)),
                refresh: tokio::sync::Mutex::new(()),
            },
            AuthConfig::Token(token) => AuthState {
                credentials: None,
                token: std::sync::RwLock::new((0, Some(token))),
                refresh: tokio::sync::Mutex::new(()),
            },
        });

        Ok(Client {
            inner: Arc::new(ClientInner {
                channel,
                metrics,
                auth,
                retry_policy,
                max_response_size: max_response_size.unwrap_or(DEFAULT_MAX_RESPONSE_SIZE),
            }),
        })
    }
}

/// An error from building a [`Client`].
#[derive(Debug)]
pub enum BuildError {
    /// An invalid URI was provided.
    InvalidUri(Box<dyn std::error::Error + Send + Sync>),
    /// No endpoints were configured before calling [`build`][ClientBuilder::build].
    NoEndpoints,
    /// The [`configure_endpoint`][ClientBuilder::configure_endpoint] callback returned an error.
    EndpointConfiguration(Box<dyn std::error::Error + Send + Sync>),
    /// An invalid auth token was provided.
    InvalidAuthToken(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(err) => write!(f, "invalid URI: {err}"),
            Self::NoEndpoints => write!(f, "no endpoints were configured"),
            Self::EndpointConfiguration(err) => write!(f, "endpoint configuration failed: {err}"),
            Self::InvalidAuthToken(err) => write!(f, "invalid auth token: {err}"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidUri(err) | Self::EndpointConfiguration(err) | Self::InvalidAuthToken(err) => Some(&**err),
            Self::NoEndpoints => None,
        }
    }
}
