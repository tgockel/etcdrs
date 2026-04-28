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

#[derive(Default)]
pub struct ClientBuilder {
    remotes: Vec<Remote>,
    configure_endpoint: Option<Box<ConfigureEndpointFn>>,
    metrics: Option<Box<dyn MetricsCollector>>,
    auth: Option<AuthConfig>,
    retry_policy: RetryPolicy,
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

    /// Set credentials for authenticating with the etcd cluster.
    ///
    /// When the client encounters an `UNAUTHENTICATED` error, it will use these credentials to
    /// obtain an auth token via the `Authenticate` RPC, then retry the request. The token is cached
    /// and reused for subsequent requests.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(AuthConfig::Credentials(Credentials {
            username: username.into(),
            password: password.into(),
        }));
        self
    }

    /// Set a pre-obtained auth token for authenticating with the etcd cluster.
    ///
    /// The token is injected into all requests. If the token expires, the client cannot refresh it
    /// automatically; use [`credentials`][Self::credentials] instead for automatic token management.
    pub fn auth_token(mut self, token: impl Into<String>) -> Result<Self, BuildError> {
        let value = token
            .into()
            .parse::<tonic::metadata::AsciiMetadataValue>()
            .map_err(|err| BuildError::InvalidAuthToken(Box::new(err)))?;
        self.auth = Some(AuthConfig::Token(value));
        Ok(self)
    }

    pub fn build(self) -> Result<Client, BuildError> {
        let Self {
            remotes,
            configure_endpoint,
            metrics,
            auth,
            retry_policy,
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
                token: tokio::sync::RwLock::new((0, None)),
                refresh: tokio::sync::Mutex::new(()),
            },
            AuthConfig::Token(token) => AuthState {
                credentials: None,
                token: tokio::sync::RwLock::new((0, Some(token))),
                refresh: tokio::sync::Mutex::new(()),
            },
        });

        Ok(Client {
            inner: Arc::new(ClientInner {
                channel,
                metrics,
                auth,
                retry_policy,
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
