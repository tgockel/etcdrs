use crate::{
    LeaseId, Revision, Version,
    record::{KeyWithMetadata, Metadata, Record},
};
use std::{fmt, ops::AsyncFn, sync::Arc};

mod metrics;
pub use metrics::{MetricsCollector, RequestCount, RequestCounter};
mod ops;
pub use ops::*;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Create a client from a connection string.
    ///
    /// The connection string is a comma-separated list of endpoint URIs, following the etcd
    /// convention used by `etcdctl --endpoints`:
    ///
    /// ```text
    /// http://host1:2379,http://host2:2379,http://host3:2379
    /// ```
    ///
    /// A single URI is also accepted.
    pub fn new(connection_string: &str) -> Result<Self, BuildError> {
        Self::builder().connection_string(connection_string)?.build()
    }
}

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

#[derive(Default)]
pub struct ClientBuilder {
    remotes: Vec<Remote>,
    configure_endpoint: Option<Box<ConfigureEndpointFn>>,
    metrics: Option<Box<dyn MetricsCollector>>,
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

    pub fn build(self) -> Result<Client, BuildError> {
        let Self {
            remotes,
            configure_endpoint,
            metrics,
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

        Ok(Client {
            inner: Arc::new(ClientInner { channel, metrics }),
        })
    }
}

/// An error from building a [`Client`].
pub enum BuildError {
    /// An invalid URI was provided.
    InvalidUri(Box<dyn std::error::Error + Send + Sync>),
    /// No endpoints were configured before calling [`build`][ClientBuilder::build].
    NoEndpoints,
    /// The [`configure_endpoint`][ClientBuilder::configure_endpoint] callback returned an error.
    EndpointConfiguration(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Debug for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(err) => f.debug_tuple("InvalidUri").field(err).finish(),
            Self::NoEndpoints => f.debug_tuple("NoEndpoints").finish(),
            Self::EndpointConfiguration(err) => f.debug_tuple("EndpointConfiguration").field(err).finish(),
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUri(err) => write!(f, "invalid URI: {err}"),
            Self::NoEndpoints => write!(f, "no endpoints were configured"),
            Self::EndpointConfiguration(err) => write!(f, "endpoint configuration failed: {err}"),
        }
    }
}

impl std::error::Error for BuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidUri(err) | Self::EndpointConfiguration(err) => Some(&**err),
            Self::NoEndpoints => None,
        }
    }
}

struct ClientInner {
    channel: tonic::transport::Channel,
    metrics: Option<Box<dyn MetricsCollector>>,
}

fn metadata_from_pb(r: &crate::pb::mvccpb::KeyValue) -> Metadata {
    Metadata {
        create_revision: Revision::new(r.create_revision).unwrap(),
        modified_revision: Revision::new(r.mod_revision).unwrap(),
        version: Version::new(r.version as u64),
        lease: LeaseId::new(r.lease),
    }
}

fn record_from_pb(r: crate::pb::mvccpb::KeyValue) -> Record {
    let metadata = metadata_from_pb(&r);
    Record::new(r.key, r.value, metadata)
}

fn key_with_metadata_from_pb(r: crate::pb::mvccpb::KeyValue) -> KeyWithMetadata {
    let metadata = metadata_from_pb(&r);
    KeyWithMetadata::new(r.key, metadata)
}

fn is_client_side_timeout(status: &tonic::Status) -> bool {
    // Tonic does a client-side timeout if the server-side does not respond in time. This is the
    // only way to check for that.
    status.code() == tonic::Code::Cancelled && status.message() == "Timeout expired"
}

impl ClientInner {
    /// Execute a unary gRPC call with retry and timeout logic.
    ///
    /// Returns `Ok(response)` on success or `Err(tonic::Status)` on failure. Callers convert the
    /// status into their operation-specific error type via `map_err`.
    async fn wrap_unary_call<R, GrpcClient, Request>(
        &self,
        create_client: impl Fn(tonic::transport::Channel) -> GrpcClient,
        call: impl AsyncFn(&mut GrpcClient, tonic::Request<Request>) -> Result<tonic::Response<R>, tonic::Status>,
        request: Request,
    ) -> Result<R, tonic::Status>
    where
        Request: Clone,
    {
        // TODO: configurable
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut client = create_client(self.channel.clone());
        loop {
            let metric = metrics::MetricsSpan::new(&self.metrics);
            let mut call_request = tonic::Request::new(request.clone());
            if let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) {
                call_request.set_timeout(remaining);
            }
            let response = call(&mut client, call_request).await;
            metric.complete(response.is_ok());
            match response {
                Ok(r) => break Ok(r.into_inner()),
                Err(e) => {
                    if Self::can_retry(&e) && std::time::Instant::now() <= deadline {
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn can_retry(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::Unavailable || is_client_side_timeout(status)
    }
}
