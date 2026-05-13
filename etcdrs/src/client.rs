#![doc = include_str!("client/README.md")]

use crate::{
    LeaseId, Revision, Version,
    record::{KeyWithMetadata, Metadata, Record},
};
use std::{ops::AsyncFn, sync::Arc};

mod builder;
pub use builder::{BuildError, ClientBuilder};
mod driver;
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

struct Credentials {
    username: String,
    password: String,
}

/// Controls how long [`wrap_unary_call`][ClientInner::wrap_unary_call] retries transient failures.
#[derive(Debug, Clone, PartialEq)]
pub enum RetryPolicy {
    /// Retry until the given duration has elapsed since the call started.
    ///
    /// The remaining time is also used as the per-request gRPC deadline sent to the server.
    WithDeadline(std::time::Duration),
    /// Never retry; return the first error immediately.
    Never,
    /// Retry forever with no deadline.
    Forever,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::WithDeadline(std::time::Duration::from_secs(5))
    }
}

impl From<std::time::Duration> for RetryPolicy {
    fn from(d: std::time::Duration) -> Self {
        Self::WithDeadline(d)
    }
}

struct AuthState {
    credentials: Option<Credentials>,
    /// The cached auth token and a generation counter. The generation is incremented each time the
    /// token is refreshed, allowing concurrent callers to detect stale tokens without a separate
    /// invalidation step.
    token: tokio::sync::RwLock<(u64, Option<tonic::metadata::AsciiMetadataValue>)>,
    /// Serializes refresh attempts so only one `Authenticate` RPC runs at a time.
    refresh: tokio::sync::Mutex<()>,
}

struct ClientInner {
    channel: tonic::transport::Channel,
    metrics: Option<Box<dyn MetricsCollector>>,
    auth: Option<AuthState>,
    retry_policy: RetryPolicy,
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

fn is_transport_error(status: &tonic::Status) -> bool {
    // On the client, tonic wraps dropped-connection errors (TCP reset, broken pipe) as
    // Code::Unknown with message "transport error" -- from tonic::transport::Error(Kind::Transport)
    // via Status::from_error. The h2-aware conversion paths in try_from_error and
    // from_hyper_error are #[cfg(feature = "server")] only and do not run on the client.
    //
    // etcd-returned Unknown statuses are decoded from gRPC response headers and carry no source.
    // The source check distinguishes those from transport-layer failures, where
    // tonic::transport::Error is stored as the source.
    //
    // Note: like is_client_side_timeout, this cannot guarantee the request was not processed by
    // the server before the connection dropped. etcd operations are safe to retry in practice.
    use std::error::Error as _;
    status.code() == tonic::Code::Unknown && status.source().is_some() && status.message() == "transport error"
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
        let deadline: Option<std::time::Instant> = match &self.retry_policy {
            RetryPolicy::WithDeadline(d) => Some(std::time::Instant::now() + *d),
            RetryPolicy::Never | RetryPolicy::Forever => None,
        };
        let timing_allows_retry = || match &self.retry_policy {
            RetryPolicy::Never => false,
            RetryPolicy::Forever => true,
            RetryPolicy::WithDeadline(_) => deadline.is_some_and(|d| std::time::Instant::now() <= d),
        };
        let mut client = create_client(self.channel.clone());
        loop {
            let metric = metrics::MetricsSpan::new(&self.metrics);
            let mut call_request = tonic::Request::new(request.clone());
            if let Some(remaining) = deadline.and_then(|d| d.checked_duration_since(std::time::Instant::now())) {
                call_request.set_timeout(remaining);
            }
            let mut token_generation = 0u64;
            if let Some(ref auth) = self.auth {
                let state = auth.token.read().await;
                if let Some(ref token) = state.1 {
                    call_request.metadata_mut().insert("token", token.clone());
                }
                token_generation = state.0;
            }
            let response = call(&mut client, call_request).await;
            metric.complete(response.is_ok());
            match response {
                Ok(r) => break Ok(r.into_inner()),
                Err(e) => {
                    if Self::is_auth_error(&e)
                        && self.auth.is_some()
                        && self.refresh_auth_token(token_generation).await.is_ok()
                        && timing_allows_retry()
                    {
                        continue;
                    }
                    if Self::can_retry(&e) && timing_allows_retry() {
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn can_retry(status: &tonic::Status) -> bool {
        status.code() == tonic::Code::Unavailable || is_client_side_timeout(status) || is_transport_error(status)
    }

    /// Check if a gRPC status represents an authentication/authorization error.
    ///
    /// etcd returns different codes depending on context: `UNAUTHENTICATED` for expired tokens,
    /// `PERMISSION_DENIED` for insufficient permissions, and `INVALID_ARGUMENT` with auth-related
    /// messages (e.g., "user name is empty") when no token is provided.
    fn is_auth_error(status: &tonic::Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
        ) || (status.code() == tonic::Code::InvalidArgument && status.message().contains("user name"))
    }
}
