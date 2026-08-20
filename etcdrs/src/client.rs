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
    // the server before the connection dropped -- this single status covers both "the h2 handshake
    // never completed" and "the connection was reset after the request was fully written", and
    // nothing in the status separates them. Only replay operations that tolerate it; see
    // `Idempotency` and `is_connect_error`.
    use std::error::Error as _;
    status.code() == tonic::Code::Unknown && status.source().is_some() && status.message() == "transport error"
}

fn is_connect_error(status: &tonic::Status) -> bool {
    // tonic builds a ConnectError only inside the connector -- TCP/UDS connect and TLS handshake --
    // so finding one in the source chain proves no part of the request was written to a connection.
    // That makes the call safe to replay even when it mutates state.
    //
    // A server cannot fake this: statuses decoded from gRPC response headers carry no source at
    // all, so this cannot be reached by anything but a local connection failure. It is the
    // structural equivalent of the "there is no address available" / "there is no connection
    // available" string match in etcd's own client (client/v3/retry.go, isSafeRetryMutableRPC).
    use std::error::Error as _;
    let mut source = status.source();
    while let Some(err) = source {
        if err.is::<tonic::ConnectError>() {
            return true;
        }
        source = err.source();
    }
    false
}

/// Whether re-sending an RPC after an ambiguous failure is safe.
///
/// A failure that arrives after the request was written is ambiguous: the server may or may not
/// have applied it. Replaying a read costs nothing, but replaying a mutation can apply a change
/// twice or turn a success into a spurious `AlreadyExists`/`NotFound`, so the two are retried under
/// different rules. See [`ClientInner::can_retry`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Idempotency {
    /// The RPC does not modify server state (`KV/Range`, `Cluster/MemberList`, ...). Re-sending it
    /// can only waste work.
    Immutable,
    /// The RPC modifies server state (`KV/Put`, `Cluster/MemberRemove`, ...). Re-sending it is only
    /// safe when the request provably never reached a server.
    Mutable,
}

impl ClientInner {
    /// Execute a unary gRPC call with retry and timeout logic.
    ///
    /// `idempotency` declares whether the RPC being invoked mutates server state, which decides
    /// how aggressively a failed attempt may be replayed. It cannot be inferred here, because the
    /// method being called is opaque inside `call`.
    ///
    /// Returns `Ok(response)` on success or `Err(tonic::Status)` on failure. Callers convert the
    /// status into their operation-specific error type via `map_err`.
    async fn wrap_unary_call<R, GrpcClient, Request>(
        &self,
        idempotency: Idempotency,
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
                    // Auth failures replay regardless of idempotency: etcd authorizes a request
                    // before applying it, so UNAUTHENTICATED proves the mutation did not take
                    // effect and re-sending it with a fresh token cannot duplicate anything.
                    if Self::is_auth_error(&e)
                        && self.auth.is_some()
                        && self.refresh_auth_token(token_generation).await.is_ok()
                        && timing_allows_retry()
                    {
                        continue;
                    }
                    if Self::can_retry(&e, idempotency) && timing_allows_retry() {
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn can_retry(status: &tonic::Status, idempotency: Idempotency) -> bool {
        match idempotency {
            // Broader than etcd's own immutable rule, which retries on Unavailable alone: a killed
            // etcd server surfaces through tonic as Unknown/"transport error" rather than
            // Unavailable (see 7def038). Replaying a read is free either way.
            Idempotency::Immutable => {
                status.code() == tonic::Code::Unavailable
                    || is_client_side_timeout(status)
                    || is_transport_error(status)
            }
            // Every other failure may already have been applied by the server, and replaying it
            // would violate write-at-most-once.
            Idempotency::Mutable => is_connect_error(status),
        }
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

#[cfg(test)]
mod test {
    use super::{Idempotency::*, *};

    /// A status shaped like the one tonic produces when the connector cannot reach an endpoint.
    /// `Status::from_error` walks the source chain and turns a `ConnectError` into `Unavailable`,
    /// keeping the original error as the source, which is exactly what `is_connect_error` reads.
    fn connect_failure() -> tonic::Status {
        tonic::Status::from_error(Box::new(tonic::ConnectError(Box::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )))))
    }

    /// A dropped connection: `Unknown` with a transport-layer source. Indistinguishable from a
    /// half-open h2 handshake, which is why mutations must not replay it.
    ///
    /// `tonic::transport::Error` cannot be constructed outside tonic, so this stands in for it.
    /// `Status::from_error` recognizes neither it nor its source chain, which is precisely how the
    /// real error reaches the `Unknown` + attached-source shape that `is_transport_error` matches.
    fn dropped_connection() -> tonic::Status {
        #[derive(Debug)]
        struct TransportError;

        impl std::fmt::Display for TransportError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("transport error")
            }
        }

        impl std::error::Error for TransportError {}

        tonic::Status::from_error(Box::new(TransportError))
    }

    #[test]
    fn connect_failure_is_recognized() {
        let status = connect_failure();
        assert_eq!(status.code(), tonic::Code::Unavailable, "{status:?}");
        assert!(is_connect_error(&status), "{status:?}");
    }

    #[test]
    fn never_sent_retries_for_both_classes() {
        // Nothing was written to a connection, so even a mutation may be re-sent.
        let status = connect_failure();
        assert!(ClientInner::can_retry(&status, Immutable));
        assert!(ClientInner::can_retry(&status, Mutable));
    }

    #[test]
    fn dropped_connection_retries_only_reads() {
        let status = dropped_connection();
        assert!(is_transport_error(&status), "{status:?}");
        assert!(!is_connect_error(&status), "{status:?}");
        assert!(ClientInner::can_retry(&status, Immutable));
        assert!(
            !ClientInner::can_retry(&status, Mutable),
            "a mutation may already have applied"
        );
    }

    #[test]
    fn server_sent_unavailable_retries_only_reads() {
        // Decoded from response headers, so it carries no source. The server did answer, so a
        // mutation cannot be assumed unapplied.
        let status = tonic::Status::new(tonic::Code::Unavailable, "etcdserver: no leader");
        assert!(!is_connect_error(&status), "{status:?}");
        assert!(ClientInner::can_retry(&status, Immutable));
        assert!(!ClientInner::can_retry(&status, Mutable));
    }

    #[test]
    fn client_side_timeout_retries_only_reads() {
        let status = tonic::Status::new(tonic::Code::Cancelled, "Timeout expired");
        assert!(is_client_side_timeout(&status));
        assert!(ClientInner::can_retry(&status, Immutable));
        assert!(!ClientInner::can_retry(&status, Mutable));
    }

    #[test]
    fn non_transient_errors_never_retry() {
        for status in [
            tonic::Status::new(tonic::Code::NotFound, "etcdserver: member not found"),
            tonic::Status::new(tonic::Code::FailedPrecondition, "etcdserver: ID exists"),
            tonic::Status::new(tonic::Code::InvalidArgument, "etcdserver: key is not provided"),
        ] {
            assert!(!ClientInner::can_retry(&status, Immutable), "{status:?}");
            assert!(!ClientInner::can_retry(&status, Mutable), "{status:?}");
        }
    }

    /// A server cannot talk the client into replaying a mutation by claiming to be a connect
    /// failure: statuses decoded from response headers have no source to walk.
    #[test]
    fn server_cannot_forge_a_connect_failure() {
        let status = tonic::Status::new(tonic::Code::Unavailable, "tcp connect error");
        assert!(!is_connect_error(&status), "{status:?}");
        assert!(!ClientInner::can_retry(&status, Mutable));
    }
}
