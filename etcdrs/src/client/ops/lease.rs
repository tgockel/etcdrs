use std::{
    future::{Future, IntoFuture},
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_core::Stream;

use crate::{Client, LeaseId, ResponseHeader, pb::etcdserverpb};

impl Client {
    /// Create a lease used to create ephemeral records.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let lease_info = client.grant_lease()
    ///     .ttl(std::time::Duration::from_secs(60))
    ///     .await
    ///     .expect("failed to grant lease");
    ///
    /// // Put a key using that lease
    /// client.put("foo")
    ///     .value("bar")
    ///     .lease(lease_info.lease_id)
    ///     .await
    ///     .expect("failed to put key");
    ///
    /// // Wait for the lease to expire
    /// std::thread::sleep(lease_info.ttl.unwrap());
    ///
    /// // etcd does not always immediately revoke leases, but if you just have one
    /// // server and the clocks are in sync, it will be fast and this will be None
    /// println!("foo? {:?}", client.get("foo").await.unwrap());
    /// # };
    /// ```
    pub fn grant_lease(&self) -> GrantLease<Self> {
        GrantLease::new().with_client(self.clone())
    }

    /// Revoke an existing lease.
    pub async fn revoke_lease(&self, lease_id: LeaseId) -> Result<RevokeLeaseResponse, RevokeLeaseError> {
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::lease_client::LeaseClient::new,
                async |c, r| c.lease_revoke(r).await,
                etcdserverpb::LeaseRevokeRequest { id: lease_id.get() },
            )
            .await
            .map_err(RevokeLeaseError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("LeaseRevokeResponse should have a valid header"));
        Ok(RevokeLeaseResponse { header })
    }

    /// Create a [`LeaseKeeper`] for keeping leases alive via a bidirectional streaming RPC.
    ///
    /// The gRPC stream is established lazily on the first poll of the returned [`LeaseKeeper`].
    /// Use [`keep_alive`][LeaseKeeper::keep_alive] to send keep-alive requests for specific
    /// leases, and consume the stream to receive responses.
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// # let lease_id: etcdrs::LeaseId = todo!();
    /// let keeper = client.lease_keeper();
    /// keeper.keep_alive(lease_id);
    /// while let Some(resp) = keeper.next().await {
    ///     let resp = resp.unwrap();
    ///     println!("lease {:?} TTL: {:?}", resp.lease_id, resp.ttl);
    /// }
    /// # };
    /// ```
    pub fn lease_keeper(&self) -> LeaseKeeper {
        let channel = self.inner.channel.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let shared_rx = Arc::new(std::sync::Mutex::new(rx));

        let inner = async_stream::stream! {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            let mut response_stream = loop {
                let request_stream = KeepAliveReceiverStream { inner: shared_rx.clone() };
                let mut lease_client = etcdserverpb::lease_client::LeaseClient::new(channel.clone());
                match lease_client.lease_keep_alive(request_stream).await {
                    Ok(resp) => break resp.into_inner(),
                    Err(status)
                        if status.code() == tonic::Code::Unavailable
                            && std::time::Instant::now() <= deadline =>
                    {
                        continue;
                    }
                    Err(status) => {
                        yield Err(KeepAliveError::from_status(status));
                        return;
                    }
                }
            };

            loop {
                match response_stream.message().await {
                    Ok(Some(resp)) => {
                        let header = ResponseHeader::from_pb(
                            resp.header.expect("LeaseKeepAliveResponse should have a valid header"),
                        );
                        let lease_id =
                            LeaseId::new(resp.id).expect("LeaseKeepAliveResponse should have a valid lease ID");
                        let ttl = if resp.ttl > 0 { Some(Duration::from_secs(resp.ttl as _)) } else { None };
                        yield Ok(KeepAliveResponse {
                            header,
                            info: LeaseInfo { lease_id, ttl },
                        });
                    }
                    Ok(None) => break,
                    Err(status) => {
                        yield Err(KeepAliveError::from_status(status));
                        break;
                    }
                }
            }
        };

        LeaseKeeper {
            sender: KeepAliveSender { sender: tx },
            stream: KeepAliveStream { inner: Box::new(inner) },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LeaseInfo {
    pub lease_id: LeaseId,
    /// The remaining time-to-live on the lease, or `None` if the lease has expired.
    pub ttl: Option<Duration>,
}

#[derive(Clone)]
#[must_use = "GrantLease does nothing unless you `await` it"]
pub struct GrantLease<C> {
    pub(crate) client: C,
    pub(crate) request: etcdserverpb::LeaseGrantRequest,
}

impl GrantLease<()> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            client: (),
            request: etcdserverpb::LeaseGrantRequest::default(),
        }
    }
}

impl<C> GrantLease<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`grant_lease`][`Client::grant_lease`].
    pub fn with_client<C2>(self, client: C2) -> GrantLease<C2> {
        GrantLease {
            client,
            request: self.request,
        }
    }

    /// Request a specific ID for the lease.
    ///
    /// If left unspecified (the default), a unique ID will be generated for you.
    pub fn lease_id(mut self, lease_id: LeaseId) -> Self {
        self.request.id = lease_id.get();
        self
    }

    /// Request a time-to-live on the lease.
    ///
    /// This is only the requested time-to-live; the service will give you a different time-to-live if it is out of
    /// bounds for the configuration. Check the returned [`ttl`][LeaseInfo::ttl] to see how long you were given in the
    /// lease.
    ///
    /// While a [`Duration`] can be nanosecond resolution, the service itself only respects whole seconds. Leaving this
    /// unspecified will use the lowest-possible TTL the server supports (usually 2 seconds).
    pub fn ttl(mut self, duration: Duration) -> Self {
        self.request.ttl = duration.as_secs() as _;
        self
    }
}

impl GrantLease<Client> {
    async fn call(self) -> Result<GrantLeaseResponse, GrantLeaseError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::lease_client::LeaseClient::new,
                async |c, r| c.lease_grant(r).await,
                self.request,
            )
            .await
            .map_err(GrantLeaseError::from_status)?;

        if !resp.error.is_empty() {
            return Err(GrantLeaseError::new(GrantLeaseErrorKind::LeaseExists, resp.error, None));
        }

        let header = ResponseHeader::from_pb(resp.header.expect("LeaseGrantResponse should have a valid header"));
        let lease_id = LeaseId::new(resp.id).expect("etcd server should have returned a lease");
        let ttl = if resp.ttl > 0 {
            Some(Duration::from_secs(resp.ttl as _))
        } else {
            None
        };

        Ok(GrantLeaseResponse {
            header,
            info: LeaseInfo { lease_id, ttl },
        })
    }
}

/// The response from a [`grant_lease`][Client::grant_lease] operation.
#[derive(Clone, Copy, Debug)]
pub struct GrantLeaseResponse {
    header: ResponseHeader,
    info: LeaseInfo,
}

impl GrantLeaseResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The lease information, including the assigned ID and granted TTL.
    pub fn info(&self) -> &LeaseInfo {
        &self.info
    }

    /// Consume the response and return the [`LeaseInfo`].
    pub fn into_info(self) -> LeaseInfo {
        self.info
    }
}

impl Deref for GrantLeaseResponse {
    type Target = LeaseInfo;

    fn deref(&self) -> &LeaseInfo {
        &self.info
    }
}

/// The response from a [`revoke_lease`][Client::revoke_lease] operation.
#[derive(Clone, Copy, Debug)]
pub struct RevokeLeaseResponse {
    header: ResponseHeader,
}

impl RevokeLeaseResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The [`Future`] type returned by awaiting a [`grant_lease`][`Client::grant_lease`].
pub struct GrantLeaseFuture(Pin<Box<dyn Future<Output = Result<GrantLeaseResponse, GrantLeaseError>> + Send>>);

impl Future for GrantLeaseFuture {
    type Output = Result<GrantLeaseResponse, GrantLeaseError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for GrantLease<Client> {
    type Output = Result<GrantLeaseResponse, GrantLeaseError>;
    type IntoFuture = GrantLeaseFuture;

    fn into_future(self) -> Self::IntoFuture {
        GrantLeaseFuture(Box::pin(self.call()))
    }
}

/// An enumeration of the [`kind`][GrantLeaseError::kind]s of errors that can occur from a
/// [`grant_lease`][Client::grant_lease] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrantLeaseErrorKind {
    /// The requested lease ID already exists.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "lease already exists", or via the response's error
    /// field from older etcd servers.
    LeaseExists,
    /// The requested TTL exceeds the server's maximum lease TTL.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`.
    TtlTooLarge,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and `INVALID_ARGUMENT` when the argument
    /// describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a [`grant_lease`][Client::grant_lease] operation.
    pub struct GrantLeaseError(GrantLeaseErrorKind);
}

impl GrantLeaseError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::Unauthenticated => GrantLeaseErrorKind::Authentication,
            tonic::Code::PermissionDenied => GrantLeaseErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => GrantLeaseErrorKind::Authentication,
            tonic::Code::ResourceExhausted => GrantLeaseErrorKind::Exhausted,
            tonic::Code::OutOfRange => GrantLeaseErrorKind::TtlTooLarge,
            tonic::Code::FailedPrecondition => {
                if status.message().contains("already exists") {
                    GrantLeaseErrorKind::LeaseExists
                } else {
                    GrantLeaseErrorKind::Unknown
                }
            }
            tonic::Code::DataLoss => GrantLeaseErrorKind::DataLoss,
            tonic::Code::Unavailable => GrantLeaseErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => GrantLeaseErrorKind::Timeout,
            _ => GrantLeaseErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

/// An enumeration of the [`kind`][RevokeLeaseError::kind]s of errors that can occur from a
/// [`revoke_lease`][Client::revoke_lease] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevokeLeaseErrorKind {
    /// The lease was not found.
    ///
    /// This comes from the gRPC API as `NOT_FOUND`.
    NotFound,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and `INVALID_ARGUMENT` when the argument
    /// describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a [`revoke_lease`][Client::revoke_lease] operation.
    pub struct RevokeLeaseError(RevokeLeaseErrorKind);
}

impl RevokeLeaseError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::NotFound => RevokeLeaseErrorKind::NotFound,
            tonic::Code::Unauthenticated => RevokeLeaseErrorKind::Authentication,
            tonic::Code::PermissionDenied => RevokeLeaseErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => RevokeLeaseErrorKind::Authentication,
            tonic::Code::ResourceExhausted => RevokeLeaseErrorKind::Exhausted,
            tonic::Code::DataLoss => RevokeLeaseErrorKind::DataLoss,
            tonic::Code::Unavailable => RevokeLeaseErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => RevokeLeaseErrorKind::Timeout,
            _ => RevokeLeaseErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

// -------------------------------------------------------------------------------------------------
// Keep-alive
// -------------------------------------------------------------------------------------------------

/// The response from a single lease keep-alive round-trip.
#[derive(Clone, Copy, Debug)]
pub struct KeepAliveResponse {
    header: ResponseHeader,
    info: LeaseInfo,
}

impl KeepAliveResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The lease information, including the ID and the remaining TTL.
    ///
    /// A [`ttl`][LeaseInfo::ttl] of [`None`] indicates the lease has expired.
    pub fn info(&self) -> &LeaseInfo {
        &self.info
    }
}

impl Deref for KeepAliveResponse {
    type Target = LeaseInfo;

    fn deref(&self) -> &LeaseInfo {
        &self.info
    }
}

/// An active lease keep-alive stream.
///
/// Created by [`Client::lease_keeper`]. A `LeaseKeeper` combines a [`KeepAliveStream`] (for
/// receiving responses) and a [`KeepAliveSender`] (for sending keep-alive requests).
///
/// `LeaseKeeper` dereferences to [`KeepAliveStream`], so it can be used directly as a [`Stream`]:
///
/// ```no_run
/// use futures::StreamExt;
/// # async {
/// # let client: etcdrs::Client = todo!();
/// # let lease_id: etcdrs::LeaseId = todo!();
/// let keeper = client.lease_keeper();
/// keeper.keep_alive(lease_id);
/// while let Some(resp) = keeper.next().await {
///     let resp = resp.unwrap();
///     println!("lease {:?} TTL: {:?}", resp.lease_id, resp.ttl);
/// }
/// # };
/// ```
///
/// Use [`into_parts`][Self::into_parts] to split the keeper when you need to send keep-alive
/// requests concurrently with consuming the stream (e.g. from separate tasks).
///
/// ## Cleanup
///
/// Dropping a `LeaseKeeper` closes the underlying gRPC stream. Leases that are no longer
/// refreshed will expire naturally after their TTL.
pub struct LeaseKeeper {
    sender: KeepAliveSender,
    stream: KeepAliveStream,
}

impl LeaseKeeper {
    /// Split the `LeaseKeeper` into its sender and stream halves.
    ///
    /// This is useful when you need to send keep-alive requests from a different context than
    /// the one consuming responses.
    pub fn into_parts(self) -> (KeepAliveSender, KeepAliveStream) {
        (self.sender, self.stream)
    }

    /// Send a keep-alive request for the given lease.
    ///
    /// See [`KeepAliveSender::keep_alive`] for details.
    pub fn keep_alive(&self, lease_id: LeaseId) {
        self.sender.keep_alive(lease_id);
    }
}

impl Deref for LeaseKeeper {
    type Target = KeepAliveStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for LeaseKeeper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl Stream for LeaseKeeper {
    type Item = Result<KeepAliveResponse, KeepAliveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl std::async_iter::AsyncIterator for LeaseKeeper {
    type Item = Result<KeepAliveResponse, KeepAliveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// The control half of a [`LeaseKeeper`], used to send keep-alive requests.
///
/// Obtained via [`LeaseKeeper::into_parts`]. All methods send requests through the shared gRPC
/// stream. Because the server processes these asynchronously, there is a window between calling
/// [`keep_alive`][Self::keep_alive] and the server responding.
pub struct KeepAliveSender {
    sender: tokio::sync::mpsc::UnboundedSender<etcdserverpb::LeaseKeepAliveRequest>,
}

impl KeepAliveSender {
    /// Send a keep-alive request for the given lease.
    ///
    /// The server will respond with a [`KeepAliveResponse`] containing the lease's remaining TTL.
    /// A [`ttl`][LeaseInfo::ttl] of [`None`] indicates the lease has already expired.
    pub fn keep_alive(&self, lease_id: LeaseId) {
        let _ = self
            .sender
            .send(etcdserverpb::LeaseKeepAliveRequest { id: lease_id.get() });
    }
}

/// The response stream half of a [`LeaseKeeper`].
///
/// Obtained via [`LeaseKeeper::into_parts`]. Implements [`Stream`] — use
/// [`StreamExt::next`][futures_core::Stream] or `for await` to consume responses.
pub struct KeepAliveStream {
    inner: Box<dyn Stream<Item = Result<KeepAliveResponse, KeepAliveError>> + Send>,
}

impl KeepAliveStream {
    fn poll_next_impl(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<Option<KeepAliveResponse>, KeepAliveError>> {
        let inner = unsafe { self.map_unchecked_mut(|s| s.inner.as_mut()) };
        inner.poll_next(cx).map(|item| match item {
            None => Ok(None),
            Some(Ok(resp)) => Ok(Some(resp)),
            Some(Err(err)) => Err(err),
        })
    }
}

impl Stream for KeepAliveStream {
    type Item = Result<KeepAliveResponse, KeepAliveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl std::async_iter::AsyncIterator for KeepAliveStream {
    type Item = Result<KeepAliveResponse, KeepAliveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

/// Wraps a shared [`tokio::sync::mpsc::UnboundedReceiver`] as a [`Stream`].
///
/// The receiver is behind an `Arc<Mutex<>>` so that the stream can be reconstructed on connection
/// retry without losing messages buffered in the channel.
struct KeepAliveReceiverStream {
    inner: Arc<std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<etcdserverpb::LeaseKeepAliveRequest>>>,
}

impl Stream for KeepAliveReceiverStream {
    type Item = etcdserverpb::LeaseKeepAliveRequest;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.lock().unwrap().poll_recv(cx)
    }
}

/// An enumeration of the [`kind`][KeepAliveError::kind]s of errors that can occur from a
/// keep-alive operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeepAliveErrorKind {
    /// The lease was not found.
    ///
    /// This comes from the gRPC API as `NOT_FOUND`.
    NotFound,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and `INVALID_ARGUMENT` when the argument
    /// describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a keep-alive operation.
    pub struct KeepAliveError(KeepAliveErrorKind);
}

impl KeepAliveError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::NotFound => KeepAliveErrorKind::NotFound,
            tonic::Code::Unauthenticated => KeepAliveErrorKind::Authentication,
            tonic::Code::PermissionDenied => KeepAliveErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => KeepAliveErrorKind::Authentication,
            tonic::Code::ResourceExhausted => KeepAliveErrorKind::Exhausted,
            tonic::Code::DataLoss => KeepAliveErrorKind::DataLoss,
            tonic::Code::Unavailable => KeepAliveErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => KeepAliveErrorKind::Timeout,
            _ => KeepAliveErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<GrantLeaseFuture>();
        _assert_send::<KeepAliveSender>();
        _assert_send::<KeepAliveStream>();
        _assert_send::<LeaseKeeper>();
    }
};
