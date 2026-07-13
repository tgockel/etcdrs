use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
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
    pub fn revoke_lease(&self, lease_id: LeaseId) -> RevokeLease<Self> {
        RevokeLease::new(lease_id).with_client(self.clone())
    }

    /// Retrieve the remaining time-to-live of a lease.
    ///
    /// A missing or expired lease is reported as a successful response with no remaining
    /// [`ttl`][LeaseInfo::ttl]. Use [`with_keys`][LeaseTimeToLive::with_keys] to also fetch the
    /// keys attached to the lease.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// # let lease_id: etcdrs::LeaseId = todo!();
    /// let response = client.lease_time_to_live(lease_id).await.unwrap();
    /// match response.ttl {
    ///     Some(remaining) => println!("lease expires in {remaining:?}"),
    ///     None => println!("lease has expired"),
    /// }
    /// # };
    /// ```
    pub fn lease_time_to_live(&self, lease_id: LeaseId) -> LeaseTimeToLive<Self> {
        LeaseTimeToLive::new(lease_id).with_client(self.clone())
    }

    /// List the IDs of all active leases in the cluster.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// for lease_id in client.leases().await.unwrap().leases() {
    ///     println!("active lease: {lease_id:?}");
    /// }
    /// # };
    /// ```
    pub fn leases(&self) -> Leases<Self> {
        Leases::new().with_client(self.clone())
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
        crate::driver::LeaseDriver::start_lease_keeper(self.clone())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LeaseInfo {
    pub lease_id: LeaseId,
    /// The remaining time-to-live on the lease, or `None` if the lease has expired.
    pub ttl: Option<Duration>,
}

/// A [`Client::grant_lease`] operation.
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

    /// The requested lease ID, or `None` when etcd should choose one.
    pub fn requested_lease_id(&self) -> Option<LeaseId> {
        LeaseId::new(self.request.id)
    }

    /// The requested lease TTL, or `None` when the server default is used.
    pub fn requested_ttl(&self) -> Option<Duration> {
        (self.request.ttl > 0).then(|| Duration::from_secs(self.request.ttl as _))
    }

    pub(crate) fn into_parts(self) -> (C, GrantLease<()>) {
        (
            self.client,
            GrantLease {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The response from a [`grant_lease`][Client::grant_lease] operation.
#[derive(Clone, Copy, Debug)]
pub struct GrantLeaseResponse {
    header: ResponseHeader,
    info: LeaseInfo,
}

impl GrantLeaseResponse {
    pub(crate) fn new(header: ResponseHeader, info: LeaseInfo) -> Self {
        Self { header, info }
    }

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
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The [`Future`] type returned by awaiting a [`grant_lease`][`Client::grant_lease`].
pub struct GrantLeaseFuture(Pin<Box<dyn Future<Output = Result<GrantLeaseResponse, GrantLeaseError>> + Send>>);

impl GrantLeaseFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<GrantLeaseResponse, GrantLeaseError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for GrantLeaseFuture {
    type Output = Result<GrantLeaseResponse, GrantLeaseError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::LeaseDriver> IntoFuture for GrantLease<C> {
    type Output = Result<GrantLeaseResponse, GrantLeaseError>;
    type IntoFuture = C::GrantFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_grant_lease(detached)
    }
}

/// A [`Client::revoke_lease`] operation.
#[derive(Clone)]
#[must_use = "RevokeLease does nothing unless you `await` it"]
pub struct RevokeLease<C> {
    client: C,
    pub(crate) lease_id: LeaseId,
}

impl RevokeLease<()> {
    pub fn new(lease_id: LeaseId) -> Self {
        Self { client: (), lease_id }
    }
}

impl<C> RevokeLease<C> {
    pub fn with_client<C2>(self, client: C2) -> RevokeLease<C2> {
        RevokeLease {
            client,
            lease_id: self.lease_id,
        }
    }

    pub fn lease_id(&self) -> LeaseId {
        self.lease_id
    }

    pub(crate) fn into_parts(self) -> (C, RevokeLease<()>) {
        (
            self.client,
            RevokeLease {
                client: (),
                lease_id: self.lease_id,
            },
        )
    }
}

pub struct RevokeLeaseFuture(Pin<Box<dyn Future<Output = Result<RevokeLeaseResponse, RevokeLeaseError>> + Send>>);

impl RevokeLeaseFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<RevokeLeaseResponse, RevokeLeaseError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RevokeLeaseFuture {
    type Output = Result<RevokeLeaseResponse, RevokeLeaseError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::LeaseDriver> IntoFuture for RevokeLease<C> {
    type Output = Result<RevokeLeaseResponse, RevokeLeaseError>;
    type IntoFuture = C::RevokeFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_revoke_lease(detached)
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
// Time-to-live
// -------------------------------------------------------------------------------------------------

/// A [`Client::lease_time_to_live`] operation.
#[derive(Clone, Debug)]
#[must_use = "LeaseTimeToLive does nothing unless you `await` it"]
pub struct LeaseTimeToLive<C, K = ()> {
    client: C,
    pub(crate) request: etcdserverpb::LeaseTimeToLiveRequest,
    _keys: PhantomData<fn() -> K>,
}

impl LeaseTimeToLive<()> {
    pub fn new(lease_id: LeaseId) -> Self {
        Self {
            client: (),
            request: etcdserverpb::LeaseTimeToLiveRequest {
                id: lease_id.get(),
                ..Default::default()
            },
            _keys: PhantomData,
        }
    }
}

impl<C, K> LeaseTimeToLive<C, K> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`lease_time_to_live`][`Client::lease_time_to_live`].
    pub fn with_client<C2>(self, client: C2) -> LeaseTimeToLive<C2, K> {
        LeaseTimeToLive {
            client,
            request: self.request,
            _keys: PhantomData,
        }
    }

    /// Return the keys attached to the lease.
    pub fn with_keys(self) -> LeaseTimeToLive<C, WithKeys> {
        let mut request = self.request;
        request.keys = true;
        LeaseTimeToLive {
            client: self.client,
            request,
            _keys: PhantomData,
        }
    }

    /// The lease this operation queries.
    pub fn lease_id(&self) -> LeaseId {
        LeaseId::new(self.request.id).expect("LeaseTimeToLive lease ID should be non-zero")
    }

    /// Whether the keys attached to the lease will be returned.
    pub fn returns_keys(&self) -> bool {
        self.request.keys
    }

    pub(crate) fn into_parts(self) -> (C, LeaseTimeToLive<(), K>) {
        (
            self.client,
            LeaseTimeToLive {
                client: (),
                request: self.request,
                _keys: PhantomData,
            },
        )
    }
}

/// The response from a [`lease_time_to_live`][Client::lease_time_to_live] operation.
#[derive(Clone, Debug)]
pub struct LeaseTimeToLiveResponse<K = ()> {
    header: ResponseHeader,
    info: LeaseInfo,
    granted_ttl: Option<Duration>,
    keys: Vec<Bytes>,
    _marker: PhantomData<fn() -> K>,
}

impl<K> LeaseTimeToLiveResponse<K> {
    /// Construct a new `LeaseTimeToLiveResponse`.
    pub fn new(header: ResponseHeader, info: LeaseInfo, granted_ttl: Option<Duration>, keys: Vec<Bytes>) -> Self {
        Self {
            header,
            info,
            granted_ttl,
            keys,
            _marker: PhantomData,
        }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The lease information, including the ID and the remaining TTL.
    ///
    /// A [`ttl`][LeaseInfo::ttl] of [`None`] indicates the lease has expired or does not exist.
    pub fn info(&self) -> &LeaseInfo {
        &self.info
    }

    /// The TTL originally granted to the lease, or `None` if the lease has expired or does not
    /// exist.
    pub fn granted_ttl(&self) -> Option<Duration> {
        self.granted_ttl
    }
}

impl LeaseTimeToLiveResponse<WithKeys> {
    /// The keys attached to the lease.
    pub fn keys(&self) -> &[Bytes] {
        &self.keys
    }

    /// Consume the response and return the keys attached to the lease.
    pub fn into_keys(self) -> Vec<Bytes> {
        self.keys
    }
}

impl<K> Deref for LeaseTimeToLiveResponse<K> {
    type Target = LeaseInfo;

    fn deref(&self) -> &LeaseInfo {
        &self.info
    }
}

/// The [`Future`] type returned by awaiting a [`lease_time_to_live`][`Client::lease_time_to_live`].
pub struct LeaseTimeToLiveFuture<T>(Pin<Box<dyn Future<Output = T> + Send>>);

impl<T> LeaseTimeToLiveFuture<T> {
    pub(crate) fn new(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl<T> Future for LeaseTimeToLiveFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C, K> IntoFuture for LeaseTimeToLive<C, K>
where
    C: crate::driver::LeaseDriver,
{
    type Output = Result<LeaseTimeToLiveResponse<K>, LeaseTimeToLiveError>;
    type IntoFuture = C::TimeToLiveFuture<K>;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_lease_time_to_live(detached)
    }
}

/// Used in [`LeaseTimeToLive`]s to denote that the keys attached to the lease should be returned.
pub struct WithKeys;

/// An enumeration of the [`kind`][LeaseTimeToLiveError::kind]s of errors that can occur from a
/// [`lease_time_to_live`][Client::lease_time_to_live] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseTimeToLiveErrorKind {
    /// The lease was not found.
    ///
    /// This comes from the gRPC API as `NOT_FOUND`. Only very old etcd servers report a missing
    /// lease this way; modern servers respond successfully with no remaining
    /// [`ttl`][LeaseInfo::ttl].
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
    /// An error from a [`lease_time_to_live`][Client::lease_time_to_live] operation.
    pub struct LeaseTimeToLiveError(LeaseTimeToLiveErrorKind);
}

impl LeaseTimeToLiveError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::NotFound => LeaseTimeToLiveErrorKind::NotFound,
            tonic::Code::Unauthenticated => LeaseTimeToLiveErrorKind::Authentication,
            tonic::Code::PermissionDenied => LeaseTimeToLiveErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => LeaseTimeToLiveErrorKind::Authentication,
            tonic::Code::ResourceExhausted => LeaseTimeToLiveErrorKind::Exhausted,
            tonic::Code::DataLoss => LeaseTimeToLiveErrorKind::DataLoss,
            tonic::Code::Unavailable => LeaseTimeToLiveErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => LeaseTimeToLiveErrorKind::Timeout,
            _ => LeaseTimeToLiveErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

// -------------------------------------------------------------------------------------------------
// Lease listing
// -------------------------------------------------------------------------------------------------

/// A [`Client::leases`] operation.
#[derive(Clone)]
#[must_use = "Leases does nothing unless you `await` it"]
pub struct Leases<C> {
    client: C,
}

impl Leases<()> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { client: () }
    }
}

impl<C> Leases<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`leases`][`Client::leases`].
    pub fn with_client<C2>(self, client: C2) -> Leases<C2> {
        Leases { client }
    }

    pub(crate) fn into_parts(self) -> (C, Leases<()>) {
        (self.client, Leases { client: () })
    }
}

/// The response from a [`leases`][Client::leases] operation.
#[derive(Clone, Debug)]
pub struct LeasesResponse {
    header: ResponseHeader,
    leases: Vec<LeaseId>,
}

impl LeasesResponse {
    pub(crate) fn new(header: ResponseHeader, leases: Vec<LeaseId>) -> Self {
        Self { header, leases }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The IDs of all active leases in the cluster.
    pub fn leases(&self) -> &[LeaseId] {
        &self.leases
    }

    /// Consume the response and return the lease IDs.
    pub fn into_leases(self) -> Vec<LeaseId> {
        self.leases
    }
}

/// The [`Future`] type returned by awaiting a [`leases`][`Client::leases`].
pub struct LeasesFuture(Pin<Box<dyn Future<Output = Result<LeasesResponse, LeasesError>> + Send>>);

impl LeasesFuture {
    pub(crate) fn new(future: impl Future<Output = Result<LeasesResponse, LeasesError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for LeasesFuture {
    type Output = Result<LeasesResponse, LeasesError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::LeaseDriver> IntoFuture for Leases<C> {
    type Output = Result<LeasesResponse, LeasesError>;
    type IntoFuture = C::LeasesFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_leases(detached)
    }
}

/// An enumeration of the [`kind`][LeasesError::kind]s of errors that can occur from a
/// [`leases`][Client::leases] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeasesErrorKind {
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
    /// An error from a [`leases`][Client::leases] operation.
    pub struct LeasesError(LeasesErrorKind);
}

impl LeasesError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::Unauthenticated => LeasesErrorKind::Authentication,
            tonic::Code::PermissionDenied => LeasesErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => LeasesErrorKind::Authentication,
            tonic::Code::ResourceExhausted => LeasesErrorKind::Exhausted,
            tonic::Code::DataLoss => LeasesErrorKind::DataLoss,
            tonic::Code::Unavailable => LeasesErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => LeasesErrorKind::Timeout,
            _ => LeasesErrorKind::Unknown,
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
    pub(crate) fn new(header: ResponseHeader, info: LeaseInfo) -> Self {
        Self { header, info }
    }

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
    pub(crate) fn new(sender: KeepAliveSender, stream: KeepAliveStream) -> Self {
        Self { sender, stream }
    }

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
    pub(crate) fn new(sender: tokio::sync::mpsc::UnboundedSender<etcdserverpb::LeaseKeepAliveRequest>) -> Self {
        Self { sender }
    }

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
    pub(crate) fn new(inner: impl Stream<Item = Result<KeepAliveResponse, KeepAliveError>> + Send + 'static) -> Self {
        Self { inner: Box::new(inner) }
    }

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
pub(crate) struct KeepAliveReceiverStream {
    pub(crate) inner: Arc<std::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<etcdserverpb::LeaseKeepAliveRequest>>>,
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
        _assert_send::<LeaseTimeToLiveFuture<Result<LeaseTimeToLiveResponse, LeaseTimeToLiveError>>>();
        _assert_send::<LeaseTimeToLiveFuture<Result<LeaseTimeToLiveResponse<WithKeys>, LeaseTimeToLiveError>>>();
        _assert_send::<LeasesFuture>();
    }
};
