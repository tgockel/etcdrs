use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{pb::etcdserverpb, Client, LeaseId};

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
    /// std::thread::sleep(lease_info.ttl);
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
    pub async fn revoke_lease(&self, lease_id: LeaseId) -> Result<(), RevokeLeaseError> {
        self.inner
            .wrap_unary_call(
                etcdserverpb::lease_client::LeaseClient::new,
                async |c, r| c.lease_revoke(r).await,
                etcdserverpb::LeaseRevokeRequest { id: lease_id.get() },
            )
            .await
            .map(|_| ())
            .map_err(RevokeLeaseError::from_status)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LeaseInfo {
    pub lease_id: LeaseId,
    pub ttl: Duration,
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
    async fn call(self) -> Result<LeaseInfo, GrantLeaseError> {
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
            return Err(GrantLeaseError::new(
                GrantLeaseErrorKind::LeaseExists,
                resp.error,
                None,
            ));
        }

        let lease_id = LeaseId::new(resp.id).expect("etcd server should have returned a lease");
        let ttl = Duration::from_secs(resp.ttl as _);

        Ok(LeaseInfo { lease_id, ttl })
    }
}

/// The [`Future`] type returned by awaiting a [`grant_lease`][`Client::grant_lease`].
pub struct GrantLeaseFuture(Pin<Box<dyn Future<Output = Result<LeaseInfo, GrantLeaseError>> + Send>>);

impl Future for GrantLeaseFuture {
    type Output = Result<LeaseInfo, GrantLeaseError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for GrantLease<Client> {
    type Output = Result<LeaseInfo, GrantLeaseError>;
    type IntoFuture = GrantLeaseFuture;

    fn into_future(self) -> Self::IntoFuture {
        GrantLeaseFuture(Box::pin(self.call()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GrantLeaseErrorKind {
    /// The requested lease ID already exists.
    LeaseExists,
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`grant_lease`][Client::grant_lease] operation.
    pub struct GrantLeaseError(GrantLeaseErrorKind);
}

impl GrantLeaseError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::FailedPrecondition => GrantLeaseErrorKind::LeaseExists,
            _ => GrantLeaseErrorKind::Transport,
        };
        Self::new(kind, "", Some(status))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RevokeLeaseErrorKind {
    /// The lease was not found.
    NotFound,
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`revoke_lease`][Client::revoke_lease] operation.
    pub struct RevokeLeaseError(RevokeLeaseErrorKind);
}

impl RevokeLeaseError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::NotFound => RevokeLeaseErrorKind::NotFound,
            _ => RevokeLeaseErrorKind::Transport,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<GrantLeaseFuture>();
    }
};
