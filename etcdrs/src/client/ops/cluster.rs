use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Client, ClusterResponseHeader, MemberId, pb::etcdserverpb};

/// # Cluster Membership
impl Client {
    /// List the members of the etcd cluster.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let resp = client.member_list().await.expect("failed to list members");
    /// for member in resp.members() {
    ///     println!("member {:?} ({}): peers={:?}", member.id(), member.name(), member.peer_urls());
    /// }
    /// # };
    /// ```
    pub fn member_list(&self) -> MemberList<Self> {
        MemberList::new().with_client(self.clone())
    }

    /// Add a new member to the etcd cluster.
    ///
    /// `peer_urls` are the URLs the new member will advertise to other peers. The new member is
    /// added in voting mode by default; use [`is_learner`][MemberAdd::is_learner] to add it as a
    /// non-voting learner that can later be promoted with
    /// [`member_promote`][Client::member_promote].
    ///
    /// The new etcd peer process must subsequently start and join the cluster on its own; this
    /// call only registers the member with the existing cluster.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let resp = client.member_add(["http://10.0.0.5:2380"])
    ///     .is_learner(true)
    ///     .await
    ///     .expect("failed to add member");
    /// println!("new member ID: {:?}", resp.member().id());
    /// # };
    /// ```
    pub fn member_add(&self, peer_urls: impl IntoIterator<Item = impl Into<String>>) -> MemberAdd<Self> {
        MemberAdd::new(peer_urls).with_client(self.clone())
    }

    /// Remove a member from the etcd cluster.
    pub fn member_remove(&self, member_id: MemberId) -> MemberRemove<Self> {
        MemberRemove::new(member_id).with_client(self.clone())
    }

    /// Update the peer URLs of an existing member.
    pub fn member_update(
        &self,
        member_id: MemberId,
        peer_urls: impl IntoIterator<Item = impl Into<String>>,
    ) -> MemberUpdate<Self> {
        MemberUpdate::new(member_id, peer_urls).with_client(self.clone())
    }

    /// Promote a learner member to a voting member.
    ///
    /// The learner must be in sync with the cluster leader; otherwise the call fails with
    /// [`ClusterErrorKind::LearnerNotReady`]. If the member is already a voter, the call fails
    /// with [`ClusterErrorKind::MemberNotLearner`].
    pub fn member_promote(&self, member_id: MemberId) -> MemberPromote<Self> {
        MemberPromote::new(member_id).with_client(self.clone())
    }
}

// -------------------------------------------------------------------------------------------------
// Member
// -------------------------------------------------------------------------------------------------

/// A single member of an etcd cluster.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Member {
    id: MemberId,
    name: String,
    peer_urls: Vec<String>,
    client_urls: Vec<String>,
    is_learner: bool,
}

impl Member {
    pub(crate) fn from_pb(pb: etcdserverpb::Member) -> Self {
        Self {
            id: MemberId::new(pb.id).expect("Member should have a non-zero ID"),
            name: pb.name,
            peer_urls: pb.peer_ur_ls,
            client_urls: pb.client_ur_ls,
            is_learner: pb.is_learner,
        }
    }

    /// The unique identifier of this member.
    pub fn id(&self) -> MemberId {
        self.id
    }

    /// The human-readable name of this member.
    ///
    /// This is empty until a newly-added member has fully joined the cluster.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The URLs this member advertises for peer-to-peer (raft) communication.
    pub fn peer_urls(&self) -> &[String] {
        &self.peer_urls
    }

    /// The URLs this member advertises for client (gRPC) connections.
    ///
    /// This is empty until a newly-added member has fully joined the cluster.
    pub fn client_urls(&self) -> &[String] {
        &self.client_urls
    }

    /// Whether this member is a non-voting learner.
    pub fn is_learner(&self) -> bool {
        self.is_learner
    }
}

// -------------------------------------------------------------------------------------------------
// member_list
// -------------------------------------------------------------------------------------------------

/// A [`Client::member_list`] operation.
#[derive(Clone)]
#[must_use = "MemberList does nothing unless you `await` it"]
pub struct MemberList<C> {
    client: C,
    pub(crate) request: etcdserverpb::MemberListRequest,
}

impl MemberList<()> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            client: (),
            request: etcdserverpb::MemberListRequest::default(),
        }
    }
}

impl<C> MemberList<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`member_list`][`Client::member_list`].
    pub fn with_client<C2>(self, client: C2) -> MemberList<C2> {
        MemberList {
            client,
            request: self.request,
        }
    }

    /// Request a linearizable read of the member list.
    ///
    /// By default the member list is read from the local member's view, which may be slightly
    /// stale. A linearizable read goes through the raft leader, ensuring the result reflects the
    /// most recent membership change.
    pub fn linearizable(mut self) -> Self {
        self.request.linearizable = true;
        self
    }

    /// Whether this operation requests a linearizable member list.
    pub fn is_linearizable(&self) -> bool {
        self.request.linearizable
    }

    pub(crate) fn into_parts(self) -> (C, MemberList<()>) {
        (
            self.client,
            MemberList {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The [`Future`] type returned by awaiting a [`member_list`][Client::member_list].
pub struct MemberListFuture(Pin<Box<dyn Future<Output = Result<MemberListResponse, ClusterError>> + Send>>);

impl MemberListFuture {
    pub(crate) fn new(future: impl Future<Output = Result<MemberListResponse, ClusterError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for MemberListFuture {
    type Output = Result<MemberListResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::ClusterDriver> IntoFuture for MemberList<C> {
    type Output = Result<MemberListResponse, ClusterError>;
    type IntoFuture = C::MemberListFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_member_list(detached)
    }
}

/// The response from a [`member_list`][Client::member_list] operation.
#[derive(Clone, Debug)]
pub struct MemberListResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberListResponse {
    pub(crate) fn new(header: ClusterResponseHeader, members: Vec<Member>) -> Self {
        Self { header, members }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ClusterResponseHeader {
        &self.header
    }

    /// The members of the cluster.
    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// Consume the response and return the member list.
    pub fn into_members(self) -> Vec<Member> {
        self.members
    }
}

// -------------------------------------------------------------------------------------------------
// member_add
// -------------------------------------------------------------------------------------------------

/// A [`Client::member_add`] operation.
#[derive(Clone)]
#[must_use = "MemberAdd does nothing unless you `await` it"]
pub struct MemberAdd<C> {
    client: C,
    pub(crate) request: etcdserverpb::MemberAddRequest,
}

impl MemberAdd<()> {
    pub fn new(peer_urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: (),
            request: etcdserverpb::MemberAddRequest {
                peer_ur_ls: peer_urls.into_iter().map(Into::into).collect(),
                is_learner: false,
            },
        }
    }
}

impl<C> MemberAdd<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`member_add`][`Client::member_add`].
    pub fn with_client<C2>(self, client: C2) -> MemberAdd<C2> {
        MemberAdd {
            client,
            request: self.request,
        }
    }

    /// Add the member as a non-voting learner.
    ///
    /// Learners receive cluster updates but do not participate in voting until they are promoted
    /// with [`member_promote`][Client::member_promote]. This is the recommended way to add a new
    /// member, as it lets the new node catch up before affecting cluster availability.
    pub fn is_learner(mut self, is_learner: bool) -> Self {
        self.request.is_learner = is_learner;
        self
    }

    /// The peer URLs to register for the new member.
    pub fn peer_urls(&self) -> &[String] {
        &self.request.peer_ur_ls
    }

    /// Whether the new member will be registered as a learner.
    pub fn is_learner_member(&self) -> bool {
        self.request.is_learner
    }

    pub(crate) fn into_parts(self) -> (C, MemberAdd<()>) {
        (
            self.client,
            MemberAdd {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The [`Future`] type returned by awaiting a [`member_add`][Client::member_add].
pub struct MemberAddFuture(Pin<Box<dyn Future<Output = Result<MemberAddResponse, ClusterError>> + Send>>);

impl MemberAddFuture {
    pub(crate) fn new(future: impl Future<Output = Result<MemberAddResponse, ClusterError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for MemberAddFuture {
    type Output = Result<MemberAddResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::ClusterDriver> IntoFuture for MemberAdd<C> {
    type Output = Result<MemberAddResponse, ClusterError>;
    type IntoFuture = C::MemberAddFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_member_add(detached)
    }
}

/// The response from a [`member_add`][Client::member_add] operation.
#[derive(Clone, Debug)]
pub struct MemberAddResponse {
    header: ClusterResponseHeader,
    member: Member,
    members: Vec<Member>,
}

impl MemberAddResponse {
    pub(crate) fn new(header: ClusterResponseHeader, member: Member, members: Vec<Member>) -> Self {
        Self {
            header,
            member,
            members,
        }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ClusterResponseHeader {
        &self.header
    }

    /// The newly-added member.
    pub fn member(&self) -> &Member {
        &self.member
    }

    /// All members of the cluster after the addition.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

// -------------------------------------------------------------------------------------------------
// member_remove / member_update / member_promote
// -------------------------------------------------------------------------------------------------

/// A [`Client::member_remove`] operation.
#[derive(Clone)]
#[must_use = "MemberRemove does nothing unless you `await` it"]
pub struct MemberRemove<C> {
    client: C,
    pub(crate) request: etcdserverpb::MemberRemoveRequest,
}

impl MemberRemove<()> {
    pub fn new(member_id: MemberId) -> Self {
        Self {
            client: (),
            request: etcdserverpb::MemberRemoveRequest { id: member_id.get() },
        }
    }
}

impl<C> MemberRemove<C> {
    pub fn with_client<C2>(self, client: C2) -> MemberRemove<C2> {
        MemberRemove {
            client,
            request: self.request,
        }
    }

    pub fn member_id(&self) -> MemberId {
        MemberId::new(self.request.id).expect("member remove request should have a member ID")
    }

    pub(crate) fn into_parts(self) -> (C, MemberRemove<()>) {
        (
            self.client,
            MemberRemove {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct MemberRemoveFuture(Pin<Box<dyn Future<Output = Result<MemberRemoveResponse, ClusterError>> + Send>>);

impl MemberRemoveFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<MemberRemoveResponse, ClusterError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for MemberRemoveFuture {
    type Output = Result<MemberRemoveResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::ClusterDriver> IntoFuture for MemberRemove<C> {
    type Output = Result<MemberRemoveResponse, ClusterError>;
    type IntoFuture = C::MemberRemoveFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_member_remove(detached)
    }
}

/// A [`Client::member_update`] operation.
#[derive(Clone)]
#[must_use = "MemberUpdate does nothing unless you `await` it"]
pub struct MemberUpdate<C> {
    client: C,
    pub(crate) request: etcdserverpb::MemberUpdateRequest,
}

impl MemberUpdate<()> {
    pub fn new(member_id: MemberId, peer_urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: (),
            request: etcdserverpb::MemberUpdateRequest {
                id: member_id.get(),
                peer_ur_ls: peer_urls.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl<C> MemberUpdate<C> {
    pub fn with_client<C2>(self, client: C2) -> MemberUpdate<C2> {
        MemberUpdate {
            client,
            request: self.request,
        }
    }

    pub fn member_id(&self) -> MemberId {
        MemberId::new(self.request.id).expect("member update request should have a member ID")
    }

    pub fn peer_urls(&self) -> &[String] {
        &self.request.peer_ur_ls
    }

    pub(crate) fn into_parts(self) -> (C, MemberUpdate<()>) {
        (
            self.client,
            MemberUpdate {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct MemberUpdateFuture(Pin<Box<dyn Future<Output = Result<MemberUpdateResponse, ClusterError>> + Send>>);

impl MemberUpdateFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<MemberUpdateResponse, ClusterError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for MemberUpdateFuture {
    type Output = Result<MemberUpdateResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::ClusterDriver> IntoFuture for MemberUpdate<C> {
    type Output = Result<MemberUpdateResponse, ClusterError>;
    type IntoFuture = C::MemberUpdateFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_member_update(detached)
    }
}

/// A [`Client::member_promote`] operation.
#[derive(Clone)]
#[must_use = "MemberPromote does nothing unless you `await` it"]
pub struct MemberPromote<C> {
    client: C,
    pub(crate) request: etcdserverpb::MemberPromoteRequest,
}

impl MemberPromote<()> {
    pub fn new(member_id: MemberId) -> Self {
        Self {
            client: (),
            request: etcdserverpb::MemberPromoteRequest { id: member_id.get() },
        }
    }
}

impl<C> MemberPromote<C> {
    pub fn with_client<C2>(self, client: C2) -> MemberPromote<C2> {
        MemberPromote {
            client,
            request: self.request,
        }
    }

    pub fn member_id(&self) -> MemberId {
        MemberId::new(self.request.id).expect("member promote request should have a member ID")
    }

    pub(crate) fn into_parts(self) -> (C, MemberPromote<()>) {
        (
            self.client,
            MemberPromote {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct MemberPromoteFuture(Pin<Box<dyn Future<Output = Result<MemberPromoteResponse, ClusterError>> + Send>>);

impl MemberPromoteFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<MemberPromoteResponse, ClusterError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for MemberPromoteFuture {
    type Output = Result<MemberPromoteResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::ClusterDriver> IntoFuture for MemberPromote<C> {
    type Output = Result<MemberPromoteResponse, ClusterError>;
    type IntoFuture = C::MemberPromoteFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_member_promote(detached)
    }
}

// -------------------------------------------------------------------------------------------------
// member_remove / member_update / member_promote responses
// -------------------------------------------------------------------------------------------------

/// The response from a [`member_remove`][Client::member_remove] operation.
#[derive(Clone, Debug)]
pub struct MemberRemoveResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberRemoveResponse {
    pub(crate) fn new(header: ClusterResponseHeader, members: Vec<Member>) -> Self {
        Self { header, members }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ClusterResponseHeader {
        &self.header
    }

    /// The members of the cluster after the removal.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

/// The response from a [`member_update`][Client::member_update] operation.
#[derive(Clone, Debug)]
pub struct MemberUpdateResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberUpdateResponse {
    pub(crate) fn new(header: ClusterResponseHeader, members: Vec<Member>) -> Self {
        Self { header, members }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ClusterResponseHeader {
        &self.header
    }

    /// The members of the cluster after the update.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

/// The response from a [`member_promote`][Client::member_promote] operation.
#[derive(Clone, Debug)]
pub struct MemberPromoteResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberPromoteResponse {
    pub(crate) fn new(header: ClusterResponseHeader, members: Vec<Member>) -> Self {
        Self { header, members }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ClusterResponseHeader {
        &self.header
    }

    /// The members of the cluster after the promotion.
    pub fn members(&self) -> &[Member] {
        &self.members
    }
}

// -------------------------------------------------------------------------------------------------
// ClusterError
// -------------------------------------------------------------------------------------------------

/// An enumeration of the [`kind`][ClusterError::kind]s of errors that can occur from a cluster
/// management operation ([`member_list`][Client::member_list],
/// [`member_add`][Client::member_add], [`member_remove`][Client::member_remove],
/// [`member_update`][Client::member_update], [`member_promote`][Client::member_promote]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterErrorKind {
    /// The requested member was not found.
    ///
    /// This comes from the gRPC API as `NOT_FOUND` with "etcdserver: member not found".
    MemberNotFound,
    /// A member with the requested ID already exists.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "etcdserver: member ID already
    /// exist".
    MemberAlreadyExists,
    /// One or more of the requested peer URLs are already in use by another member.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "etcdserver: Peer URLs already
    /// exists".
    PeerUrlExists,
    /// Tried to promote a member that is not a learner.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "etcdserver: can only promote a
    /// learner member".
    MemberNotLearner,
    /// Tried to promote a learner that is not yet caught up with the cluster leader.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "etcdserver: can only promote a
    /// learner member which is in sync with leader".
    LearnerNotReady,
    /// The cluster is not healthy enough to satisfy this reconfiguration.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE` with "etcdserver: unhealthy cluster", or as
    /// `UNKNOWN` with "etcdserver: re-configuration failed due to not enough started members".
    UnhealthyCluster,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED` or `PERMISSION_DENIED`, or as
    /// `INVALID_ARGUMENT` with "etcdserver: user name is empty", "etcdserver: revision of auth
    /// store is old" or "etcdserver: invalid auth management". Other `INVALID_ARGUMENT` responses
    /// describe the request rather than the caller, and are reported as [`Unknown`][Self::Unknown].
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
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish
    /// between the two, as the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless
    /// the etcd server has changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a cluster management operation ([`member_list`][Client::member_list],
    /// [`member_add`][Client::member_add], [`member_remove`][Client::member_remove],
    /// [`member_update`][Client::member_update], [`member_promote`][Client::member_promote]).
    pub struct ClusterError(ClusterErrorKind);
}

impl ClusterError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match (status.code(), status.message()) {
            (tonic::Code::NotFound, _) => ClusterErrorKind::MemberNotFound,
            // etcd sends every rejected membership change as one of these exact strings
            // (`api/v3rpc/rpctypes/error.go`), so nothing weaker than an exact match is needed to
            // tell them apart -- and nothing weaker would separate "can only promote a learner
            // member" from "... which is in sync with leader", which is a different kind.
            //
            // "not enough started members" arrives as UNKNOWN rather than FAILED_PRECONDITION
            // because `toGRPCErrorMap` routes it through `rpctypes.ErrMemberNotEnoughStarted`,
            // which has no `GRPCStatus()` method for gRPC to read a code off. Accept it under
            // either code: the text is what identifies it, and FAILED_PRECONDITION is evidently
            // what etcd meant to send.
            (tonic::Code::FailedPrecondition | tonic::Code::Unknown, message) => match message {
                "etcdserver: member ID already exist" => ClusterErrorKind::MemberAlreadyExists,
                "etcdserver: Peer URLs already exists" => ClusterErrorKind::PeerUrlExists,
                "etcdserver: can only promote a learner member" => ClusterErrorKind::MemberNotLearner,
                "etcdserver: can only promote a learner member which is in sync with leader" => {
                    ClusterErrorKind::LearnerNotReady
                }
                "etcdserver: re-configuration failed due to not enough started members" => {
                    ClusterErrorKind::UnhealthyCluster
                }
                // "etcdserver: too many learner members in cluster" belongs here too, but the kind
                // it needs cannot be added to an exhaustive enum in a released 0.1.x without
                // breaking downstream matches. It stays `Unknown` until an incompatible release.
                _ => ClusterErrorKind::Unknown,
            },
            (tonic::Code::Unauthenticated, _) => ClusterErrorKind::Authentication,
            (tonic::Code::PermissionDenied, _) => ClusterErrorKind::Authentication,
            // The only INVALID_ARGUMENT statuses a cluster RPC reaches that describe the caller --
            // the same three `ClientInner::is_stale_token_error` keys on. The rest describe the
            // request, above all "etcdserver: given member URLs are invalid" for the peer URLs
            // `member_add` hands to `types.NewURLs`. Those fall through to `Unknown`, because the
            // kind that one wants cannot be added to an exhaustive enum in a released 0.1.x
            // without breaking downstream matches.
            (
                tonic::Code::InvalidArgument,
                "etcdserver: user name is empty"
                | "etcdserver: revision of auth store is old"
                | "etcdserver: invalid auth management",
            ) => ClusterErrorKind::Authentication,
            (tonic::Code::ResourceExhausted, _) => ClusterErrorKind::Exhausted,
            (tonic::Code::DataLoss, _) => ClusterErrorKind::DataLoss,
            // `ErrGRPCUnhealthy` is the one strict-reconfig refusal that keeps a code of its own,
            // and it shares UNAVAILABLE with an unreachable server, so the message is again the only
            // thing separating "this reconfiguration was refused" from "that server is not there".
            (tonic::Code::Unavailable, "etcdserver: unhealthy cluster") => ClusterErrorKind::UnhealthyCluster,
            (tonic::Code::Unavailable, _) => ClusterErrorKind::Unavailable,
            // Don't care who timed us out
            (tonic::Code::Cancelled | tonic::Code::DeadlineExceeded, _) => ClusterErrorKind::Timeout,
            _ => ClusterErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<MemberListFuture>();
        _assert_send::<MemberAddFuture>();
    }
};

#[cfg(test)]
mod test {
    use super::*;

    fn kind_of(code: tonic::Code, message: &str) -> ClusterErrorKind {
        ClusterError::from_status(tonic::Status::new(code, message)).kind()
    }

    /// Every rejection an etcd 3.7 server sends for a cluster-membership RPC, as observed against a
    /// live server. These are the whole reason the classifier matches on the message at all.
    #[test]
    fn etcd_membership_rejections_are_classified() {
        use ClusterErrorKind::*;
        use tonic::Code;

        for (code, message, expected) in [
            (Code::NotFound, "etcdserver: member not found", MemberNotFound),
            (
                Code::FailedPrecondition,
                "etcdserver: member ID already exist",
                MemberAlreadyExists,
            ),
            (
                Code::FailedPrecondition,
                "etcdserver: Peer URLs already exists",
                PeerUrlExists,
            ),
            (
                Code::FailedPrecondition,
                "etcdserver: can only promote a learner member",
                MemberNotLearner,
            ),
            (
                Code::FailedPrecondition,
                "etcdserver: can only promote a learner member which is in sync with leader",
                LearnerNotReady,
            ),
            (Code::Unavailable, "etcdserver: unhealthy cluster", UnhealthyCluster),
            (
                Code::Unknown,
                "etcdserver: re-configuration failed due to not enough started members",
                UnhealthyCluster,
            ),
        ] {
            assert_eq!(kind_of(code, message), expected, "{code:?} {message:?}");
        }
    }

    /// etcd refuses a learner past `--max-learners` with a message of its own, but giving it a kind
    /// of its own would add a variant to an exhaustive public enum that 0.1.0 already shipped. The
    /// message is pinned here so that the release which can add the kind only has to change the
    /// expectation.
    #[test]
    fn too_many_learners_has_no_kind_of_its_own_yet() {
        assert_eq!(
            kind_of(
                tonic::Code::FailedPrecondition,
                "etcdserver: too many learner members in cluster"
            ),
            ClusterErrorKind::Unknown
        );
    }

    /// `INVALID_ARGUMENT` is not answered on the code alone: only these three messages describe
    /// an authentication problem. "given member URLs are invalid" is the reason it cannot be --
    /// `member_add` forwards caller-supplied peer URLs, so it is the one a caller can actually
    /// provoke. It wants a kind of its own, which an exhaustive public enum shipped in 0.1.0
    /// cannot have, so it stays `Unknown` until an incompatible release.
    #[test]
    fn invalid_argument_is_authentication_only_for_auth_messages() {
        use ClusterErrorKind::*;

        for (message, expected) in [
            ("etcdserver: user name is empty", Authentication),
            ("etcdserver: revision of auth store is old", Authentication),
            ("etcdserver: invalid auth management", Authentication),
            ("etcdserver: given member URLs are invalid", Unknown),
            ("etcdserver: request is too large", Unknown),
            ("etcdserver: invalid client api version", Unknown),
        ] {
            assert_eq!(kind_of(tonic::Code::InvalidArgument, message), expected, "{message:?}");
        }
    }

    /// The not-enough-started-members refusal is also accepted under the code etcd meant to send
    /// it with, so a server that ever grows a `GRPCStatus()` for it keeps classifying.
    #[test]
    fn not_enough_started_members_is_accepted_under_either_code() {
        let message = "etcdserver: re-configuration failed due to not enough started members";
        assert_eq!(
            kind_of(tonic::Code::FailedPrecondition, message),
            ClusterErrorKind::UnhealthyCluster
        );
    }

    /// Matching that refusal by text alone is only safe if an ordinary `UNKNOWN` -- what a dropped
    /// connection looks like to tonic -- is left alone.
    #[test]
    fn unrelated_unknown_stays_unknown() {
        assert_eq!(
            kind_of(tonic::Code::Unknown, "transport error"),
            ClusterErrorKind::Unknown
        );
    }

    /// Likewise for `UNAVAILABLE`, which a refused reconfiguration shares with an unreachable
    /// server.
    #[test]
    fn unrelated_unavailable_stays_unavailable() {
        for message in ["transport error", "etcdserver: no leader"] {
            assert_eq!(
                kind_of(tonic::Code::Unavailable, message),
                ClusterErrorKind::Unavailable,
                "{message:?}"
            );
        }
    }

    /// Substrings that read like etcd errors but that etcd never sends -- among them the ones an
    /// earlier version of this classifier matched on.
    #[test]
    fn messages_etcd_never_sends_are_unknown() {
        for message in [
            "etcdserver: ID exists",
            "etcdserver: ID removed",
            "etcdserver: peerURL exists",
            "etcdserver: learner not ready",
            "etcdserver: not a learner",
        ] {
            assert_eq!(
                kind_of(tonic::Code::FailedPrecondition, message),
                ClusterErrorKind::Unknown,
                "{message:?}"
            );
        }
    }

    /// The codes answered on the code alone. The message is one that would classify under a
    /// different code, proving it is not consulted here.
    #[test]
    fn code_only_classifications() {
        use ClusterErrorKind::*;
        use tonic::Code;

        for (code, expected) in [
            (Code::Unauthenticated, Authentication),
            (Code::PermissionDenied, Authentication),
            (Code::ResourceExhausted, Exhausted),
            (Code::DataLoss, DataLoss),
            (Code::Cancelled, Timeout),
            (Code::DeadlineExceeded, Timeout),
            (Code::Internal, Unknown),
        ] {
            assert_eq!(kind_of(code, "etcdserver: unhealthy cluster"), expected, "{code:?}");
        }
    }
}
