use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Client, ClusterResponseHeader, MemberId, pb::etcdserverpb};

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
    pub async fn member_remove(&self, member_id: MemberId) -> Result<MemberRemoveResponse, ClusterError> {
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::cluster_client::ClusterClient::new,
                async |c, r| c.member_remove(r).await,
                etcdserverpb::MemberRemoveRequest { id: member_id.get() },
            )
            .await
            .map_err(ClusterError::from_status)?;
        let header =
            ClusterResponseHeader::from_pb(resp.header.expect("MemberRemoveResponse should have a valid header"));
        let members = resp.members.into_iter().map(Member::from_pb).collect();
        Ok(MemberRemoveResponse { header, members })
    }

    /// Update the peer URLs of an existing member.
    pub async fn member_update(
        &self,
        member_id: MemberId,
        peer_urls: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<MemberUpdateResponse, ClusterError> {
        let request = etcdserverpb::MemberUpdateRequest {
            id: member_id.get(),
            peer_ur_ls: peer_urls.into_iter().map(Into::into).collect(),
        };
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::cluster_client::ClusterClient::new,
                async |c, r| c.member_update(r).await,
                request,
            )
            .await
            .map_err(ClusterError::from_status)?;
        let header =
            ClusterResponseHeader::from_pb(resp.header.expect("MemberUpdateResponse should have a valid header"));
        let members = resp.members.into_iter().map(Member::from_pb).collect();
        Ok(MemberUpdateResponse { header, members })
    }

    /// Promote a learner member to a voting member.
    ///
    /// The learner must be in sync with the cluster leader; otherwise the call fails with
    /// [`ClusterErrorKind::LearnerNotReady`]. If the member is already a voter, the call fails
    /// with [`ClusterErrorKind::MemberNotLearner`].
    pub async fn member_promote(&self, member_id: MemberId) -> Result<MemberPromoteResponse, ClusterError> {
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::cluster_client::ClusterClient::new,
                async |c, r| c.member_promote(r).await,
                etcdserverpb::MemberPromoteRequest { id: member_id.get() },
            )
            .await
            .map_err(ClusterError::from_status)?;
        let header =
            ClusterResponseHeader::from_pb(resp.header.expect("MemberPromoteResponse should have a valid header"));
        let members = resp.members.into_iter().map(Member::from_pb).collect();
        Ok(MemberPromoteResponse { header, members })
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

/// A builder for the [`member_list`][Client::member_list] operation.
#[derive(Clone)]
#[must_use = "MemberList does nothing unless you `await` it"]
pub struct MemberList<C> {
    client: C,
    request: etcdserverpb::MemberListRequest,
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
}

impl MemberList<Client> {
    async fn call(self) -> Result<MemberListResponse, ClusterError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::cluster_client::ClusterClient::new,
                async |c, r| c.member_list(r).await,
                self.request,
            )
            .await
            .map_err(ClusterError::from_status)?;
        let header =
            ClusterResponseHeader::from_pb(resp.header.expect("MemberListResponse should have a valid header"));
        let members = resp.members.into_iter().map(Member::from_pb).collect();
        Ok(MemberListResponse { header, members })
    }
}

/// The [`Future`] type returned by awaiting a [`member_list`][Client::member_list].
pub struct MemberListFuture(Pin<Box<dyn Future<Output = Result<MemberListResponse, ClusterError>> + Send>>);

impl Future for MemberListFuture {
    type Output = Result<MemberListResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for MemberList<Client> {
    type Output = Result<MemberListResponse, ClusterError>;
    type IntoFuture = MemberListFuture;

    fn into_future(self) -> Self::IntoFuture {
        MemberListFuture(Box::pin(self.call()))
    }
}

/// The response from a [`member_list`][Client::member_list] operation.
#[derive(Clone, Debug)]
pub struct MemberListResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberListResponse {
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

/// A builder for the [`member_add`][Client::member_add] operation.
#[derive(Clone)]
#[must_use = "MemberAdd does nothing unless you `await` it"]
pub struct MemberAdd<C> {
    client: C,
    request: etcdserverpb::MemberAddRequest,
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
}

impl MemberAdd<Client> {
    async fn call(self) -> Result<MemberAddResponse, ClusterError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::cluster_client::ClusterClient::new,
                async |c, r| c.member_add(r).await,
                self.request,
            )
            .await
            .map_err(ClusterError::from_status)?;
        let header = ClusterResponseHeader::from_pb(resp.header.expect("MemberAddResponse should have a valid header"));
        let member = Member::from_pb(resp.member.expect("MemberAddResponse should have a valid member"));
        let members = resp.members.into_iter().map(Member::from_pb).collect();
        Ok(MemberAddResponse {
            header,
            member,
            members,
        })
    }
}

/// The [`Future`] type returned by awaiting a [`member_add`][Client::member_add].
pub struct MemberAddFuture(Pin<Box<dyn Future<Output = Result<MemberAddResponse, ClusterError>> + Send>>);

impl Future for MemberAddFuture {
    type Output = Result<MemberAddResponse, ClusterError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for MemberAdd<Client> {
    type Output = Result<MemberAddResponse, ClusterError>;
    type IntoFuture = MemberAddFuture;

    fn into_future(self) -> Self::IntoFuture {
        MemberAddFuture(Box::pin(self.call()))
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
// member_remove / member_update / member_promote responses
// -------------------------------------------------------------------------------------------------

/// The response from a [`member_remove`][Client::member_remove] operation.
#[derive(Clone, Debug)]
pub struct MemberRemoveResponse {
    header: ClusterResponseHeader,
    members: Vec<Member>,
}

impl MemberRemoveResponse {
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
    /// This comes from the gRPC API as `NOT_FOUND`, or `FAILED_PRECONDITION` with "member not found".
    MemberNotFound,
    /// A member with the requested ID already exists.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "ID exists".
    MemberAlreadyExists,
    /// One or more of the requested peer URLs are already in use by another member.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "Peer URLs already exists".
    PeerUrlExists,
    /// Tried to promote a member that is not a learner.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "can only promote a learner".
    MemberNotLearner,
    /// Tried to promote a learner that is not yet caught up with the cluster leader.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "learner not ready".
    LearnerNotReady,
    /// The cluster is not healthy enough to satisfy this reconfiguration.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "unhealthy cluster".
    UnhealthyCluster,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and
    /// `INVALID_ARGUMENT` when the argument describes an authentication error.
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
        let kind = match status.code() {
            tonic::Code::NotFound => ClusterErrorKind::MemberNotFound,
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("member not found") || msg.contains("ID removed") {
                    ClusterErrorKind::MemberNotFound
                } else if msg.contains("ID exists") {
                    ClusterErrorKind::MemberAlreadyExists
                } else if msg.contains("Peer URLs already exists") || msg.contains("peerURL exists") {
                    ClusterErrorKind::PeerUrlExists
                } else if msg.contains("learner not ready") {
                    ClusterErrorKind::LearnerNotReady
                } else if msg.contains("not a learner") || msg.contains("can only promote a learner") {
                    ClusterErrorKind::MemberNotLearner
                } else if msg.contains("unhealthy cluster") {
                    ClusterErrorKind::UnhealthyCluster
                } else {
                    ClusterErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => ClusterErrorKind::Authentication,
            tonic::Code::PermissionDenied => ClusterErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => ClusterErrorKind::Authentication,
            tonic::Code::ResourceExhausted => ClusterErrorKind::Exhausted,
            tonic::Code::DataLoss => ClusterErrorKind::DataLoss,
            tonic::Code::Unavailable => ClusterErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => ClusterErrorKind::Timeout,
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
