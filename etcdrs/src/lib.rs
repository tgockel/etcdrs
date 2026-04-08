#![cfg_attr(feature = "nightly-async-iterator", feature(async_iterator))]
#![doc = include_str!("../README.md")]

use std::num::{NonZeroI64, NonZeroU64};

#[macro_use]
pub(crate) mod error;
pub mod client;
pub mod driver;
pub(crate) mod pb;
pub(crate) mod range;
pub mod record;

pub use client::{
    AuthDisableResponse, AuthEnableResponse, AuthError, AuthErrorKind, AuthenticateResponse, BuildError, Client,
    DeleteError, DeleteErrorKind, GetError, GetErrorKind, GrantLeaseError, GrantLeaseErrorKind, KeepAliveError,
    KeepAliveErrorKind, PutError, PutErrorKind, RevokeLeaseError, RevokeLeaseErrorKind, RoleAddResponse, RoleError,
    RoleErrorKind, TransactionError, TransactionErrorKind, UserAddResponse, UserError, UserErrorKind,
    UserGrantRoleResponse, WatchError, WatchErrorKind, WatchId,
};
pub use error::OperationError;
pub use range::{AsRange, Prefix};
pub use record::{KeyWithMetadata, Metadata, Record};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct ClusterId(NonZeroU64);

impl ClusterId {
    pub(crate) fn new(source: u64) -> Option<Self> {
        NonZeroU64::new(source).map(Self)
    }

    /// Returns the underlying cluster ID as a `u64`.
    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct MemberId(NonZeroU64);

impl MemberId {
    pub(crate) fn new(source: u64) -> Option<Self> {
        NonZeroU64::new(source).map(Self)
    }

    /// Returns the underlying member ID as a `u64`.
    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct LeaseId(NonZeroI64);

impl LeaseId {
    pub fn new(source: i64) -> Option<Self> {
        NonZeroI64::new(source).map(Self)
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Term(NonZeroU64);

impl Term {
    pub fn new(source: u64) -> Option<Self> {
        NonZeroU64::new(source).map(Self)
    }

    /// Returns the underlying term as a `u64`.
    pub fn get(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Revision(NonZeroI64);

impl Revision {
    pub fn new(source: i64) -> Option<Self> {
        NonZeroI64::new(source).map(Self)
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

/// Global metadata returned with every etcd API response.
///
/// Every response from etcd includes a response header with metadata about the cluster and the
/// request. See the [etcd API reference](https://etcd.io/docs/v3.5/learning/api/#response-header)
/// for more information.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ResponseHeader {
    cluster_id: ClusterId,
    member_id: MemberId,
    revision: Revision,
    raft_term: Term,
}

impl ResponseHeader {
    /// Construct a new `ResponseHeader`.
    pub fn new(cluster_id: ClusterId, member_id: MemberId, revision: Revision, raft_term: Term) -> Self {
        Self {
            cluster_id,
            member_id,
            revision,
            raft_term,
        }
    }

    pub(crate) fn from_pb(pb: crate::pb::etcdserverpb::ResponseHeader) -> Self {
        Self {
            cluster_id: ClusterId::new(pb.cluster_id).expect("cluster_id should be non-zero"),
            member_id: MemberId::new(pb.member_id).expect("member_id should be non-zero"),
            revision: Revision::new(pb.revision).expect("revision should be non-zero"),
            raft_term: Term::new(pb.raft_term).expect("raft_term should be non-zero"),
        }
    }

    /// ID of the cluster which sent the response.
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// ID of the member which sent the response.
    pub fn member_id(&self) -> MemberId {
        self.member_id
    }

    /// Key-value store revision when the request was applied.
    ///
    /// Every key-value mutation increments the store revision, making this a global logical clock
    /// for the cluster.
    pub fn revision(&self) -> Revision {
        self.revision
    }

    /// Raft term of the member when the request was applied.
    ///
    /// The raft term increases monotonically whenever the cluster leader changes.
    pub fn raft_term(&self) -> Term {
        self.raft_term
    }
}

/// A monotonically increasing version number.
///
/// This is used to track the [`version`][record::Metadata::version] of a [`Record`]. Each time the record is modified,
/// the version is incremented. A newly-created record has the version `1`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(transparent)]
pub struct Version(u64);

impl Version {
    pub const fn new(source: u64) -> Self {
        Self(source)
    }
}
