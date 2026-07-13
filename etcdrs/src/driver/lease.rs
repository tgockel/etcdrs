use std::future::Future;

use futures_core::Stream;

use crate::client::{
    GrantLease, GrantLeaseError, GrantLeaseResponse, KeepAliveError, KeepAliveResponse, LeaseTimeToLive,
    LeaseTimeToLiveError, LeaseTimeToLiveResponse, Leases, LeasesError, LeasesResponse, RevokeLease, RevokeLeaseError,
    RevokeLeaseResponse,
};

/// Driver for lease operations.
pub trait LeaseDriver {
    /// The future returned by [`execute_grant_lease`][Self::execute_grant_lease].
    type GrantFuture: Future<Output = Result<GrantLeaseResponse, GrantLeaseError>> + Send;

    /// The future returned by [`execute_revoke_lease`][Self::execute_revoke_lease].
    type RevokeFuture: Future<Output = Result<RevokeLeaseResponse, RevokeLeaseError>> + Send;

    /// The future returned by [`execute_lease_time_to_live`][Self::execute_lease_time_to_live].
    type TimeToLiveFuture<K>: Future<Output = Result<LeaseTimeToLiveResponse<K>, LeaseTimeToLiveError>> + Send;

    /// The future returned by [`execute_leases`][Self::execute_leases].
    type LeasesFuture: Future<Output = Result<LeasesResponse, LeasesError>> + Send;

    /// The keep-alive stream returned by [`start_lease_keeper`][Self::start_lease_keeper].
    type LeaseKeeper: Stream<Item = Result<KeepAliveResponse, KeepAliveError>> + Send;

    /// Execute a lease grant operation.
    fn execute_grant_lease(self, grant: GrantLease<()>) -> Self::GrantFuture;

    /// Revoke an existing lease.
    fn execute_revoke_lease(self, revoke: RevokeLease<()>) -> Self::RevokeFuture;

    /// Execute a lease time-to-live query.
    fn execute_lease_time_to_live<K>(self, op: LeaseTimeToLive<(), K>) -> Self::TimeToLiveFuture<K>;

    /// Execute a lease listing operation.
    fn execute_leases(self, op: Leases<()>) -> Self::LeasesFuture;

    /// Create a lease keep-alive stream.
    fn start_lease_keeper(self) -> Self::LeaseKeeper;
}
