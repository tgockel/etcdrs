use std::future::Future;

use futures_core::Stream;

use crate::client::{
    GrantLease, GrantLeaseError, GrantLeaseResponse, KeepAliveError, KeepAliveResponse, RevokeLease, RevokeLeaseError,
    RevokeLeaseResponse,
};

/// Driver for lease operations.
pub trait LeaseDriver {
    /// The future returned by [`execute_grant_lease`][Self::execute_grant_lease].
    type GrantFuture: Future<Output = Result<GrantLeaseResponse, GrantLeaseError>> + Send;

    /// The future returned by [`execute_revoke_lease`][Self::execute_revoke_lease].
    type RevokeFuture: Future<Output = Result<RevokeLeaseResponse, RevokeLeaseError>> + Send;

    /// The keep-alive stream returned by [`start_lease_keeper`][Self::start_lease_keeper].
    type LeaseKeeper: Stream<Item = Result<KeepAliveResponse, KeepAliveError>> + Send;

    /// Execute a lease grant operation.
    fn execute_grant_lease(self, grant: GrantLease<()>) -> Self::GrantFuture;

    /// Revoke an existing lease.
    fn execute_revoke_lease(self, revoke: RevokeLease<()>) -> Self::RevokeFuture;

    /// Create a lease keep-alive stream.
    fn start_lease_keeper(self) -> Self::LeaseKeeper;
}
