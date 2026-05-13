use std::future::Future;

use crate::client::{
    ClusterError, MemberAdd, MemberAddResponse, MemberList, MemberListResponse, MemberPromote, MemberPromoteResponse,
    MemberRemove, MemberRemoveResponse, MemberUpdate, MemberUpdateResponse,
};

/// Driver for cluster membership operations.
pub trait ClusterDriver {
    type MemberListFuture: Future<Output = Result<MemberListResponse, ClusterError>> + Send;
    type MemberAddFuture: Future<Output = Result<MemberAddResponse, ClusterError>> + Send;
    type MemberRemoveFuture: Future<Output = Result<MemberRemoveResponse, ClusterError>> + Send;
    type MemberUpdateFuture: Future<Output = Result<MemberUpdateResponse, ClusterError>> + Send;
    type MemberPromoteFuture: Future<Output = Result<MemberPromoteResponse, ClusterError>> + Send;

    fn execute_member_list(self, op: MemberList<()>) -> Self::MemberListFuture;
    fn execute_member_add(self, op: MemberAdd<()>) -> Self::MemberAddFuture;
    fn execute_member_remove(self, op: MemberRemove<()>) -> Self::MemberRemoveFuture;
    fn execute_member_update(self, op: MemberUpdate<()>) -> Self::MemberUpdateFuture;
    fn execute_member_promote(self, op: MemberPromote<()>) -> Self::MemberPromoteFuture;
}
