use std::{sync::Arc, time::Duration};

use bytes::Bytes;

use crate::{
    Client, ClusterResponseHeader, GetError, GetErrorKind, KeyWithMetadata, LeaseId, Record, ResponseHeader,
    client::{
        AuthDisable, AuthDisableFuture, AuthDisableResponse, AuthEnable, AuthEnableFuture, AuthEnableResponse,
        AuthError, AuthErrorKind, Authenticate, AuthenticateFuture, AuthenticateResponse, ClusterError, Compact,
        CompactError, CompactFuture, CompactResponse, CountResponse, Delete, DeleteError, DeleteFuture, DeleteResponse,
        Get, GetFuture, GetResponse, GrantLease, GrantLeaseError, GrantLeaseErrorKind, GrantLeaseFuture,
        GrantLeaseResponse, KeepAliveError, KeepAliveReceiverStream, KeepAliveResponse, KeepAliveSender,
        KeepAliveStream, LeaseInfo, LeaseKeeper, LeaseTimeToLive, LeaseTimeToLiveError, LeaseTimeToLiveFuture,
        LeaseTimeToLiveResponse, Leases, LeasesError, LeasesFuture, LeasesResponse, List, ListContinuation, ListFuture,
        ListView, Member, MemberAdd, MemberAddFuture, MemberAddResponse, MemberList, MemberListFuture,
        MemberListResponse, MemberPromote, MemberPromoteFuture, MemberPromoteResponse, MemberRemove,
        MemberRemoveFuture, MemberRemoveResponse, MemberUpdate, MemberUpdateFuture, MemberUpdateResponse, Put,
        PutError, PutFuture, PutResponse, RevokeLease, RevokeLeaseError, RevokeLeaseFuture, RevokeLeaseResponse,
        RoleAdd, RoleAddFuture, RoleAddResponse, RoleError, Transaction, TransactionError, TransactionFuture,
        TransactionResponse, UserAdd, UserAddFuture, UserAddResponse, UserError, UserGrantRole, UserGrantRoleFuture,
        UserGrantRoleResponse, WatchBuilder, Watcher,
    },
    pb::{etcdserverpb, mvccpb},
};

async fn fetch_first_list_batch(
    client: Client,
    mut request: etcdserverpb::RangeRequest,
) -> Result<(ResponseHeader, Vec<mvccpb::KeyValue>, Option<ListContinuation>), GetError> {
    if request.key.is_empty() && request.range_end.is_empty() {
        request.key = Bytes::from_static(&[0]);
        request.range_end = Bytes::from_static(&[0]);
    }

    let resp = client
        .inner
        .wrap_unary_call(
            etcdserverpb::kv_client::KvClient::new,
            async |c, r| c.range(r).await,
            request.clone(),
        )
        .await
        .map_err(GetError::from_status)?;

    let header = ResponseHeader::from_pb(resp.header.expect("RangeResponse should have a valid header"));

    let continuation = if resp.more {
        if request.revision == 0 {
            request.revision = header.revision().get();
        }

        let Some(last_kv) = resp.kvs.last() else {
            return Err(GetError::new(
                GetErrorKind::Unknown,
                "`range` call has no results, but `more = true`...is something wrong with the server?",
                None,
            ));
        };
        request.key = crate::range::successor(&last_kv.key).into();
        Some(ListContinuation { client, request })
    } else {
        None
    };
    Ok((header, resp.kvs, continuation))
}

fn execute_list_impl<R: Send + 'static>(
    client: Client,
    list: List<(), R>,
    convert: fn(mvccpb::KeyValue) -> R,
) -> ListFuture<Result<ListView<R>, GetError>> {
    let request = list.request;
    ListFuture::new(async move {
        let (header, first_batch_kvs, continuation) = fetch_first_list_batch(client, request).await?;
        Ok(ListView::new(header, first_batch_kvs, continuation, convert))
    })
}

impl crate::driver::KvDriver for Client {
    type GetFuture = GetFuture;
    type PutFuture<R> = PutFuture<Result<PutResponse<R>, PutError>>;
    type DeleteFuture<R, P> = DeleteFuture<Result<DeleteResponse<R, P>, DeleteError>>;
    type ListView<R> = ListView<R>;
    type ListViewFuture<R> = ListFuture<Result<Self::ListView<R>, GetError>>;
    type CountFuture = ListFuture<Result<CountResponse, GetError>>;
    type CommitFuture = TransactionFuture;
    type CompactFuture = CompactFuture;

    fn execute_get(self, get: Get<()>) -> Self::GetFuture {
        let request = get.request;
        GetFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.range(r).await,
                    request,
                )
                .await
                .map_err(GetError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("RangeResponse should have a valid header"));
            if resp.more || resp.kvs.len() > 1 {
                Err(GetError::new(
                    GetErrorKind::Unknown,
                    "call to get should have only 1 response",
                    None,
                ))
            } else if let Some(r) = resp.kvs.into_iter().next() {
                Ok(GetResponse::new(header, Some(super::record_from_pb(r))))
            } else {
                Ok(GetResponse::new(header, None))
            }
        })
    }

    fn execute_put<R>(self, put: Put<(), R>) -> Self::PutFuture<R> {
        let request = put.request;
        PutFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.put(r).await,
                    request,
                )
                .await
                .map_err(PutError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("PutResponse should have a valid header"));
            let previous = resp.prev_kv.map(super::record_from_pb);
            Ok(PutResponse::new(header, previous))
        })
    }

    fn execute_delete<R, P>(self, delete: Delete<(), R, P>) -> Self::DeleteFuture<R, P> {
        let request = delete.request;
        DeleteFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.delete_range(r).await,
                    request,
                )
                .await
                .map_err(DeleteError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("DeleteRangeResponse should have a valid header"));
            let deleted = resp.deleted as usize;
            let previous = resp.prev_kvs.into_iter().map(super::record_from_pb).collect();
            Ok(DeleteResponse::new(header, deleted, previous))
        })
    }

    fn execute_list_records(self, list: List<(), Record>) -> Self::ListViewFuture<Record> {
        execute_list_impl(self, list, super::record_from_pb)
    }

    fn execute_list_keys(self, list: List<(), KeyWithMetadata>) -> Self::ListViewFuture<KeyWithMetadata> {
        execute_list_impl(self, list, super::key_with_metadata_from_pb)
    }

    fn execute_count(self, list: List<(), usize>) -> Self::CountFuture {
        let request = list.request;
        ListFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.range(r).await,
                    request,
                )
                .await
                .map_err(GetError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("RangeResponse should have a valid header"));
            Ok(CountResponse::new(header, resp.count as usize))
        })
    }

    fn execute_transaction(self, txn: Transaction<()>) -> Self::CommitFuture {
        let (request, success_kinds, failure_kinds) = txn.into_request_parts();
        TransactionFuture::new(async move {
            self.inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.txn(r).await,
                    request,
                )
                .await
                .map(|response| TransactionResponse::from_pb(response, &success_kinds, &failure_kinds))
                .map_err(TransactionError::from_status)
        })
    }

    fn execute_compact(self, compact: Compact<()>) -> Self::CompactFuture {
        let request = compact.request;
        CompactFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.compact(r).await,
                    request,
                )
                .await
                .map_err(CompactError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("CompactionResponse should have a valid header"));
            Ok(CompactResponse::new(header))
        })
    }
}

impl crate::driver::LeaseDriver for Client {
    type GrantFuture = GrantLeaseFuture;
    type RevokeFuture = RevokeLeaseFuture;
    type TimeToLiveFuture<K> = LeaseTimeToLiveFuture<Result<LeaseTimeToLiveResponse<K>, LeaseTimeToLiveError>>;
    type LeasesFuture = LeasesFuture;
    type LeaseKeeper = LeaseKeeper;

    fn execute_grant_lease(self, grant: GrantLease<()>) -> Self::GrantFuture {
        let request = grant.request;
        GrantLeaseFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::lease_client::LeaseClient::new,
                    async |c, r| c.lease_grant(r).await,
                    request,
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
            Ok(GrantLeaseResponse::new(header, LeaseInfo { lease_id, ttl }))
        })
    }

    fn execute_revoke_lease(self, revoke: RevokeLease<()>) -> Self::RevokeFuture {
        let lease_id = revoke.lease_id();
        RevokeLeaseFuture::new(async move {
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
            Ok(RevokeLeaseResponse::new(header))
        })
    }

    fn execute_lease_time_to_live<K>(self, op: LeaseTimeToLive<(), K>) -> Self::TimeToLiveFuture<K> {
        let request = op.request;
        LeaseTimeToLiveFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::lease_client::LeaseClient::new,
                    async |c, r| c.lease_time_to_live(r).await,
                    request,
                )
                .await
                .map_err(LeaseTimeToLiveError::from_status)?;
            let header =
                ResponseHeader::from_pb(resp.header.expect("LeaseTimeToLiveResponse should have a valid header"));
            let lease_id = LeaseId::new(resp.id).expect("LeaseTimeToLiveResponse should echo the lease ID");
            // The server reports a missing or expired lease as `ttl = -1` on a successful response.
            let ttl = (resp.ttl > 0).then(|| Duration::from_secs(resp.ttl as _));
            let granted_ttl = (resp.granted_ttl > 0).then(|| Duration::from_secs(resp.granted_ttl as _));
            Ok(LeaseTimeToLiveResponse::new(
                header,
                LeaseInfo { lease_id, ttl },
                granted_ttl,
                resp.keys,
            ))
        })
    }

    fn execute_leases(self, _: Leases<()>) -> Self::LeasesFuture {
        LeasesFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::lease_client::LeaseClient::new,
                    async |c, r| c.lease_leases(r).await,
                    etcdserverpb::LeaseLeasesRequest {},
                )
                .await
                .map_err(LeasesError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("LeaseLeasesResponse should have a valid header"));
            let leases = resp
                .leases
                .into_iter()
                .map(|status| LeaseId::new(status.id).expect("lease listing should not contain a zero lease ID"))
                .collect();
            Ok(LeasesResponse::new(header, leases))
        })
    }

    fn start_lease_keeper(self) -> Self::LeaseKeeper {
        let channel = self.inner.channel.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let shared_rx = Arc::new(std::sync::Mutex::new(rx));
        let inner = async_stream::stream! {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let mut response_stream = loop {
                let request_stream = KeepAliveReceiverStream { inner: shared_rx.clone() };
                let mut lease_client = etcdserverpb::lease_client::LeaseClient::new(channel.clone());
                match lease_client.lease_keep_alive(request_stream).await {
                    Ok(resp) => break resp.into_inner(),
                    Err(status) if status.code() == tonic::Code::Unavailable && std::time::Instant::now() <= deadline => {
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
                        let header = ResponseHeader::from_pb(resp.header.expect("LeaseKeepAliveResponse should have a valid header"));
                        let lease_id = LeaseId::new(resp.id).expect("LeaseKeepAliveResponse should have a valid lease ID");
                        let ttl = if resp.ttl > 0 { Some(Duration::from_secs(resp.ttl as _)) } else { None };
                        yield Ok(KeepAliveResponse::new(header, LeaseInfo { lease_id, ttl }));
                    }
                    Ok(None) => break,
                    Err(status) => {
                        yield Err(KeepAliveError::from_status(status));
                        break;
                    }
                }
            }
        };
        LeaseKeeper::new(KeepAliveSender::new(tx), KeepAliveStream::new(inner))
    }
}

impl crate::driver::WatchDriver for Client {
    type Watcher = Watcher;

    fn start_watch(self, builder: WatchBuilder<()>) -> Self::Watcher {
        builder.with_client(self).start_client_watch()
    }
}

impl crate::driver::AuthDriver for Client {
    type AuthEnableFuture = AuthEnableFuture;
    type AuthDisableFuture = AuthDisableFuture;
    type AuthenticateFuture = AuthenticateFuture;
    type UserAddFuture = UserAddFuture;
    type UserGrantRoleFuture = UserGrantRoleFuture;
    type RoleAddFuture = RoleAddFuture;

    fn execute_auth_enable(self, _: AuthEnable<()>) -> Self::AuthEnableFuture {
        AuthEnableFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::auth_client::AuthClient::new,
                    async |c, r| c.auth_enable(r).await,
                    etcdserverpb::AuthEnableRequest {},
                )
                .await
                .map_err(AuthError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("AuthEnableResponse should have a valid header"));
            Ok(AuthEnableResponse::new(header))
        })
    }

    fn execute_auth_disable(self, _: AuthDisable<()>) -> Self::AuthDisableFuture {
        AuthDisableFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::auth_client::AuthClient::new,
                    async |c, r| c.auth_disable(r).await,
                    etcdserverpb::AuthDisableRequest {},
                )
                .await
                .map_err(AuthError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("AuthDisableResponse should have a valid header"));
            Ok(AuthDisableResponse::new(header))
        })
    }

    fn execute_authenticate(self, _: Authenticate<()>) -> Self::AuthenticateFuture {
        AuthenticateFuture::new(async move {
            let Some(auth) = &self.inner.auth else {
                return Err(AuthError::new(
                    AuthErrorKind::InvalidCredentials,
                    "no credentials configured on client",
                    None,
                ));
            };
            let Some(creds) = &auth.credentials else {
                return Err(AuthError::new(
                    AuthErrorKind::InvalidCredentials,
                    "no credentials configured on client",
                    None,
                ));
            };
            let resp = self
                .inner
                .authenticate(&creds.username, &creds.password)
                .await
                .map_err(AuthError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("AuthenticateResponse should have a valid header"));
            Ok(AuthenticateResponse::new(header, resp.token))
        })
    }

    fn execute_user_add(self, op: UserAdd<()>) -> Self::UserAddFuture {
        let request = op.request;
        UserAddFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::auth_client::AuthClient::new,
                    async |c, r| c.user_add(r).await,
                    request,
                )
                .await
                .map_err(UserError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("AuthUserAddResponse should have a valid header"));
            Ok(UserAddResponse::new(header))
        })
    }

    fn execute_user_grant_role(self, op: UserGrantRole<()>) -> Self::UserGrantRoleFuture {
        let request = op.request;
        UserGrantRoleFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::auth_client::AuthClient::new,
                    async |c, r| c.user_grant_role(r).await,
                    request,
                )
                .await
                .map_err(UserError::from_status)?;
            let header = ResponseHeader::from_pb(
                resp.header
                    .expect("AuthUserGrantRoleResponse should have a valid header"),
            );
            Ok(UserGrantRoleResponse::new(header))
        })
    }

    fn execute_role_add(self, op: RoleAdd<()>) -> Self::RoleAddFuture {
        let request = op.request;
        RoleAddFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::auth_client::AuthClient::new,
                    async |c, r| c.role_add(r).await,
                    request,
                )
                .await
                .map_err(RoleError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("AuthRoleAddResponse should have a valid header"));
            Ok(RoleAddResponse::new(header))
        })
    }
}

impl crate::driver::ClusterDriver for Client {
    type MemberListFuture = MemberListFuture;
    type MemberAddFuture = MemberAddFuture;
    type MemberRemoveFuture = MemberRemoveFuture;
    type MemberUpdateFuture = MemberUpdateFuture;
    type MemberPromoteFuture = MemberPromoteFuture;

    fn execute_member_list(self, op: MemberList<()>) -> Self::MemberListFuture {
        let request = op.request;
        MemberListFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::cluster_client::ClusterClient::new,
                    async |c, r| c.member_list(r).await,
                    request,
                )
                .await
                .map_err(ClusterError::from_status)?;
            let header =
                ClusterResponseHeader::from_pb(resp.header.expect("MemberListResponse should have a valid header"));
            let members = resp.members.into_iter().map(Member::from_pb).collect();
            Ok(MemberListResponse::new(header, members))
        })
    }

    fn execute_member_add(self, op: MemberAdd<()>) -> Self::MemberAddFuture {
        let request = op.request;
        MemberAddFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::cluster_client::ClusterClient::new,
                    async |c, r| c.member_add(r).await,
                    request,
                )
                .await
                .map_err(ClusterError::from_status)?;
            let header =
                ClusterResponseHeader::from_pb(resp.header.expect("MemberAddResponse should have a valid header"));
            let member = Member::from_pb(resp.member.expect("MemberAddResponse should have a valid member"));
            let members = resp.members.into_iter().map(Member::from_pb).collect();
            Ok(MemberAddResponse::new(header, member, members))
        })
    }

    fn execute_member_remove(self, op: MemberRemove<()>) -> Self::MemberRemoveFuture {
        let request = op.request;
        MemberRemoveFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::cluster_client::ClusterClient::new,
                    async |c, r| c.member_remove(r).await,
                    request,
                )
                .await
                .map_err(ClusterError::from_status)?;
            let header =
                ClusterResponseHeader::from_pb(resp.header.expect("MemberRemoveResponse should have a valid header"));
            let members = resp.members.into_iter().map(Member::from_pb).collect();
            Ok(MemberRemoveResponse::new(header, members))
        })
    }

    fn execute_member_update(self, op: MemberUpdate<()>) -> Self::MemberUpdateFuture {
        let request = op.request;
        MemberUpdateFuture::new(async move {
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
            Ok(MemberUpdateResponse::new(header, members))
        })
    }

    fn execute_member_promote(self, op: MemberPromote<()>) -> Self::MemberPromoteFuture {
        let request = op.request;
        MemberPromoteFuture::new(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::cluster_client::ClusterClient::new,
                    async |c, r| c.member_promote(r).await,
                    request,
                )
                .await
                .map_err(ClusterError::from_status)?;
            let header =
                ClusterResponseHeader::from_pb(resp.header.expect("MemberPromoteResponse should have a valid header"));
            let members = resp.members.into_iter().map(Member::from_pb).collect();
            Ok(MemberPromoteResponse::new(header, members))
        })
    }
}
