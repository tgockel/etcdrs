use std::sync::Arc;
use std::time::Duration;

use etcdrs::client::{Get, KeepAliveResponse, LeaseKeeper, RequestCounter, Watch, WatchEvent, Watcher};
use etcdrs::{
    KeepAliveError, KeepAliveErrorKind, Permission, Prefix, Record, TargetRange, WatchError, WatchErrorKind, WatchId,
};
use etcdrs_test::{EtcdServer, etcd_server};
use futures::StreamExt;
use rstest::rstest;

/// Helper: create the root user with the root role and enable authentication.
async fn enable_auth_with_root(client: &etcdrs::Client) {
    client.user_add("root").password("rootpw").await.unwrap();
    client.role_add("root").await.unwrap();
    client.user_grant_role("root", "root").await.unwrap();
    client.auth_enable().await.unwrap();
}

/// Helper: a client authenticated with the given credentials.
fn client_with_credentials(etcd_server: &EtcdServer, user: &str, password: &str) -> etcdrs::Client {
    etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .credentials(user, password)
        .build()
        .unwrap()
}

/// Helper: a client that sends a pre-obtained `token`.
fn client_with_token(etcd_server: &EtcdServer, token: &str) -> etcdrs::Client {
    etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .auth_token(token)
        .unwrap()
        .build()
        .unwrap()
}

/// Helper: enable auth and return a client with root's credentials, which the caller needs to
/// disable auth again.
async fn enable_auth(etcd_server: &EtcdServer) -> etcdrs::Client {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    enable_auth_with_root(&client).await;
    client_with_credentials(etcd_server, "root", "rootpw")
}

/// Helper: enable auth and add `eve`, who may read `pub/` and nothing else. Seeds `pub/x` and
/// `secret/x`. Returns the root client, which the caller needs to disable auth again.
async fn enable_auth_with_reader(etcd_server: &EtcdServer) -> etcdrs::Client {
    let root = enable_auth(etcd_server).await;

    root.user_add("eve").password("evepw").await.unwrap();
    root.role_add("reader").await.unwrap();
    root.role_grant_permission("reader", Permission::read(Prefix("pub/")))
        .await
        .unwrap();
    root.user_grant_role("eve", "reader").await.unwrap();

    root.put("pub/x").value("visible").await.unwrap();
    root.put("secret/x").value("hidden").await.unwrap();
    root
}

/// Helper: revoke every token root holds. etcd revokes a user's tokens whenever their password
/// changes, even to the password they already have.
async fn revoke_root_tokens(root: &etcdrs::Client) {
    root.user_change_password("root", "rootpw").await.unwrap();
}

/// How a test client authenticates as root.
#[derive(Clone, Copy, Debug)]
enum RootAuth {
    Credentials,
    Token,
}

/// Helper: a client that authenticates as root the way `auth` says, with a token from `root` for
/// [`RootAuth::Token`].
async fn root_client(etcd_server: &EtcdServer, root: &etcdrs::Client, auth: RootAuth) -> etcdrs::Client {
    match auth {
        RootAuth::Credentials => client_with_credentials(etcd_server, "root", "rootpw"),
        RootAuth::Token => client_with_token(etcd_server, root.authenticate().await.unwrap().token()),
    }
}

/// Helper: the next item from a watcher, with a timeout.
async fn next_watch_item(watcher: &mut Watcher) -> Result<WatchEvent, WatchError> {
    tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out waiting for watch event")
        .expect("watch stream ended unexpectedly")
}

/// Helper: the watch ID and record of the next event from a watcher, which must be a put.
async fn next_put(watcher: &mut Watcher) -> (WatchId, Record) {
    match next_watch_item(watcher).await {
        Ok(WatchEvent::Put { watch_id, record, .. }) => (watch_id, record),
        other => panic!("expected a put, got {other:?}"),
    }
}

/// Helper: the next item from a lease keeper, with a timeout.
async fn next_keep_alive(keeper: &mut LeaseKeeper) -> Result<KeepAliveResponse, KeepAliveError> {
    tokio::time::timeout(Duration::from_secs(5), keeper.next())
        .await
        .expect("timed out waiting for keep-alive response")
        .expect("keep-alive stream ended unexpectedly")
}

#[rstest]
#[tokio::test]
async fn auth_lifecycle(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    // Setup: create root user and role, then enable auth
    client.user_add("root").password("rootpw").await.unwrap();
    client.role_add("root").await.unwrap();
    client.user_grant_role("root", "root").await.unwrap();
    client.auth_enable().await.unwrap();

    // A client with credentials can make authenticated requests
    let authed = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .credentials("root", "rootpw")
        .build()
        .unwrap();

    // Verify authenticate returns a token
    let token = authed.authenticate().await.unwrap().into_token();
    assert!(!token.is_empty(), "token should not be empty");
    authed.put("foo").value("bar").await.unwrap();
    let record = authed.get("foo").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"bar"[..]);

    // A client with a pre-obtained token can also make authenticated requests
    let token_client = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .auth_token(&token)
        .unwrap()
        .build()
        .unwrap();
    let record = token_client.get("foo").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"bar"[..]);

    // A client without credentials should fail
    let unauthed = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    let err = unauthed.get("foo").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::GetErrorKind::Authentication);
    let err = unauthed
        .transaction()
        .and_then_do(Get::new("foo"))
        .commit()
        .await
        .unwrap_err();
    assert_eq!(err.kind(), etcdrs::TransactionErrorKind::Authentication, "{err:?}");
    assert_eq!(
        err.grpc_status().map(|s| s.message()),
        Some("etcdserver: user name is empty"),
        "{err:?}"
    );

    // Cleanup: disable auth
    authed.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn auth_enable_requires_root(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    // Enabling auth without a root user should fail
    let err = client.auth_enable().await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::AuthErrorKind::RootUserRequired);
}

#[rstest]
#[tokio::test]
async fn auth_status_reports_enabled(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let status = client.auth_status().await.unwrap();
    assert!(!status.enabled(), "auth should start disabled");

    enable_auth_with_root(&client).await;

    let authed = client_with_credentials(&etcd_server, "root", "rootpw");
    let status = authed.auth_status().await.unwrap();
    assert!(status.enabled());
    assert!(status.auth_revision() > 0);

    // Cleanup: disable auth
    authed.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn user_management_lifecycle(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.user_add("alice").password("pw").await.unwrap();
    client.user_add("bob").password("pw").await.unwrap();

    let users = client.user_list().await.unwrap();
    assert!(users.users().contains(&"alice".to_string()), "{users:?}");
    assert!(users.users().contains(&"bob".to_string()), "{users:?}");

    client.role_add("dev").await.unwrap();
    client.user_grant_role("alice", "dev").await.unwrap();
    let alice = client.user_get("alice").await.unwrap();
    assert_eq!(alice.roles(), ["dev".to_string()]);

    client.user_revoke_role("alice", "dev").await.unwrap();
    assert!(client.user_get("alice").await.unwrap().roles().is_empty());

    let err = client.user_revoke_role("alice", "dev").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::UserErrorKind::RoleNotGranted);

    client.user_delete("bob").await.unwrap();
    let err = client.user_get("bob").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::UserErrorKind::UserNotFound);

    client.user_change_password("alice", "newpw").await.unwrap();
}

#[rstest]
#[tokio::test]
async fn change_password_takes_effect(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    client.user_add("carol").password("old").await.unwrap();
    enable_auth_with_root(&client).await;

    let root = client_with_credentials(&etcd_server, "root", "rootpw");
    root.user_change_password("carol", "new").await.unwrap();

    let carol = client_with_credentials(&etcd_server, "carol", "new");
    let token = carol.authenticate().await.unwrap().into_token();
    assert!(!token.is_empty(), "token should not be empty");

    let carol_old = client_with_credentials(&etcd_server, "carol", "old");
    let err = carol_old.authenticate().await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::AuthErrorKind::InvalidCredentials);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn role_permission_lifecycle(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.role_add("app").await.unwrap();
    let rw_prefix = Permission::read_write(Prefix("app/"));
    let read_key = Permission::read("config");
    client.role_grant_permission("app", rw_prefix.clone()).await.unwrap();
    client.role_grant_permission("app", read_key.clone()).await.unwrap();

    let role = client.role_get("app").await.unwrap();
    assert!(role.permissions().contains(&rw_prefix), "{:?}", role.permissions());
    assert!(role.permissions().contains(&read_key), "{:?}", role.permissions());
    assert_eq!(read_key.target_range(), TargetRange::Single(b"config"));

    client.role_revoke_permission("app", Prefix("app/")).await.unwrap();
    let role = client.role_get("app").await.unwrap();
    assert_eq!(role.permissions(), [read_key]);

    let err = client.role_revoke_permission("app", Prefix("app/")).await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::PermissionNotGranted);

    let roles = client.role_list().await.unwrap();
    assert!(roles.roles().contains(&"app".to_string()), "{roles:?}");

    client.role_delete("app").await.unwrap();
    let err = client.role_get("app").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::RoleNotFound);
}

#[rstest]
#[tokio::test]
async fn permissions_enforced_end_to_end(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;

    let eve = client_with_credentials(&etcd_server, "eve", "evepw");
    let record = eve.get("pub/x").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"visible"[..]);

    let err = eve.put("pub/x").value("nope").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::PutErrorKind::Authentication);
    let err = eve.get("secret/x").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::GetErrorKind::Authentication);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// An inclusive range ends at its key. `Permission::read("a"..="a")` used to be granted as
/// `Prefix("a")`, so the role could read every key under `a`.
#[rstest]
#[tokio::test]
async fn inclusive_permission_ends_at_its_key(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;
    root.role_grant_permission("reader", Permission::read("a"..="a"))
        .await
        .unwrap();
    root.put("a").value("visible").await.unwrap();
    root.put("a/secret").value("hidden").await.unwrap();

    let eve = client_with_credentials(&etcd_server, "eve", "evepw");
    let record = eve.get("a").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"visible"[..]);
    let err = eve.get("a/secret").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::GetErrorKind::Authentication);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A range that ends below every key addresses none. etcd checks a read of it at its lower bound,
/// as it does for any empty range, and refuses to grant it. `Permission::read(..="")` used to be
/// granted as every key.
#[rstest]
#[tokio::test]
async fn ranges_over_no_keys_under_auth(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;

    let eve = client_with_credentials(&etcd_server, "eve", "evepw");
    assert_eq!(eve.list("pub/"..="").count_only().await.unwrap().count(), 0);

    let err = root
        .role_grant_permission("reader", Permission::read(..=""))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::InvalidAuthManagement);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// The empty prefix is every key, so a permission over it is the permission over `..`. It used to
/// be sent from the empty key, which etcd refuses to grant.
#[rstest]
#[tokio::test]
async fn empty_prefix_permission_covers_every_key(etcd_server: EtcdServer) {
    assert_eq!(Permission::read(Prefix("")), Permission::read(..));
    assert_eq!(Permission::read(Prefix("")).target_range(), TargetRange::All);

    let root = enable_auth_with_reader(&etcd_server).await;
    root.role_grant_permission("reader", Permission::read(Prefix("")))
        .await
        .unwrap();
    let eve = client_with_credentials(&etcd_server, "eve", "evepw");
    let record = eve.get("secret/x").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"hidden"[..]);

    root.role_revoke_permission("reader", ""..).await.unwrap();
    let role = root.role_get("reader").await.unwrap();
    assert_eq!(role.permissions(), [Permission::read(Prefix("pub/"))]);

    // The single key `""` is no key at all.
    let err = root
        .role_grant_permission("reader", Permission::read(""))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::InvalidAuthManagement);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A permission failure is an answer, not a transient fault: etcd settles the token and identifies
/// the user before it can reach a permission decision, so re-authenticating hands back a token for
/// the same user with the same roles and the same denial. The client used to treat every
/// `PERMISSION_DENIED` as a stale token, which cost an `Authenticate` plus a replay per attempt for
/// as long as the retry policy allowed, so this counts attempts rather than just checking the error.
#[rstest]
#[tokio::test]
async fn denied_permission_is_not_retried(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;

    let metrics = Arc::new(RequestCounter::default());
    let eve = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .credentials("eve", "evepw")
        .metrics(metrics.clone())
        .build()
        .unwrap();

    // The first call is the one that obtains the token: it goes out without one, the server
    // refuses it, and the client authenticates and replays. Two attempts, and no more -- asserted
    // here because a client that cannot get past this has no token to be denied with below, which
    // would make the rest of the test pass for the wrong reason.
    eve.get("pub/x").await.unwrap();
    assert_eq!(metrics.get().requested(), 2, "{:?}", metrics.get());

    let before = metrics.get().requested();
    let err = eve.put("pub/x").value("nope").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::PutErrorKind::Authentication);
    assert_eq!(
        metrics.get().requested() - before,
        1,
        "a denied request was replayed; {:?}",
        metrics.get()
    );

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// The same denial under a policy with nothing to run it out. `RetryPolicy::Forever` used to turn
/// it into an unbounded `Authenticate`-and-replay loop with no backoff.
#[rstest]
#[tokio::test]
async fn denied_permission_terminates_under_retry_forever(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;

    let eve = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .credentials("eve", "evepw")
        .retry_forever()
        .build()
        .unwrap();

    let err = tokio::time::timeout(Duration::from_secs(10), eve.put("pub/x").value("nope"))
        .await
        .expect("a denied request should fail rather than retry forever")
        .unwrap_err();
    assert_eq!(err.kind(), etcdrs::PutErrorKind::Authentication);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn delete_root_user_rejected(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    enable_auth_with_root(&client).await;
    let root = client_with_credentials(&etcd_server, "root", "rootpw");

    let err = root.user_delete("root").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::UserErrorKind::InvalidAuthManagement);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn user_already_exists(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.user_add("alice").password("pw").await.unwrap();
    let err = client.user_add("alice").password("pw").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::UserErrorKind::UserAlreadyExists);
}

#[rstest]
#[tokio::test]
async fn role_already_exists(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.role_add("myrole").await.unwrap();
    let err = client.role_add("myrole").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::RoleAlreadyExists);
}

/// etcd refuses a role with no name with an `INVALID_ARGUMENT` about the request, which used to be
/// reported as `Authentication`. A user with no name gets the same status as a request without a
/// token, so it still is.
#[rstest]
#[tokio::test]
async fn empty_names_are_refused(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let err = client.role_add("").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::RoleErrorKind::Unknown, "{err:?}");
    assert_eq!(
        err.grpc_status().map(|s| s.message()),
        Some("etcdserver: role name is empty"),
        "{err:?}"
    );

    let err = client.user_add("").password("pw").await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::UserErrorKind::Authentication, "{err:?}");
    assert_eq!(
        err.grpc_status().map(|s| s.message()),
        Some("etcdserver: user name is empty"),
        "{err:?}"
    );
}

/// Watch and keep-alive streams used to open with no token, so etcd refused every watch and ended
/// every keep-alive stream with "user name is empty".
#[rstest]
#[case::credentials(RootAuth::Credentials)]
#[case::token(RootAuth::Token)]
#[tokio::test]
async fn watch_under_auth(etcd_server: EtcdServer, #[case] auth: RootAuth) {
    let root = enable_auth(&etcd_server).await;
    let revision = root.put("w").value("1").await.unwrap().header().revision();
    let client = root_client(&etcd_server, &root, auth).await;

    let mut watcher = client.watch().key("w").start_revision(revision).start();
    let (_, record) = next_put(&mut watcher).await;
    assert_eq!(record.value(), &b"1"[..]);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

#[rstest]
#[case::credentials(RootAuth::Credentials)]
#[case::token(RootAuth::Token)]
#[tokio::test]
async fn keep_alive_under_auth(etcd_server: EtcdServer, #[case] auth: RootAuth) {
    let root = enable_auth(&etcd_server).await;
    let lease = root.grant_lease().ttl(Duration::from_secs(60)).await.unwrap().lease_id;
    let client = root_client(&etcd_server, &root, auth).await;

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease);
    let response = next_keep_alive(&mut keeper).await.unwrap();
    assert_eq!(response.lease_id, lease);
    assert!(response.ttl.is_some(), "{response:?}");

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A client can hold a token etcd no longer accepts, and etcd checks a watch stream's token only
/// when it creates a watch. A watcher that opened its stream with such a token replaces the stream
/// with one that carries a refreshed token.
#[rstest]
#[tokio::test]
async fn watch_opened_with_a_revoked_token(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");
    // Caches the token the watcher opens its stream with.
    let revision = client.put("w").value("1").await.unwrap().header().revision();
    revoke_root_tokens(&root).await;

    let mut watcher = client.watch().key("w").start_revision(revision).start();
    let (_, record) = next_put(&mut watcher).await;
    assert_eq!(record.value(), &b"1"[..]);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// The token of an open watch stream can stop working before etcd creates any watch on it.
#[rstest]
#[tokio::test]
async fn watch_added_after_its_token_was_revoked(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");
    let revision = client.put("w").value("1").await.unwrap().header().revision();

    let mut watcher = client.watch().start();
    // Opens the stream with the cached token.
    let pending = tokio::time::timeout(Duration::from_millis(100), watcher.next()).await;
    assert!(
        pending.is_err(),
        "expected nothing from a watcher with no watch, got {pending:?}"
    );
    revoke_root_tokens(&root).await;

    let id = watcher.add(Watch::new("w").start_revision(revision));
    let (watch_id, record) = next_put(&mut watcher).await;
    assert_eq!(watch_id, id);
    assert_eq!(record.value(), &b"1"[..]);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// etcd keeps delivering the watches it created after the token of their stream stops working, so
/// a watcher with a live watch keeps its stream, and etcd refuses the watches added to it.
#[rstest]
#[tokio::test]
async fn revoked_token_refuses_watches_beside_a_live_one(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");
    let live_revision = client.put("live").value("1").await.unwrap().header().revision();
    let refused_revision = client.put("refused").value("1").await.unwrap().header().revision();

    let mut watcher = client.watch().key("live").start_revision(live_revision).start();
    let (live_id, _) = next_put(&mut watcher).await;
    revoke_root_tokens(&root).await;

    let refused_id = watcher.add(Watch::new("refused").start_revision(refused_revision));
    let error = next_watch_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(error.watch_id(), Some(refused_id));
    assert_eq!(
        error.cancel_reason(),
        Some("rpc error: code = Unauthenticated desc = etcdserver: invalid auth token")
    );

    root.put("live").value("2").await.unwrap();
    let (watch_id, record) = next_put(&mut watcher).await;
    assert_eq!(watch_id, live_id);
    assert_eq!(record.value(), &b"2"[..]);

    // The client still holds the revoked token, which a new watcher replaces.
    let mut watcher = client.watch().key("refused").start_revision(refused_revision).start();
    let (_, record) = next_put(&mut watcher).await;
    assert_eq!(record.key(), &b"refused"[..]);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// etcd refuses the first keep-alive on a stream whose token it does not accept by failing the call
/// that opens the stream.
#[rstest]
#[tokio::test]
async fn keep_alive_opened_with_a_revoked_token(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");
    // Caches the token the keeper opens its stream with.
    let lease = client
        .grant_lease()
        .ttl(Duration::from_secs(60))
        .await
        .unwrap()
        .lease_id;
    revoke_root_tokens(&root).await;

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease);
    assert_eq!(next_keep_alive(&mut keeper).await.unwrap().lease_id, lease);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// etcd checks a keep-alive stream's token on every keep-alive, and ends the stream at the first it
/// refuses. The keeper reopens it and sends again what etcd did not answer, in order and once.
#[rstest]
#[tokio::test]
async fn keep_alive_outlives_its_token(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");
    let first = client
        .grant_lease()
        .ttl(Duration::from_secs(60))
        .await
        .unwrap()
        .lease_id;
    let second = client
        .grant_lease()
        .ttl(Duration::from_secs(60))
        .await
        .unwrap()
        .lease_id;

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(first);
    assert_eq!(next_keep_alive(&mut keeper).await.unwrap().lease_id, first);
    revoke_root_tokens(&root).await;

    keeper.keep_alive(first);
    keeper.keep_alive(second);
    assert_eq!(next_keep_alive(&mut keeper).await.unwrap().lease_id, first);
    assert_eq!(next_keep_alive(&mut keeper).await.unwrap().lease_id, second);
    let extra = tokio::time::timeout(Duration::from_millis(200), keeper.next()).await;
    assert!(extra.is_err(), "expected no more answers, got {extra:?}");

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A pre-obtained token cannot be refreshed. Once etcd stops accepting it, it refuses the watches
/// a stream creates and ends a keep-alive stream.
#[rstest]
#[tokio::test]
async fn revoked_pre_obtained_token_fails_streams(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let revision = root.put("w").value("1").await.unwrap().header().revision();
    let lease = root.grant_lease().ttl(Duration::from_secs(60)).await.unwrap().lease_id;
    let client = client_with_token(&etcd_server, root.authenticate().await.unwrap().token());
    revoke_root_tokens(&root).await;

    let mut watcher = client.watch().key("w").start_revision(revision).start();
    let error = next_watch_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(
        error.cancel_reason(),
        Some("rpc error: code = Unauthenticated desc = etcdserver: invalid auth token")
    );

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease);
    let error = next_keep_alive(&mut keeper).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), KeepAliveErrorKind::Authentication);
    let end = tokio::time::timeout(Duration::from_secs(5), keeper.next()).await;
    assert!(matches!(end, Ok(None)), "expected the keeper to end, got {end:?}");

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A stream whose client cannot authenticate opens without a token, and etcd's refusal ends it at
/// once.
#[rstest]
#[tokio::test]
async fn wrong_credentials_fail_streams(etcd_server: EtcdServer) {
    let root = enable_auth(&etcd_server).await;
    let lease = root.grant_lease().ttl(Duration::from_secs(60)).await.unwrap().lease_id;
    let client = client_with_credentials(&etcd_server, "root", "wrong");

    let mut watcher = client.watch().key("w").start();
    let error = next_watch_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(
        error.cancel_reason(),
        Some("rpc error: code = InvalidArgument desc = etcdserver: user name is empty")
    );

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease);
    let error = next_keep_alive(&mut keeper).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), KeepAliveErrorKind::Authentication);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

/// A client authenticates before it opens a stream, and etcd refuses to authenticate anyone while
/// auth is disabled. The streams work without a token.
#[rstest]
#[tokio::test]
async fn credentials_without_auth_enabled(etcd_server: EtcdServer) {
    let plain = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    let revision = plain.put("w").value("1").await.unwrap().header().revision();
    let lease = plain.grant_lease().ttl(Duration::from_secs(60)).await.unwrap().lease_id;
    let client = client_with_credentials(&etcd_server, "root", "rootpw");

    let mut watcher = client.watch().key("w").start_revision(revision).start();
    let (_, record) = next_put(&mut watcher).await;
    assert_eq!(record.value(), &b"1"[..]);

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease);
    assert_eq!(next_keep_alive(&mut keeper).await.unwrap().lease_id, lease);
}

/// etcd refuses a watch over a range the client may not read. #21 left this cause out of its
/// refusal tests because no watch carried a token then.
#[rstest]
#[tokio::test]
async fn watch_refused_without_permission_alone(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;
    let eve = client_with_credentials(&etcd_server, "eve", "evepw");

    let mut watcher = eve.watch().key("secret/x").start();
    let error = next_watch_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    // Watches given to the builder are numbered from 1.
    assert_eq!(error.watch_id(), WatchId::new(1));
    assert_eq!(
        error.cancel_reason(),
        Some("rpc error: code = PermissionDenied desc = etcdserver: permission denied")
    );

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn watch_refused_without_permission_beside_live_watch(etcd_server: EtcdServer) {
    let root = enable_auth_with_reader(&etcd_server).await;
    let revision = root
        .get("pub/x")
        .await
        .unwrap()
        .into_record()
        .unwrap()
        .metadata()
        .modified_revision;
    let eve = client_with_credentials(&etcd_server, "eve", "evepw");

    let mut watcher = eve.watch().key("pub/x").start_revision(revision).start();
    let (live_id, _) = next_put(&mut watcher).await;

    let refused_id = watcher.add(Watch::new("secret/x"));
    let error = next_watch_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(error.watch_id(), Some(refused_id));
    assert_eq!(
        error.cancel_reason(),
        Some("rpc error: code = PermissionDenied desc = etcdserver: permission denied")
    );

    root.put("pub/x").value("again").await.unwrap();
    let (watch_id, record) = next_put(&mut watcher).await;
    assert_eq!(watch_id, live_id);
    assert_eq!(record.value(), &b"again"[..]);

    // Cleanup: disable auth
    root.auth_disable().await.unwrap();
}
