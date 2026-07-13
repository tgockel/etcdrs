use etcdrs::{Permission, Prefix, TargetRange};
use etcdrs_test::{EtcdServer, etcd_server};
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
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    enable_auth_with_root(&client).await;
    let root = client_with_credentials(&etcd_server, "root", "rootpw");

    root.user_add("eve").password("evepw").await.unwrap();
    root.role_add("reader").await.unwrap();
    root.role_grant_permission("reader", Permission::read(Prefix("pub/")))
        .await
        .unwrap();
    root.user_grant_role("eve", "reader").await.unwrap();

    root.put("pub/x").value("visible").await.unwrap();
    root.put("secret/x").value("hidden").await.unwrap();

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
