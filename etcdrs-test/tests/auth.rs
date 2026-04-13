use etcdrs_test::{EtcdServer, etcd_server};
use rstest::rstest;

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
