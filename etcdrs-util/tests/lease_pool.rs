use std::sync::Arc;
use std::time::Duration;

use etcdrs::client::RequestCounter;
use etcdrs::{Client, GrantLeaseErrorKind};
use etcdrs_test::{EtcdServer, etcd_server};
use etcdrs_util::lease_pool::LeasePool;
use rstest::rstest;
use tokio::task::JoinSet;

#[rstest]
#[tokio::test]
async fn get_lease_pools_by_ttl(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client);

    let a = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    let b = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    assert_eq!(a, b, "same TTL should return the same pooled lease");
}

/// Single-node deliberately: `leases()` lists only the leases of the member that answers it.
#[rstest]
#[tokio::test]
async fn concurrent_get_lease_grants_one_lease(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client.clone());

    let mut tasks = JoinSet::new();
    for _ in 0..10 {
        let pool = pool.clone();
        tasks.spawn(async move { pool.get_lease(Duration::from_secs(10)).await.unwrap() });
    }
    let lease_ids = tasks.join_all().await;

    assert!(lease_ids.iter().all(|&id| id == lease_ids[0]), "{lease_ids:?}");
    assert_eq!(client.leases().await.unwrap().into_leases(), [lease_ids[0]]);
}

#[rstest]
#[tokio::test]
async fn concurrent_get_lease_shares_a_refused_grant(etcd_server: EtcdServer) {
    let metrics = Arc::new(RequestCounter::default());
    let client = Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .metrics(metrics.clone())
        .build()
        .unwrap();
    let pool = LeasePool::new(client);
    // One second over etcd's maximum lease TTL.
    let ttl = Duration::from_secs(9_000_000_001);

    let mut tasks = JoinSet::new();
    for _ in 0..10 {
        let pool = pool.clone();
        tasks.spawn(async move { pool.get_lease(ttl).await.unwrap_err() });
    }
    for err in tasks.join_all().await {
        assert_eq!(err.kind(), GrantLeaseErrorKind::TtlTooLarge, "{err:?}");
        assert_eq!(
            err.grpc_status().map(|s| s.message()),
            Some("etcdserver: too large lease TTL")
        );
    }
    assert_eq!(metrics.get().requested(), 1, "concurrent calls should share one grant");

    let err = pool.get_lease(ttl).await.unwrap_err();
    assert_eq!(err.kind(), GrantLeaseErrorKind::TtlTooLarge, "{err:?}");
    assert_eq!(metrics.get().requested(), 2, "the next call should grant again");
}

#[rstest]
#[tokio::test]
async fn get_lease_different_ttls(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client);

    let a = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    let b = pool.get_lease(Duration::from_secs(20)).await.unwrap();
    assert_ne!(a, b, "different TTLs should return different leases");
}

#[rstest]
#[tokio::test]
async fn grant_lease_always_unique(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client);

    let pooled = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    let granted = pool.grant_lease(Duration::from_secs(10)).await.unwrap();
    assert_ne!(pooled, granted, "grant_lease should always create a new lease");
}

#[rstest]
#[tokio::test]
async fn keep_alive(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client.clone());

    let lease_id = pool.get_lease(Duration::from_secs(2)).await.unwrap();
    client.put("ka/pool").value("val").lease(lease_id).await.unwrap();

    // Wait well past the original 2s TTL — the pool should keep the lease alive.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let record = client.get("ka/pool").await.unwrap();
    assert!(
        record.record().is_some(),
        "key should still exist due to pool keep-alive"
    );
}

#[rstest]
#[tokio::test]
async fn revoke_pooled_lease(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client.clone());

    let lease_id = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    client.put("revoke/key").value("val").lease(lease_id).await.unwrap();

    pool.revoke_lease(lease_id).await.unwrap();

    let record = client.get("revoke/key").await.unwrap();
    assert!(record.record().is_none(), "key should be gone after revoking its lease");

    // A subsequent get_lease with the same TTL should return a new lease.
    let new_lease = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    assert_ne!(
        lease_id, new_lease,
        "should get a fresh lease after revoking the old one"
    );
}

#[rstest]
#[tokio::test]
async fn drop_stops_keepalive(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client.clone());

    let lease_id = pool.get_lease(Duration::from_secs(2)).await.unwrap();
    client.put("drop/key").value("val").lease(lease_id).await.unwrap();

    drop(pool);

    // Wait for the lease to expire without keep-alive.
    for _ in 0..100 {
        if client.get("drop/key").await.unwrap().record().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("key should have expired after dropping the pool");
}

/// The keep-alive stream used to carry no token, so etcd ended it at its first keep-alive, the pool
/// reconnected every 100 ms, and its leases expired. Root's tokens are revoked partway as well: a
/// stream that reconnected with the client's revoked token would not get past etcd either.
#[rstest]
#[tokio::test]
async fn keep_alive_under_auth(etcd_server: EtcdServer) {
    let setup = Client::new(&etcd_server.connect_string()).unwrap();
    setup.user_add("root").password("rootpw").await.unwrap();
    setup.role_add("root").await.unwrap();
    setup.user_grant_role("root", "root").await.unwrap();
    setup.auth_enable().await.unwrap();
    let root = || {
        Client::builder()
            .add_connection(etcd_server.connect_string())
            .unwrap()
            .credentials("root", "rootpw")
            .build()
            .unwrap()
    };
    let (admin, client) = (root(), root());
    let pool = LeasePool::new(client.clone());

    let lease_id = pool.get_lease(Duration::from_secs(2)).await.unwrap();
    client.put("ka/auth").value("val").lease(lease_id).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // etcd revokes a user's tokens whenever their password changes, even to the one they have.
    admin.user_change_password("root", "rootpw").await.unwrap();

    // Wait well past the original 2s TTL — the pool should keep the lease alive.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let record = admin.get("ka/auth").await.unwrap();
    assert!(
        record.record().is_some(),
        "key should still exist due to pool keep-alive"
    );

    // Cleanup: disable auth
    admin.auth_disable().await.unwrap();
}
