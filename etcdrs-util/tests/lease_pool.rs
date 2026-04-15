use std::time::Duration;

use etcdrs::Client;
use etcdrs_test::{EtcdServer, etcd_server};
use etcdrs_util::lease_pool::LeasePool;
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn get_lease_pools_by_ttl(etcd_server: EtcdServer) {
    let client = Client::new(&etcd_server.connect_string()).unwrap();
    let pool = LeasePool::new(client);

    let a = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    let b = pool.get_lease(Duration::from_secs(10)).await.unwrap();
    assert_eq!(a, b, "same TTL should return the same pooled lease");
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
