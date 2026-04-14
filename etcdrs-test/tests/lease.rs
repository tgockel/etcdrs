use std::time::Duration;

use etcdrs::{Client, GrantLeaseErrorKind, RevokeLeaseErrorKind};
use etcdrs_test::{EtcdCluster, etcd_cluster};
use futures::StreamExt;
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn leases(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let lease = client
        .grant_lease()
        .ttl(Duration::from_secs(1))
        .await
        .expect("Should have granted lease");
    println!("got lease: {:?}", lease.info());

    let retry_err = client
        .grant_lease()
        .lease_id(lease.lease_id)
        .await
        .expect_err("trying to use the same lease ID");
    assert_eq!(retry_err.kind(), GrantLeaseErrorKind::LeaseExists);

    client.put("foo").value("bar").lease(lease.lease_id).await.unwrap();

    for trial in 1..1000 {
        let foo = client.get("foo").await.unwrap();
        println!("{trial}: {foo:?}");
        if foo.record().is_none() {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let revoke_err = client.revoke_lease(lease.lease_id).await.unwrap_err();
    assert_eq!(revoke_err.kind(), RevokeLeaseErrorKind::NotFound, "{revoke_err:?}");
}

#[rstest]
#[tokio::test]
async fn keep_alive(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let lease = client
        .grant_lease()
        .ttl(Duration::from_secs(2))
        .await
        .expect("Should have granted lease");

    client.put("ka/key").value("val").lease(lease.lease_id).await.unwrap();

    let mut keeper = client.lease_keeper();

    // Send keep-alive requests and consume responses, keeping the lease alive past its original TTL.
    for _ in 0..4 {
        keeper.keep_alive(lease.lease_id);
        let resp = tokio::time::timeout(Duration::from_secs(5), keeper.next())
            .await
            .expect("timed out waiting for keep-alive response")
            .expect("keep-alive stream ended unexpectedly")
            .expect("keep-alive error");
        assert_eq!(resp.lease_id, lease.lease_id);
        assert!(resp.ttl.is_some(), "TTL should be present for a live lease");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // The key should still exist — we've been keeping the lease alive for ~4 seconds with a 2s TTL.
    let record = client.get("ka/key").await.unwrap();
    assert!(record.record().is_some(), "key should still exist due to keep-alive");

    // Stop keeping alive and wait for expiry.
    drop(keeper);
    for _ in 1..100 {
        if client.get("ka/key").await.unwrap().record().is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("key should have expired after dropping the keeper");
}

#[rstest]
#[tokio::test]
async fn keep_alive_expired_lease(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let lease = client
        .grant_lease()
        .ttl(Duration::from_secs(2))
        .await
        .expect("Should have granted lease");

    // Wait for the lease to expire.
    tokio::time::sleep(Duration::from_secs(4)).await;

    let mut keeper = client.lease_keeper();
    keeper.keep_alive(lease.lease_id);

    let resp = tokio::time::timeout(Duration::from_secs(5), keeper.next())
        .await
        .expect("timed out waiting for keep-alive response")
        .expect("keep-alive stream ended unexpectedly")
        .expect("keep-alive should not error for expired lease");
    assert_eq!(resp.lease_id, lease.lease_id);
    assert_eq!(resp.ttl, None, "expired lease should have no TTL");
}
