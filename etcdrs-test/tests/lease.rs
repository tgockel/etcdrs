use std::time::Duration;

use etcdrs::{Client, GrantLeaseErrorKind, RevokeLeaseErrorKind};
use etcdrs_test::{EtcdCluster, etcd_cluster};
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
