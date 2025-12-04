use std::time::Duration;

use etcdrs::{Client, ErrorKind};
use rstest::rstest;

use crate::{tests::etcd_cluster, EtcdCluster};

#[rstest]
#[tokio::test]
async fn leases(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let lease_info = client
        .grant_lease()
        .ttl(Duration::from_secs(1))
        .await
        .expect("Should have granted lease");
    println!("got lease: {lease_info:?}");

    let retry_err = client
        .grant_lease()
        .lease_id(lease_info.lease_id)
        .await
        .expect_err("trying to use the same lease ID");
    assert_eq!(retry_err.kind(), ErrorKind::FailedPrecondition);

    client.put("foo").value("bar").lease(lease_info.lease_id).await.unwrap();

    for trial in 1..1000 {
        let foo = client.get("foo").await.unwrap();
        println!("{trial}: {foo:?}");
        if foo.is_none() {
            break;
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let revoke_err = client.revoke_lease(lease_info.lease_id).await.unwrap_err();
    assert_eq!(revoke_err.kind(), ErrorKind::NotFound, "{revoke_err:?}");
}
