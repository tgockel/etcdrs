use std::time::Duration;

use etcdrs::{Client, ClusterErrorKind, MemberId};
use etcdrs_test::{EtcdCluster, etcd_cluster};
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn lists_members(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let resp = client.member_list().await.expect("member_list should succeed");
    assert_eq!(resp.members().len(), 3, "expected 3 members, got {:?}", resp.members());
    for member in resp.members() {
        assert!(!member.name().is_empty(), "member name should be populated: {member:?}");
        assert!(
            !member.peer_urls().is_empty(),
            "peer URLs should be populated: {member:?}"
        );
        assert!(
            !member.client_urls().is_empty(),
            "client URLs should be populated: {member:?}"
        );
        assert!(!member.is_learner(), "fixture peers should not be learners: {member:?}");
    }
}

#[rstest]
#[tokio::test]
async fn linearizable_member_list(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let resp = client
        .member_list()
        .linearizable()
        .await
        .expect("linearizable member_list should succeed");
    assert_eq!(resp.members().len(), 3);
}

#[rstest]
#[tokio::test]
async fn remove_member(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let initial = client.member_list().await.unwrap();
    let target = initial.members().last().expect("cluster should have members").id();

    let after_remove = client
        .member_remove(target)
        .await
        .expect("member_remove should succeed");
    assert_eq!(after_remove.members().len(), 2);
    assert!(
        after_remove.members().iter().all(|m| m.id() != target),
        "removed member should not appear: {:?}",
        after_remove.members()
    );

    let listed = client.member_list().await.unwrap();
    assert_eq!(listed.members().len(), 2);
}

#[rstest]
#[tokio::test]
async fn member_remove_unknown_id_is_not_found(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let bogus = MemberId::new(0xdead_beef_dead_beef).unwrap();
    let err = client
        .member_remove(bogus)
        .await
        .expect_err("removing an unknown member should fail");
    assert_eq!(err.kind(), ClusterErrorKind::MemberNotFound, "{err:?}");
}

#[rstest]
#[tokio::test]
async fn add_and_promote_learner(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();

    // Warm the cluster up with a write so all peer-to-peer connections are active. etcd's
    // strict-reconfig check otherwise rejects the member_add as "unhealthy cluster" if any peer
    // hasn't recently exchanged messages.
    client.put("warm").value("up").await.expect("warmup put should succeed");

    let joining_config = etcd_cluster.new_joining_peer();
    let peer_url = joining_config.peer_url();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let added = loop {
        match client.member_add([peer_url.clone()]).is_learner(true).await {
            Ok(resp) => break resp,
            Err(err) if err.kind() == ClusterErrorKind::UnhealthyCluster => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for cluster to become healthy: {err:?}",
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(err) => panic!("member_add should succeed: {err:?}"),
        }
    };
    let new_member_id = added.member().id();
    assert!(added.member().is_learner(), "newly added member should be a learner");
    assert!(added.member().peer_urls().contains(&peer_url));

    // Now start the new peer process so it can join the cluster as a learner.
    let _new_server = joining_config.start().expect("starting joining peer should succeed");

    // Wait for the new member to catch up: its `name` becomes populated once it joins.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listed = client.member_list().linearizable().await.unwrap();
        let new_member = listed
            .members()
            .iter()
            .find(|m| m.id() == new_member_id)
            .expect("new member should appear in member_list");
        if !new_member.name().is_empty() && !new_member.client_urls().is_empty() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for learner to catch up; last seen: {new_member:?}",
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Promote the learner. etcd may briefly return LearnerNotReady if the learner hasn't fully
    // caught up; retry until the deadline.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        match client.member_promote(new_member_id).await {
            Ok(resp) => {
                let promoted = resp
                    .members()
                    .iter()
                    .find(|m| m.id() == new_member_id)
                    .expect("promoted member should be present");
                assert!(
                    !promoted.is_learner(),
                    "member should no longer be a learner: {promoted:?}"
                );
                break;
            }
            Err(err) if err.kind() == ClusterErrorKind::LearnerNotReady => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for learner to be ready for promotion: {err:?}",
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(err) => panic!("member_promote failed: {err:?}"),
        }
    }
}

#[rstest]
#[tokio::test]
async fn update_member_peer_urls(etcd_cluster: EtcdCluster) {
    let client = Client::new(&etcd_cluster.connect_string()).unwrap();
    let initial = client.member_list().await.unwrap();
    let target = initial.members().first().expect("cluster should have members");
    let target_id = target.id();

    // Append an extra URL while preserving the existing ones so the peer remains reachable.
    let mut new_urls = target.peer_urls().to_vec();
    new_urls.push("http://127.0.0.1:65500".to_string());

    let updated = client
        .member_update(target_id, new_urls.clone())
        .await
        .expect("member_update should succeed");
    let updated_member = updated
        .members()
        .iter()
        .find(|m| m.id() == target_id)
        .expect("updated member should appear in response");
    assert_eq!(updated_member.peer_urls(), new_urls.as_slice());
}
