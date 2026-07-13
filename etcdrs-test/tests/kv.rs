use std::{sync::Arc, time::Duration};

use etcdrs::{CompactErrorKind, Revision, Version, WatchErrorKind};
use etcdrs_test::{EtcdCluster, EtcdServer, etcd_cluster, etcd_server};
use futures::StreamExt;
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn get_put_get(etcd_cluster: EtcdCluster) {
    let client = etcdrs::Client::new(&etcd_cluster.connect_string()).unwrap();
    assert!(client.get("foo").await.unwrap().into_record().is_none());

    client.put("foo").value("value").await.unwrap();
    let fetched = client.get("foo").await.unwrap().into_record().unwrap();
    assert_eq!(fetched.value(), &b"value"[..]);
    assert_eq!(fetched.metadata().version, Version::new(1));
}

#[rstest]
#[tokio::test]
async fn list_basics(etcd_server: EtcdServer) {
    let metrics = Arc::new(etcdrs::client::RequestCounter::default());
    let client = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .metrics(metrics.clone())
        .build()
        .unwrap();

    let things = [("foo/a", b"a"), ("foo/b", b"b"), ("foo/c", b"c"), ("foo/d", b"d")];
    for (key, value) in things.iter() {
        client.put(*key).value(*value).await.unwrap();
    }
    assert_eq!(4, metrics.get().succeeded());

    let count = client.list_prefix("foo/").count_only().await.unwrap().count();
    assert_eq!(count, things.len());
    assert_eq!(5, metrics.get().succeeded());
    let keys = client
        .list("foo/a"..="foo/d")
        .keys_only()
        .limit(2)
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(count, keys.len());
    assert_eq!(7, metrics.get().succeeded()); // NOTE: `.limit(2)` above takes 2 requests to list 4 records
}

#[rstest]
#[tokio::test]
async fn list_pagination_consistent_revision(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    for (key, value) in [("foo/a", "a"), ("foo/b", "b"), ("foo/c", "c"), ("foo/d", "d")] {
        client.put(key).value(value).await.unwrap();
    }

    // Page size of 1 — each poll triggers a separate RangeRequest
    let mut stream = client.list_prefix("foo/").limit(1).await.unwrap().into_stream();

    // Pull the first item — this pins the revision
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.key(), &b"foo/a"[..]);
    assert_eq!(first.value(), &b"a"[..]);

    // Modify a key that hasn't been returned yet
    client.put("foo/c").value("modified").await.unwrap();

    // Remaining pages should read from the pinned revision and see the old value
    let remaining: Vec<_> = stream.map(Result::unwrap).collect().await;
    let values: Vec<&[u8]> = remaining.iter().map(|r| &r.value()[..]).collect();
    assert_eq!(values, vec![b"b", b"c", b"d"]);
}

#[rstest]
#[tokio::test]
async fn create_delete_get(etcd_server: EtcdServer) {
    let metrics = Arc::new(etcdrs::client::RequestCounter::default());
    let client = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .metrics(metrics.clone())
        .build()
        .unwrap();

    assert!(client.get("foo").await.unwrap().into_record().is_none());
    client.put("foo").value("value").await.unwrap();
    let fetched = client.get("foo").await.unwrap().into_record().unwrap();
    assert_eq!(fetched.value(), &b"value"[..]);
    assert_eq!(fetched.metadata().version, Version::new(1));

    assert!(client.delete("foo").await.unwrap().deleted());
    assert!(!client.delete("foo").await.unwrap().deleted());
}

#[rstest]
#[tokio::test]
async fn delete_prefix(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    for key in ["foo/a", "foo/b", "foo/c", "bar/a"] {
        client.put(key).value(key).await.unwrap();
    }

    assert_eq!(client.delete_prefix("foo/").await.unwrap().deleted(), 3);

    // Verify the prefixed keys are gone
    assert!(client.get("foo/a").await.unwrap().record().is_none());
    assert!(client.get("foo/b").await.unwrap().record().is_none());
    assert!(client.get("foo/c").await.unwrap().record().is_none());
    // bar/a should still exist
    assert!(client.get("bar/a").await.unwrap().record().is_some());
}

#[rstest]
#[tokio::test]
async fn delete_range(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    for key in ["foo/a", "foo/b", "foo/c", "foo/d"] {
        client.put(key).value(key).await.unwrap();
    }

    // "foo/a".."foo/c" should delete "foo/a" and "foo/b" (exclusive upper bound)
    assert_eq!(client.delete_range("foo/a".."foo/c").await.unwrap().deleted(), 2);

    assert!(client.get("foo/a").await.unwrap().record().is_none());
    assert!(client.get("foo/b").await.unwrap().record().is_none());
    assert!(client.get("foo/c").await.unwrap().record().is_some());
    assert!(client.get("foo/d").await.unwrap().record().is_some());
}

#[rstest]
#[tokio::test]
async fn delete_range_get_previous(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    for key in ["foo/a", "foo/b", "foo/c"] {
        client.put(key).value(key).await.unwrap();
    }

    let response = client.delete_prefix("foo/").get_previous().await.unwrap();
    let previous = response.previous();
    assert_eq!(previous.len(), 3);
    let keys: Vec<&[u8]> = previous.iter().map(|r| &r.key()[..]).collect();
    assert_eq!(keys, vec![b"foo/a", b"foo/b", b"foo/c"]);
}

#[rstest]
#[tokio::test]
async fn get_at_revision(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let rev1 = client.put("gar/key").value("v1").await.unwrap().header().revision();
    let rev2 = client.put("gar/key").value("v2").await.unwrap().header().revision();
    client.put("gar/other").value("x").await.unwrap();

    // A plain get reads the current value; a pinned get reads the historical one.
    let current = client.get("gar/key").await.unwrap().into_record().unwrap();
    assert_eq!(current.value(), &b"v2"[..]);
    let old = client
        .get("gar/key")
        .at_revision(rev1)
        .await
        .unwrap()
        .into_record()
        .unwrap();
    assert_eq!(old.value(), &b"v1"[..]);

    // A key that did not exist yet at the pinned revision is absent.
    let absent = client.get("gar/other").at_revision(rev2).await.unwrap();
    assert!(absent.record().is_none());

    // Reading a revision the server has not reached yet fails.
    let future_rev = etcdrs::Revision::new(rev2.get() + 1000).unwrap();
    let err = client.get("gar/key").at_revision(future_rev).await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::GetErrorKind::FutureRevision);
}

#[rstest]
#[tokio::test]
async fn compact(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    // Build up some revision history.
    let first_rev = client.put("compact-key").value("v1").await.unwrap().header().revision();
    let mid_rev = client.put("compact-key").value("v2").await.unwrap().header().revision();
    let last_rev = client.put("compact-key").value("v3").await.unwrap().header().revision();

    // Compaction succeeds in both the default and physical modes, and the current value survives.
    client.compact(mid_rev).await.unwrap();
    client.compact(last_rev).physical().await.unwrap();
    let fetched = client.get("compact-key").await.unwrap().into_record().unwrap();
    assert_eq!(fetched.value(), &b"v3"[..]);

    // Compacting the same revision again fails as already-compacted.
    let err = client.compact(last_rev).await.unwrap_err();
    assert_eq!(err.kind(), CompactErrorKind::CompactedRevision);

    // Compacting a revision the server does not have yet fails as a future revision.
    let future_rev = Revision::new(last_rev.get() + 1_000_000).unwrap();
    let err = client.compact(future_rev).await.unwrap_err();
    assert_eq!(err.kind(), CompactErrorKind::FutureRevision);

    // History from before the compacted revision is no longer readable...
    let err = client.get("compact-key").at_revision(first_rev).await.unwrap_err();
    assert_eq!(err.kind(), etcdrs::GetErrorKind::CompactedRevision);

    // ...nor watchable.
    let mut watcher = client.watch().key("compact-key").start_revision(first_rev).start();
    let err = tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out waiting for watch error")
        .expect("watch stream ended unexpectedly")
        .expect_err("watching from a compacted revision should fail");
    assert_eq!(err.kind(), WatchErrorKind::Compacted);
    assert_eq!(err.compact_revision(), Some(last_rev.get()));
}

#[rstest]
#[tokio::test]
async fn data_persists_across_restart(mut etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    client.put("foo").value("bar").await.unwrap();

    etcd_server.stop().unwrap();
    etcd_server.start().unwrap();

    let record = client.get("foo").await.unwrap().into_record().unwrap();
    assert_eq!(record.value(), &b"bar"[..]);
}
