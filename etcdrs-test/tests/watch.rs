use std::time::Duration;

use etcdrs::client::{Watch, WatchEvent};
use etcdrs::{Revision, WatchError, WatchErrorKind, WatchId};
use etcdrs_test::{EtcdServer, etcd_server};
use futures::StreamExt;
use rstest::rstest;

/// Helper: the next item from a watcher, with a timeout.
async fn next_item(watcher: &mut etcdrs::client::Watcher) -> Result<WatchEvent, WatchError> {
    tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out waiting for watch event")
        .expect("watch stream ended unexpectedly")
}

/// Helper: collect the next `n` events from a watcher, with a timeout.
async fn next_events(watcher: &mut etcdrs::client::Watcher, n: usize) -> Vec<WatchEvent> {
    let mut events = Vec::with_capacity(n);
    for _ in 0..n {
        events.push(next_item(watcher).await.expect("watch stream yielded an error"));
    }
    events
}

/// Helper: get the modified_revision of a key.
async fn revision_of(client: &etcdrs::Client, key: &str) -> etcdrs::Revision {
    client
        .get(key)
        .await
        .unwrap()
        .into_record()
        .unwrap()
        .metadata()
        .modified_revision
}

#[rstest]
#[tokio::test]
async fn watch_put(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("watch-put").value("hello").await.unwrap();
    let rev = revision_of(&client, "watch-put").await;

    let mut watcher = client.watch().key("watch-put").start_revision(rev).start();

    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, .. } = &events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };
    assert_eq!(record.key(), &b"watch-put"[..]);
    assert_eq!(record.value(), &b"hello"[..]);
}

#[rstest]
#[tokio::test]
async fn watch_delete(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("watch-del").value("val").await.unwrap();
    let rev = revision_of(&client, "watch-del").await;

    let mut watcher = client.watch().key("watch-del").start_revision(rev).start();

    client.delete("watch-del").await.unwrap();

    let events = next_events(&mut watcher, 2).await;
    assert!(matches!(&events[0], WatchEvent::Put { .. }));
    let WatchEvent::Delete { key, .. } = &events[1] else {
        panic!("expected Delete, got {:?}", events[1]);
    };
    assert_eq!(key.key(), &b"watch-del"[..]);
}

#[rstest]
#[tokio::test]
async fn watch_prefix(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("wp/a").value("1").await.unwrap();
    let rev = revision_of(&client, "wp/a").await;

    client.put("wp/b").value("2").await.unwrap();
    client.put("wp/c").value("3").await.unwrap();

    let mut watcher = client.watch().prefix("wp/").start_revision(rev).start();

    let events = next_events(&mut watcher, 3).await;
    let keys: Vec<&[u8]> = events
        .iter()
        .map(|e| match e {
            WatchEvent::Put { record, .. } => &record.key()[..],
            other => panic!("expected Put, got {other:?}"),
        })
        .collect();
    assert_eq!(keys, vec![b"wp/a" as &[u8], b"wp/b", b"wp/c"]);
}

#[rstest]
#[tokio::test]
async fn watch_get_previous(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("watch-prev").value("v1").await.unwrap();
    let rev = revision_of(&client, "watch-prev").await;

    client.put("watch-prev").value("v2").await.unwrap();

    let mut watcher = client
        .watch()
        .key("watch-prev")
        .get_previous()
        .start_revision(rev)
        .start();

    let events = next_events(&mut watcher, 2).await;

    // First put has no previous record (key didn't exist before that revision).
    let WatchEvent::Put { prev_record: prev0, .. } = &events[0] else {
        panic!("expected Put");
    };
    assert!(prev0.is_none());

    // Second put has the previous record.
    let WatchEvent::Put { prev_record: prev1, .. } = &events[1] else {
        panic!("expected Put");
    };
    assert_eq!(prev1.as_ref().expect("expected prev_record").value(), &b"v1"[..]);
}

#[rstest]
#[tokio::test]
async fn watch_start_revision(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("watch-rev").value("before").await.unwrap();
    let rev = revision_of(&client, "watch-rev").await;

    let mut watcher = client.watch().key("watch-rev").start_revision(rev).start();

    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, .. } = &events[0] else {
        panic!("expected Put");
    };
    assert_eq!(record.value(), &b"before"[..]);
}

#[rstest]
#[tokio::test]
async fn watch_multiple_targets(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("wm/a").value("1").await.unwrap();
    let rev = revision_of(&client, "wm/a").await;
    client.put("wm/b").value("2").await.unwrap();

    let mut watcher = client
        .watch()
        .key("wm/a")
        .start_revision(rev)
        .key("wm/b")
        .start_revision(rev)
        .start();

    let events = next_events(&mut watcher, 2).await;

    let WatchEvent::Put {
        record: r0,
        watch_id: w0,
        ..
    } = &events[0]
    else {
        panic!("expected Put, got {:?}", events[0]);
    };
    let WatchEvent::Put {
        record: r1,
        watch_id: w1,
        ..
    } = &events[1]
    else {
        panic!("expected Put, got {:?}", events[1]);
    };
    let mut keys = [r0.key().as_ref(), r1.key().as_ref()];
    keys.sort();
    assert_eq!(keys, [&b"wm/a"[..], &b"wm/b"[..]]);
    assert_ne!(w0, w1);
}

#[rstest]
#[tokio::test]
async fn watch_add_dynamic(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("wd/initial").value("1").await.unwrap();
    let rev = revision_of(&client, "wd/initial").await;

    let mut watcher = client.watch().key("wd/initial").start_revision(rev).start();

    let events = next_events(&mut watcher, 1).await;
    assert!(matches!(&events[0], WatchEvent::Put { record, .. } if record.key() == &b"wd/initial"[..]));

    client.put("wd/dynamic").value("2").await.unwrap();
    let dyn_rev = revision_of(&client, "wd/dynamic").await;
    let dynamic_id = watcher.add(Watch::new("wd/dynamic").start_revision(dyn_rev));

    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, watch_id, .. } = &events[0] else {
        panic!("expected Put");
    };
    assert_eq!(record.key(), &b"wd/dynamic"[..]);
    assert_eq!(*watch_id, dynamic_id);
}

#[rstest]
#[tokio::test]
async fn watch_cancel(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("wc/keep").value("init").await.unwrap();
    let rev = revision_of(&client, "wc/keep").await;

    let mut watcher = client.watch().key("wc/keep").start_revision(rev).start();

    let events = next_events(&mut watcher, 1).await;
    assert!(matches!(&events[0], WatchEvent::Put { .. }));

    let cancel_id = watcher.add(Watch::new("wc/cancel"));
    watcher.cancel(cancel_id);

    let result = tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out")
        .expect("stream ended");
    assert_eq!(result.unwrap_err().kind(), etcdrs::WatchErrorKind::Canceled);

    client.put("wc/keep").value("still alive").await.unwrap();
    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, .. } = &events[0] else {
        panic!("expected Put");
    };
    assert_eq!(record.key(), &b"wc/keep"[..]);
    assert_eq!(record.value(), &b"still alive"[..]);
}

#[rstest]
#[tokio::test]
async fn watch_request_progress(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    // Write a key so the store has a non-zero revision.
    client.put("wrp/key").value("val").await.unwrap();
    let rev = revision_of(&client, "wrp/key").await;

    let mut watcher = client.watch().key("wrp/key").start_revision(rev).start();

    // Consume the replayed put event to ensure the stream is established.
    let events = next_events(&mut watcher, 1).await;
    assert!(matches!(&events[0], WatchEvent::Put { .. }));

    // Request progress — the server should reply with the current revision.
    watcher.request_progress();

    let event = tokio::time::timeout(Duration::from_secs(5), watcher.next())
        .await
        .expect("timed out waiting for progress")
        .expect("stream ended")
        .expect("stream yielded an error");

    let WatchEvent::Progress { revision, .. } = event else {
        panic!("expected Progress, got {event:?}");
    };
    assert!(revision >= rev);
}

/// etcd refuses a watch with one response that is both `created` and `canceled`, for watch ID -1.
/// The stream used to skip every `created` response, so a refused watch that was the watcher's only
/// one left `next()` pending forever.
#[rstest]
#[case::inverted_range(Watch::new("b".."a"), "mvcc: watcher range is empty")]
#[case::negative_start_revision(
    Watch::new("refused").start_revision(Revision::new(-1).unwrap()),
    "etcdserver: mvcc: required revision has been compacted"
)]
#[tokio::test]
async fn watch_refused_alone(etcd_server: EtcdServer, #[case] refused: Watch, #[case] reason: &str) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let mut watcher = client.watch().add(refused).start();

    let error = next_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    // Watches given to the builder are numbered from 1.
    assert_eq!(error.watch_id(), WatchId::new(1));
    assert_eq!(error.cancel_reason(), Some(reason));
}

/// A refusal answers the oldest create request etcd has not answered yet. It used to be dropped, so
/// the ID that `add` returned never got an error. The other watches keep delivering events.
#[rstest]
#[case::inverted_range(Watch::new("b".."a"), "mvcc: watcher range is empty")]
#[case::negative_start_revision(
    Watch::new("refused").start_revision(Revision::new(-1).unwrap()),
    "etcdserver: mvcc: required revision has been compacted"
)]
#[tokio::test]
async fn watch_refused_beside_live_watch(etcd_server: EtcdServer, #[case] refused: Watch, #[case] reason: &str) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("wr/live").value("1").await.unwrap();
    let live_rev = revision_of(&client, "wr/live").await;
    client.put("wr/other").value("1").await.unwrap();
    let other_rev = revision_of(&client, "wr/other").await;

    let mut watcher = client.watch().key("wr/live").start_revision(live_rev).start();
    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { watch_id: live_id, .. } = events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };

    // Refused first, so the refusal is not for the most recent create.
    let refused_id = watcher.add(refused);
    let other_id = watcher.add(Watch::new("wr/other").start_revision(other_rev));

    // etcd answers creates in order, and holds a watch's events until its create is answered.
    let error = next_item(&mut watcher).await.expect_err("expected the refusal");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(error.watch_id(), Some(refused_id));
    assert_eq!(error.cancel_reason(), Some(reason));

    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, watch_id, .. } = &events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };
    assert_eq!(*watch_id, other_id);
    assert_eq!(record.key(), &b"wr/other"[..]);

    client.put("wr/live").value("2").await.unwrap();
    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, watch_id, .. } = &events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };
    assert_eq!(*watch_id, live_id);
    assert_eq!(record.value(), &b"2"[..]);
}
