use std::time::Duration;

use etcdrs::client::{Watch, WatchEvent};
use etcdrs::{Revision, WatchError, WatchErrorKind, WatchId};
use etcdrs_test::{EtcdServer, etcd_server};
use futures::{FutureExt, StreamExt};
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

/// etcd keeps a compacted watch until the client cancels it, and answers no progress request on its
/// stream while it does. The stream never canceled one, so after a compaction, `request_progress`
/// went unanswered for as long as the stream lived.
#[rstest]
#[tokio::test]
async fn watch_request_progress_after_compaction(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let old = client.put("wpc/old").value("1").await.unwrap().header().revision();
    let head = client.put("wpc/live").value("1").await.unwrap().header().revision();
    client.compact(head).await.unwrap();

    let mut watcher = client.watch().key("wpc/live").start_revision(head).start();
    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { watch_id: live_id, .. } = events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };

    let compacted_id = watcher.add(Watch::new("wpc/old").start_revision(old));
    let error = next_item(&mut watcher).await.expect_err("expected the compaction");
    assert_eq!(error.kind(), WatchErrorKind::Compacted);
    assert_eq!(error.watch_id(), Some(compacted_id));

    watcher.request_progress();
    let event = next_item(&mut watcher).await.expect("watch stream yielded an error");
    let WatchEvent::Progress { revision, .. } = event else {
        panic!("expected Progress, got {event:?}");
    };
    assert!(revision >= head);

    // etcd confirms cancellations in the order it receives them, so a confirmation of the compacted
    // watch's cancellation would come first.
    watcher.cancel(live_id);
    let error = next_item(&mut watcher).await.expect_err("expected the cancellation");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(error.watch_id(), Some(live_id));
}

/// etcd splits a response of 2 MiB or more into fragments, which can split a revision. The events
/// of one were yielded as each fragment arrived, so those of the last fragment were not ready with
/// the others.
#[rstest]
#[tokio::test]
async fn watch_fragmented_response(etcd_server: EtcdServer) {
    // A window this small keeps etcd from sending the last fragment before the first is read.
    let client = etcdrs::Client::builder()
        .add_connection(etcd_server.connect_string())
        .unwrap()
        .configure_endpoint(|endpoint| Ok(endpoint.initial_stream_window_size(65_535)))
        .build()
        .unwrap();

    // A delete event carries its key and no value, so the keys are what make it large.
    for i in 0..25 {
        client
            .put(format!("wfr/{i:02}/{}", "x".repeat(100 * 1024)))
            .value("")
            .await
            .unwrap();
    }
    let sentinel = *client.put("wfr/sentinel").value("").await.unwrap().header();

    // etcd sends a watch's responses whole until it has announced the watch, which it has once the
    // watch delivers an event.
    let mut watcher = client
        .watch()
        .prefix("wfr/")
        .start_revision(sentinel.revision())
        .start();
    let events = next_events(&mut watcher, 1).await;
    assert!(matches!(&events[0], WatchEvent::Put { .. }));

    let deleted = *client.delete_prefix("wfr/").await.unwrap().header();
    let mut events = vec![next_item(&mut watcher).await.unwrap()];
    while let Some(Some(item)) = watcher.next().now_or_never() {
        events.push(item.unwrap());
    }
    assert_eq!(events.len(), 26);
    for event in &events {
        let WatchEvent::Delete { key, .. } = event else {
            panic!("expected Delete, got {event:?}");
        };
        assert_eq!(key.metadata().modified_revision, deleted.revision());
    }
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

/// A watch added before the stream's first poll, on a watcher with no other watch. The request used
/// to be dropped, so `next()` never returned.
#[rstest]
#[tokio::test]
async fn watch_add_before_first_poll(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("early/add").value("1").await.unwrap();
    let rev = revision_of(&client, "early/add").await;

    let mut watcher = client.watch().start();
    let id = watcher.add(Watch::new("early/add").start_revision(rev));

    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, watch_id, .. } = &events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };
    assert_eq!(*watch_id, id);
    assert_eq!(record.key(), &b"early/add"[..]);
}

/// A watch added and canceled before the stream's first poll. Both requests used to be dropped, so
/// the cancellation never arrived.
#[rstest]
#[tokio::test]
async fn watch_cancel_before_first_poll(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    let mut watcher = client.watch().key("early/keep").start();
    let canceled_id = watcher.add(Watch::new("early/cancel"));
    watcher.cancel(canceled_id);

    let error = next_item(&mut watcher).await.expect_err("expected the cancellation");
    assert_eq!(error.kind(), WatchErrorKind::Canceled);
    assert_eq!(error.watch_id(), Some(canceled_id));

    // etcd answers creates and cancels in the order it receives them, so the first watch exists.
    client.put("early/keep").value("1").await.unwrap();
    let events = next_events(&mut watcher, 1).await;
    let WatchEvent::Put { record, .. } = &events[0] else {
        panic!("expected Put, got {:?}", events[0]);
    };
    assert_eq!(record.key(), &b"early/keep"[..]);
}

/// A progress request before the stream's first poll. It used to be dropped, so no progress
/// notification arrived.
#[rstest]
#[tokio::test]
async fn watch_request_progress_before_first_poll(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("early/progress").value("1").await.unwrap();
    let rev = revision_of(&client, "early/progress").await;

    // etcd ignores a progress request while any watch on the stream is still catching up, and
    // answers none on a stream without watches. A watch with no start revision starts caught up.
    // etcd can still answer before it confirms the watch, and the stream drops such an answer, so
    // a new watcher asks again: asking on the same one would come after its first poll.
    let mut watchers = 0;
    let event = loop {
        let mut watcher = client.watch().key("early/progress").start();
        watcher.request_progress();
        if let Ok(item) = tokio::time::timeout(Duration::from_secs(1), watcher.next()).await {
            break item
                .expect("watch stream ended unexpectedly")
                .expect("watch stream yielded an error");
        }
        watchers += 1;
        assert!(watchers < 5, "no progress notification on 5 watchers");
    };
    let WatchEvent::Progress { revision, .. } = event else {
        panic!("expected Progress, got {event:?}");
    };
    assert!(revision >= rev);
}

/// A watch added through [`into_parts`][etcdrs::client::Watcher::into_parts] before the task that
/// polls the stream first runs. The request used to be dropped.
#[rstest]
#[tokio::test]
async fn watch_add_from_another_task_before_first_poll(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();

    client.put("early/task").value("1").await.unwrap();
    let rev = revision_of(&client, "early/task").await;

    let (sender, mut stream) = client.watch().start().into_parts();
    let first = tokio::spawn(async move { stream.next().await });
    // The test runtime has one thread, so the task cannot poll the stream before `add` runs.
    let id = sender.add(Watch::new("early/task").start_revision(rev));

    let event = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("timed out waiting for watch event")
        .unwrap()
        .expect("watch stream ended unexpectedly")
        .expect("watch stream yielded an error");
    let WatchEvent::Put { record, watch_id, .. } = &event else {
        panic!("expected Put, got {event:?}");
    };
    assert_eq!(*watch_id, id);
    assert_eq!(record.key(), &b"early/task"[..]);
}

/// A watch added while the stream retries connecting. A failed attempt used to take the request
/// with it, and the next attempt sent only the watches given to the builder.
#[rstest]
#[tokio::test]
async fn watch_add_while_establishing(mut etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    client.put("early/retry").value("1").await.unwrap();
    let rev = revision_of(&client, "early/retry").await;
    etcd_server.stop().unwrap();

    // Every attempt of a client that never connected fails with `Unavailable`, which the stream
    // retries. On the first client's dead connection, an attempt can fail with `Unknown` instead.
    let unconnected = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    let mut watcher = unconnected.watch().start();
    let pending = tokio::time::timeout(Duration::from_millis(100), watcher.next()).await;
    assert!(pending.is_err(), "expected nothing while etcd is down, got {pending:?}");

    let id = watcher.add(Watch::new("early/retry").start_revision(rev));
    // The attempt in flight when `add` ran fails too.
    let pending = tokio::time::timeout(Duration::from_millis(100), watcher.next()).await;
    assert!(pending.is_err(), "expected nothing while etcd is down, got {pending:?}");

    etcd_server.start().unwrap();
    // A restarted etcd takes about a second to elect itself, and longer on a loaded machine.
    let event = tokio::time::timeout(Duration::from_secs(30), watcher.next())
        .await
        .expect("timed out waiting for watch event")
        .expect("watch stream ended unexpectedly")
        .expect("watch stream yielded an error");
    let WatchEvent::Put { record, watch_id, .. } = &event else {
        panic!("expected Put, got {event:?}");
    };
    assert_eq!(*watch_id, id);
    assert_eq!(record.key(), &b"early/retry"[..]);
}
