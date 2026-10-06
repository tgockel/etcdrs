use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use etcdrs::client::{WatchErrorKind, WatchEvent, WatchId, WatchStream};
use etcdrs::{GetError, ResponseHeader, Revision};
use futures::{FutureExt, StreamExt};

use super::{RangeState, Shared};

/// Delay between watcher generations, and before the first retry of an unseeded range.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Longest delay between retries of an unseeded range, which start at [`RETRY_DELAY`] and double.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Background task that seeds the cached ranges and keeps them coherent with the store via a
/// watch.
///
/// Runs in generations: seed whatever is unseeded, establish one watcher covering every seeded
/// range, then apply its events until the stream dies or its auth token goes stale (the etcdrs
/// watch stream does not reconnect on its own). Each new generation resumes every range's watch
/// from the revision its store is already exact at, so nothing is lost or reapplied across
/// reconnects.
pub(crate) async fn run(shared: Arc<Shared>) {
    if shared.ranges.is_empty() {
        return;
    }

    loop {
        seed_unseeded_ranges(&shared).await;

        // Watches given to the builder are numbered from 1, in order.
        let mut builder = shared.client.watch();
        let mut routing = HashMap::new();
        {
            let state = shared.state.lock().unwrap();
            for (index, (spec, range)) in shared.ranges.iter().zip(state.iter()).enumerate() {
                if let Some(header) = range.header {
                    builder = builder.add(spec.watch(next_revision(header.revision())));
                    let id = WatchId::new(routing.len() as i64 + 1).expect("routing.len() + 1 is nonzero");
                    routing.insert(id, index);
                }
            }
        }
        let (sender, stream) = builder.start().into_parts();
        shared.progress.lock().unwrap().sender = Some(sender);

        watch_generation(&shared, stream, routing).await;

        shared.progress.lock().unwrap().sender = None;
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// List every unseeded range once. A range that fails is listed again after a later one succeeds,
/// since etcd may have become reachable in between, so that it starts in the watcher.
async fn seed_unseeded_ranges(shared: &Shared) {
    let mut pending: VecDeque<usize> = (0..shared.ranges.len())
        .filter(|&index| shared.state.lock().unwrap()[index].header.is_none())
        .collect();
    let mut failed = Vec::new();
    while let Some(index) = pending.pop_front() {
        match seed_range(shared, index).await {
            Ok(_) => pending.extend(failed.drain(..)),
            Err(_) => failed.push(index),
        }
    }
}

/// Seed one range: list its full contents and swap them in.
///
/// The list stream pins its revision on the first page, so the scan is a consistent snapshot of
/// the range as of the returned header's revision even when it spans multiple pages.
async fn seed_range(shared: &Shared, index: usize) -> Result<ResponseHeader, GetError> {
    let view = shared.ranges[index]
        .seed_list()
        .with_client(shared.client.clone())
        .await?;
    let header = *view.header();

    let mut store = BTreeMap::new();
    let mut records = view.into_stream();
    while let Some(record) = records.next().await {
        let record = record?;
        store.insert(record.key().clone(), record);
    }

    let mut state = shared.state.lock().unwrap();
    state[index] = RangeState {
        store,
        header: Some(header),
    };
    drop(state);

    shared.observe_header(&header);
    Ok(header)
}

/// Consume one watcher generation's stream until it dies or its auth token goes stale.
///
/// `routing` maps live watch IDs to range indexes, starting with the watcher's own watches,
/// numbered from 1. It is the source of truth for which watches are healthy: a compacted or refused
/// watch is removed before its range is reseeded and re-added, so stream-wide progress
/// notifications cannot advance a range whose watch is dead.
async fn watch_generation(shared: &Shared, mut stream: WatchStream, mut routing: HashMap<WatchId, usize>) {
    let started = routing.len() as i64;
    // Retries of unseeded ranges as (due, range index), and each range's delay for its next retry.
    let mut retries = BTreeSet::new();
    let mut delays = vec![RETRY_DELAY; shared.ranges.len()];
    for index in 0..shared.ranges.len() {
        if shared.state.lock().unwrap()[index].header.is_none() {
            schedule_retry(&mut retries, &mut delays, index);
        }
    }

    loop {
        // Checked on every pass, not only on timeout: the timeout below never fires while the
        // stream always has an item ready.
        while let Some(&(due, index)) = retries.first()
            && due <= tokio::time::Instant::now()
        {
            retries.pop_first();
            match seed_range(shared, index).await {
                // A watch added to a running stream gets no stream-wide progress, so start over
                // with this range in the watcher.
                Ok(_) if started == 0 => return,
                Ok(header) => rewatch(shared, &mut routing, index, header),
                Err(_) => schedule_retry(&mut retries, &mut delays, index),
            }
        }

        let next = match retries.first() {
            Some(&(due, _)) => match tokio::time::timeout_at(due, stream.next()).await {
                Ok(next) => next,
                Err(_) => continue,
            },
            None => stream.next().await,
        };
        let Some(first) = next else { return };

        // Drain everything already available so all events of one watch response, which etcdrs
        // yields back to back and which covers whole revisions, are applied under a single lock. A
        // reader then never observes part of a revision.
        let mut batch = vec![first];
        while let Some(Some(item)) = stream.next().now_or_never() {
            batch.push(item);
        }

        let outcome = apply_batch(shared, &mut routing, started, batch);

        if outcome.stream_progressed {
            shared.progress.lock().unwrap().last_request = None;
        }

        if outcome.fatal {
            return;
        }

        for index in outcome.compacted {
            // The watch's resume point predates the server's compaction horizon: replaying the
            // missed events is impossible, so take a fresh snapshot and watch from there. The
            // range keeps serving its stale-but-honest snapshot until the swap, or drops it if
            // the listing fails.
            match seed_range(shared, index).await {
                Ok(header) => rewatch(shared, &mut routing, index, header),
                Err(_) => {
                    shared.state.lock().unwrap()[index] = RangeState::default();
                    schedule_retry(&mut retries, &mut delays, index);
                }
            }
        }

        for index in outcome.refused {
            schedule_retry(&mut retries, &mut delays, index);
        }
    }
}

/// Schedule a retry of unseeded range `index` after its delay, doubling the delay for the next one.
fn schedule_retry(retries: &mut BTreeSet<(tokio::time::Instant, usize)>, delays: &mut [Duration], index: usize) {
    retries.insert((tokio::time::Instant::now() + delays[index], index));
    delays[index] = (delays[index] * 2).min(MAX_RETRY_DELAY);
}

/// Watch range `index` from just past `header`'s revision on the generation's stream.
fn rewatch(shared: &Shared, routing: &mut HashMap<WatchId, usize>, index: usize, header: ResponseHeader) {
    let progress = shared.progress.lock().unwrap();
    let sender = progress
        .sender
        .as_ref()
        .expect("sender is installed for the duration of the generation");
    routing.insert(
        sender.add(shared.ranges[index].watch(next_revision(header.revision()))),
        index,
    );
}

/// The result of applying one batch of stream items.
#[derive(Default)]
struct BatchOutcome {
    /// Ranges whose watches were compacted and need a reseed + re-add.
    compacted: Vec<usize>,
    /// Ranges whose watches the server refused; they are unseeded and need a later reseed + re-add.
    refused: Vec<usize>,
    /// A stream-wide progress notification arrived; the read path's progress-request debounce
    /// should reset so the next gated read may request again immediately.
    stream_progressed: bool,
    /// The stream yielded a non-recoverable error, or refused an added watch for its token; the
    /// generation must be torn down.
    fatal: bool,
}

/// Apply a batch of stream items to the cache under a single state lock. The generation's watcher
/// started with watches 1..=`started`.
fn apply_batch(
    shared: &Shared,
    routing: &mut HashMap<WatchId, usize>,
    started: i64,
    batch: Vec<Result<WatchEvent, etcdrs::WatchError>>,
) -> BatchOutcome {
    let mut outcome = BatchOutcome::default();
    let mut max_header: Option<ResponseHeader> = None;

    let mut state = shared.state.lock().unwrap();
    for item in batch {
        let header = match item {
            Ok(WatchEvent::Put {
                header,
                watch_id,
                record,
                ..
            }) => {
                let header = with_revision(header, record.metadata().modified_revision);
                if let Some(&index) = routing.get(&watch_id) {
                    let range = &mut state[index];
                    range.store.insert(record.key().clone(), record);
                    range.header = Some(header);
                }
                header
            }
            Ok(WatchEvent::Delete {
                header, watch_id, key, ..
            }) => {
                let header = with_revision(header, key.metadata().modified_revision);
                if let Some(&index) = routing.get(&watch_id) {
                    let range = &mut state[index];
                    range.store.remove(key.key());
                    range.header = Some(header);
                }
                header
            }
            Ok(WatchEvent::Progress { header, watch_id, .. }) => {
                match watch_id {
                    // Per-watch progress notification for a live watch.
                    Some(id) if id.get() > 0 => {
                        if let Some(&index) = routing.get(&id) {
                            advance_header(&mut state[index], header);
                        }
                    }
                    // Stream-wide progress: every live watch has delivered everything up to this
                    // revision. etcd responds to manual progress requests with watch ID -1, which
                    // etcdrs surfaces as-is (only 0 maps to `None`), so treat both the same.
                    _ => {
                        // Only the watches the generation started with: etcd may have answered
                        // this request before a watch added since then existed.
                        for (&id, &index) in routing.iter() {
                            if id.get() <= started {
                                advance_header(&mut state[index], header);
                            }
                        }
                        outcome.stream_progressed = true;
                    }
                }
                header
            }
            Err(error) if error.kind() == WatchErrorKind::Compacted => {
                if let Some(id) = error.watch_id()
                    && let Some(index) = routing.remove(&id)
                {
                    outcome.compacted.push(index);
                }
                continue;
            }
            Err(error) if error.kind() == WatchErrorKind::Canceled => {
                // The cache never cancels its watches, so the server refused to create this one.
                if let Some(id) = error.watch_id()
                    && let Some(index) = routing.remove(&id)
                {
                    // Only a new stream carries a fresh token. The added watch's range was just
                    // listed with a token etcd accepts, so the next generation watches its snapshot.
                    if id.get() > started && error.cancel_reason().is_some_and(is_stale_token_refusal) {
                        outcome.fatal = true;
                        break;
                    }
                    state[index] = RangeState::default();
                    outcome.refused.push(index);
                }
                continue;
            }
            Err(_) => {
                // The etcdrs stream ends after yielding a status error, so nothing follows.
                outcome.fatal = true;
                break;
            }
        };
        if max_header.is_none_or(|current| current.revision() < header.revision()) {
            max_header = Some(header);
        }
    }
    drop(state);

    if let Some(header) = max_header {
        shared.observe_header(&header);
    }
    outcome
}

/// Whether etcd refused a watch for the stream's auth token, which etcdrs yields with one of these
/// reasons once a watch on the stream is live.
fn is_stale_token_refusal(reason: &str) -> bool {
    matches!(
        reason,
        "rpc error: code = Unauthenticated desc = etcdserver: invalid auth token"
            | "rpc error: code = InvalidArgument desc = etcdserver: revision of auth store is old"
            | "rpc error: code = InvalidArgument desc = etcdserver: user name is empty"
    )
}

/// `header` with `revision` in its place. etcd labels a watch response that catches up on past
/// revisions with the store's current revision, which can be later than that of the events in it.
fn with_revision(header: ResponseHeader, revision: Revision) -> ResponseHeader {
    ResponseHeader::new(header.cluster_id(), header.member_id(), revision, header.raft_term())
}

/// Advance a range's header, never regressing its revision.
fn advance_header(range: &mut RangeState, header: ResponseHeader) {
    if range
        .header
        .is_none_or(|current| current.revision() < header.revision())
    {
        range.header = Some(header);
    }
}

/// The revision immediately after `revision`.
fn next_revision(revision: Revision) -> Revision {
    revision
        .get()
        .checked_add(1)
        .and_then(Revision::new)
        .expect("store revision overflowed i64")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicI64;

    use bytes::Bytes;
    use etcdrs::client::{WatchEvent, WatchId};
    use etcdrs::{Client, KeyWithMetadata, Metadata, Prefix, Record, Revision, Version};
    use etcdrs_test::{EtcdServer, etcd_server};
    use rstest::rstest;

    use super::super::range_spec::RangeSpec;
    use super::super::{ProgressControl, RangeState, Shared};
    use super::{apply_batch, is_stale_token_refusal, with_revision};

    /// A stream-wide progress reply can arrive after a watch is added but have been computed before
    /// etcd created it, so it must not advance that watch's range. It used to, which labeled the
    /// range's fresh snapshot as current before its watch had replayed anything.
    #[rstest]
    #[tokio::test]
    async fn stream_wide_progress_skips_watches_added_during_the_generation(etcd_server: EtcdServer) {
        let client = Client::new(&etcd_server.connect_string()).unwrap();
        let seeded = *client.put("k").value("1").await.unwrap().header();
        let later = *client.put("k").value("2").await.unwrap().header();
        let shared = Shared {
            client,
            ranges: vec![RangeSpec::from_range(Prefix("a/")), RangeSpec::from_range(Prefix("b/"))],
            state: Mutex::new(vec![
                RangeState {
                    store: Default::default(),
                    header: Some(seeded),
                },
                RangeState {
                    store: Default::default(),
                    header: Some(seeded),
                },
            ]),
            last_known: AtomicI64::new(0),
            progress: Mutex::new(ProgressControl::default()),
        };
        // The generation started with range 0's watch alone; range 1's watch was added as watch 2.
        let mut routing = HashMap::from([(WatchId::new(1).unwrap(), 0), (WatchId::new(2).unwrap(), 1)]);

        let progress = WatchEvent::Progress {
            header: later,
            watch_id: WatchId::new(-1),
            revision: later.revision(),
        };
        apply_batch(&shared, &mut routing, 1, vec![Ok(progress)]);

        let state = shared.state.lock().unwrap();
        assert_eq!(state[0].header, Some(later));
        assert_eq!(state[1].header, Some(seeded));
    }

    /// etcd labels each batch of a watch's catch-up with the store's current revision, which can be
    /// later than the events in it. The cache used to take that label as its range's revision, so a
    /// range was current from the first batch on, and a stream lost before the last batch resumed
    /// after it.
    #[rstest]
    #[tokio::test]
    async fn events_label_their_range_with_their_own_revision(etcd_server: EtcdServer) {
        let client = Client::new(&etcd_server.connect_string()).unwrap();
        let header = *client.put("k").value("v").await.unwrap().header();
        let at = |revision| with_revision(header, Revision::new(revision).unwrap());
        let metadata = |revision| Metadata {
            create_revision: Revision::new(20).unwrap(),
            modified_revision: Revision::new(revision).unwrap(),
            version: Version::new(1),
            lease: None,
        };
        let shared = Shared {
            client,
            ranges: vec![RangeSpec::from_range(Prefix("a/"))],
            state: Mutex::new(vec![RangeState {
                store: Default::default(),
                header: Some(at(10)),
            }]),
            last_known: AtomicI64::new(10),
            progress: Mutex::new(ProgressControl::default()),
        };
        let watch_id = WatchId::new(1).unwrap();
        let mut routing = HashMap::from([(watch_id, 0)]);

        let put = WatchEvent::Put {
            header: at(30),
            watch_id,
            record: Record::new(Bytes::from_static(b"a/1"), Bytes::from_static(b"v"), metadata(20)),
            prev_record: None,
            created: true,
        };
        apply_batch(&shared, &mut routing, 1, vec![Ok(put)]);
        assert_eq!(shared.state.lock().unwrap()[0].header, Some(at(20)));
        assert_eq!(shared.last_known_revision(), Revision::new(20));

        let delete = WatchEvent::Delete {
            header: at(30),
            watch_id,
            key: KeyWithMetadata::new(Bytes::from_static(b"a/1"), metadata(25)),
            prev_record: None,
        };
        apply_batch(&shared, &mut routing, 1, vec![Ok(delete)]);
        assert_eq!(shared.state.lock().unwrap()[0].header, Some(at(25)));
        assert_eq!(shared.last_known_revision(), Revision::new(25));
    }

    #[test]
    fn stale_token_refusals_are_recognized() {
        for reason in [
            "rpc error: code = Unauthenticated desc = etcdserver: invalid auth token",
            "rpc error: code = InvalidArgument desc = etcdserver: revision of auth store is old",
            "rpc error: code = InvalidArgument desc = etcdserver: user name is empty",
        ] {
            assert!(is_stale_token_refusal(reason), "{reason}");
        }
        for reason in [
            "rpc error: code = PermissionDenied desc = etcdserver: permission denied",
            "mvcc: watcher range is empty",
        ] {
            assert!(!is_stale_token_refusal(reason), "{reason}");
        }
    }
}
