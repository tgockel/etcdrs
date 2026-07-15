use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use etcdrs::client::{WatchErrorKind, WatchEvent, WatchId, WatchStream};
use etcdrs::{GetError, ResponseHeader, Revision};
use futures::{FutureExt, StreamExt};

use super::{RangeState, Shared};

/// Delay between retries of a failed seed and between watcher generations.
const RETRY_DELAY: Duration = Duration::from_millis(100);

/// Background task that seeds the cached ranges and keeps them coherent with the store via a
/// watch.
///
/// Runs in generations: seed whatever is unseeded, establish one watcher covering every range,
/// then apply its events until the stream dies (the etcdrs watch stream does not reconnect on its
/// own). Each new generation resumes every range's watch from the revision its store is already
/// exact at, so nothing is lost or reapplied across reconnects.
pub(crate) async fn run(shared: Arc<Shared>) {
    if shared.ranges.is_empty() {
        return;
    }

    loop {
        seed_unseeded_ranges(&shared).await;

        // Every range must be passed as an initial spec: requests sent through `Watcher::add`
        // before the stream's first poll are silently dropped, and only initial specs are re-sent
        // when establishment retries. Initial specs are assigned watch IDs 1..=N in order.
        let mut builder = shared.client.watch();
        {
            let state = shared.state.lock().unwrap();
            for (spec, range) in shared.ranges.iter().zip(state.iter()) {
                let coherent = range.header.expect("every range was seeded above").revision();
                builder = builder.add(spec.watch(next_revision(coherent)));
            }
        }
        let (sender, stream) = builder.start().into_parts();
        let routing = (0..shared.ranges.len())
            .map(|index| (WatchId::new(index as i64 + 1).expect("index + 1 is nonzero"), index))
            .collect();
        shared.progress.lock().unwrap().sender = Some(sender);

        watch_generation(&shared, stream, routing).await;

        shared.progress.lock().unwrap().sender = None;
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Seed every range that does not yet have a coherent snapshot, retrying each until it succeeds.
async fn seed_unseeded_ranges(shared: &Shared) {
    for index in 0..shared.ranges.len() {
        if shared.state.lock().unwrap()[index].header.is_some() {
            continue;
        }
        seed_range_until_success(shared, index).await;
    }
}

/// Seed one range, retrying until it succeeds. Returns the snapshot's header.
async fn seed_range_until_success(shared: &Shared, index: usize) -> ResponseHeader {
    loop {
        match seed_range(shared, index).await {
            Ok(header) => return header,
            Err(_) => tokio::time::sleep(RETRY_DELAY).await,
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

/// Consume one watcher generation's stream until it dies.
///
/// `routing` maps live watch IDs to range indexes. It is the source of truth for which watches
/// are healthy: a compacted watch is removed before its range is reseeded and re-added, so
/// stream-wide progress notifications cannot advance a range whose watch is dead.
async fn watch_generation(shared: &Shared, mut stream: WatchStream, mut routing: HashMap<WatchId, usize>) {
    while let Some(first) = stream.next().await {
        // Drain everything already available so all events of one watch response -- typically one
        // revision, e.g. a multi-key transaction -- are applied under a single lock. A reader can
        // then never observe a prefix of a revision's events under that revision's header.
        let mut batch = vec![first];
        while let Some(Some(item)) = stream.next().now_or_never() {
            batch.push(item);
        }

        let outcome = apply_batch(shared, &mut routing, batch);

        if outcome.stream_progressed {
            shared.progress.lock().unwrap().last_request = None;
        }

        for index in outcome.compacted {
            // The watch's resume point predates the server's compaction horizon: replaying the
            // missed events is impossible, so take a fresh snapshot and watch from there. The
            // range keeps serving its stale-but-honest snapshot until the swap.
            let header = seed_range_until_success(shared, index).await;
            let new_id = {
                let progress = shared.progress.lock().unwrap();
                progress
                    .sender
                    .as_ref()
                    .expect("sender is installed for the duration of the generation")
                    .add(shared.ranges[index].watch(next_revision(header.revision())))
            };
            routing.insert(new_id, index);
        }

        if outcome.fatal {
            return;
        }
    }
}

/// The result of applying one batch of stream items.
#[derive(Default)]
struct BatchOutcome {
    /// Ranges whose watches were compacted and need a reseed + re-add.
    compacted: Vec<usize>,
    /// A stream-wide progress notification arrived; the read path's progress-request debounce
    /// should reset so the next gated read may request again immediately.
    stream_progressed: bool,
    /// The stream yielded a non-recoverable error; the generation must be torn down.
    fatal: bool,
}

/// Apply a batch of stream items to the cache under a single state lock.
fn apply_batch(
    shared: &Shared,
    routing: &mut HashMap<WatchId, usize>,
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
                        for &index in routing.values() {
                            advance_header(&mut state[index], header);
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
