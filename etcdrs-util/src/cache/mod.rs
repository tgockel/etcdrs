#![doc = include_str!("README.md")]

mod coherence;
mod driver;
mod range_spec;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use etcdrs::client::{Compact, Delete, Get, List, Put, Transaction, WatchSender};
use etcdrs::record::AsKey;
use etcdrs::{AsRange, Client, Prefix, Record, ResponseHeader, Revision};

pub use driver::{CacheFuture, CacheListStream, CacheListView};
use range_spec::RangeSpec;

/// A caching wrapper around [`Client`] that serves reads of configured key ranges from a local,
/// watch-maintained, in-memory store.
///
/// Configure the ranges to cache with [`builder`][Self::builder], then use the cache like a
/// client. Reads of keys outside the configured ranges — and reads the cache cannot prove are
/// fresh — transparently pass through to the server, so a read through the cache is never slower
/// than one through the wrapped [`Client`].
///
/// Handles are cheap to clone and share one cache. Dropping the last handle stops the background
/// coherence task, which cancels its watches.
///
/// See the [module-level documentation](self) for the coherence model.
#[derive(Clone)]
pub struct CacheClient(Arc<CacheClientInner>);

struct CacheClientInner {
    shared: Arc<Shared>,
    task: tokio::task::AbortHandle,
}

impl Drop for CacheClientInner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// State shared between [`CacheClient`] handles and the coherence task.
struct Shared {
    client: Client,
    /// The configured cache ranges. Immutable after construction; a range's index in this list
    /// identifies it everywhere else (the state vector and watch routing).
    ranges: Vec<RangeSpec>,
    /// Per-range cache contents, index-aligned with `ranges`. Never held across an `.await`.
    state: Mutex<Vec<RangeState>>,
    /// The highest store revision observed through this client (response headers, seed snapshots,
    /// and watch events). Zero means nothing has been observed yet.
    last_known: AtomicI64,
    /// Control channel to the active watch. Never locked together with `state`.
    progress: Mutex<ProgressControl>,
}

impl Shared {
    /// Record an observed response header, advancing the last-known revision.
    fn observe_header(&self, header: &ResponseHeader) {
        self.last_known.fetch_max(header.revision().get(), Ordering::AcqRel);
    }

    /// The highest observed store revision, if any.
    fn last_known_revision(&self) -> Option<Revision> {
        Revision::new(self.last_known.load(Ordering::Acquire))
    }
}

/// Handle to the active watch, shared between the coherence task and the read path.
#[derive(Default)]
struct ProgressControl {
    /// The current watcher generation's sender; `None` while the watch is being (re)established.
    sender: Option<WatchSender>,
    /// When the last progress request was fired by a gated read, for debouncing. Cleared when a
    /// stream-wide progress notification arrives so the next gated read may re-request
    /// immediately.
    last_request: Option<tokio::time::Instant>,
}

/// The cached contents of a single configured range.
#[derive(Default)]
struct RangeState {
    /// The latest known records within the range.
    store: BTreeMap<Bytes, Record>,
    /// The header of the most recent snapshot or event applied to `store`: the store is exact as
    /// of `header.revision()`. `None` until the initial seed completes.
    header: Option<ResponseHeader>,
}

impl CacheClient {
    /// Start building a cache backed by `client`.
    ///
    /// ```no_run
    /// # let client: etcdrs::Client = todo!();
    /// let cache = etcdrs_util::cache::CacheClient::builder(client)
    ///     .cache(etcdrs::Prefix("config/"))
    ///     .cache("leader")
    ///     .build();
    /// ```
    pub fn builder(client: Client) -> CacheClientBuilder {
        CacheClientBuilder {
            client,
            ranges: Vec::new(),
        }
    }

    /// The wrapped [`Client`], for operations that should never involve the cache.
    pub fn client(&self) -> &Client {
        &self.0.shared.client
    }

    /// The highest store revision observed through this cache, or `None` if nothing has been
    /// observed yet.
    ///
    /// Every response header seen by this cache — from operations executed through it, from seed
    /// snapshots, and from watch events — advances this value. It never decreases.
    pub fn last_known_revision(&self) -> Option<Revision> {
        self.0.shared.last_known_revision()
    }

    /// The revision the cached contents of `range` are exact as of.
    ///
    /// Returns `None` if `range` is not one of the configured cache ranges (compared by exact
    /// boundaries, so e.g. `Prefix("a/")` only matches a range cached as that same prefix) or if
    /// its initial seed has not completed yet.
    pub fn coherent_revision(&self, range: impl AsRange) -> Option<Revision> {
        let spec = RangeSpec::from_range(range);
        let index = self.0.shared.ranges.iter().position(|configured| *configured == spec)?;
        let state = self.0.shared.state.lock().unwrap();
        state[index].header.map(|header| header.revision())
    }

    /// Get the record associated with a key.
    ///
    /// Served from the local store when the key lies in a configured range that can prove
    /// freshness; otherwise the read passes through to the server. Revision-pinned gets
    /// ([`at_revision`][Get::at_revision]) always pass through, since the cache holds only the
    /// latest values.
    ///
    /// ```no_run
    /// # async {
    /// # let cache: etcdrs_util::cache::CacheClient = todo!();
    /// let response = cache.get("my-key").await.unwrap();
    /// if let Some(record) = response.record() {
    ///     println!("value: {:?}", record.value());
    /// }
    /// # };
    /// ```
    pub fn get(&self, key: impl AsKey) -> Get<Self> {
        Get::new(key).with_client(self.clone())
    }

    /// Put a value for a key.
    ///
    /// Always executed on the server. The cache never applies writes locally — the change becomes
    /// visible through the watch — but the response's revision immediately gates cached reads, so
    /// a get issued after this put resolves will never observe the pre-put value.
    pub fn put(&self, key: impl AsKey) -> Put<Self, ()> {
        Put::new(key).with_client(self.clone())
    }

    /// Delete a single key. Always executed on the server; see [`put`][Self::put] for how writes
    /// interact with the cache.
    pub fn delete(&self, key: impl AsKey) -> Delete<Self, bool> {
        Delete::new(key).with_client(self.clone())
    }

    /// Delete a range of keys. Always executed on the server; see [`put`][Self::put] for how
    /// writes interact with the cache.
    pub fn delete_range(&self, range: impl AsRange) -> Delete<Self, usize> {
        Delete::with_range(range).with_client(self.clone())
    }

    /// Delete all keys with the given prefix. Always executed on the server; see
    /// [`put`][Self::put] for how writes interact with the cache.
    pub fn delete_prefix(&self, prefix: impl AsKey) -> Delete<Self, usize> {
        Delete::with_prefix(prefix).with_client(self.clone())
    }

    /// List the records within a range.
    ///
    /// Served from the local store when the query is fully contained in a single configured range
    /// that can prove freshness; otherwise it passes through to the server. Revision-pinned lists
    /// always pass through.
    pub fn list(&self, query: impl AsRange) -> List<Self, Record> {
        List::new(query).with_client(self.clone())
    }

    /// List the records associated with a prefix.
    ///
    /// This is the same as calling [`list`][Self::list] with a [`Prefix`] query.
    pub fn list_prefix(&self, prefix: impl AsKey) -> List<Self, Record> {
        self.list(Prefix(prefix))
    }

    /// Begin a transaction. Always executed on the server, including any read operations it
    /// contains; see [`put`][Self::put] for how writes interact with the cache.
    pub fn transaction(&self) -> Transaction<Self> {
        Transaction::default().with_client(self.clone())
    }

    /// Compact the store's history up to `revision`. Always executed on the server. If compaction
    /// outruns a cached range's watch, the coherence task recovers by taking a fresh snapshot.
    pub fn compact(&self, revision: Revision) -> Compact<Self> {
        Compact::new(revision).with_client(self.clone())
    }
}

/// Builder for a [`CacheClient`]. Created by [`CacheClient::builder`].
#[must_use = "CacheClientBuilder does nothing unless you call `build`"]
pub struct CacheClientBuilder {
    client: Client,
    ranges: Vec<RangeSpec>,
}

impl CacheClientBuilder {
    /// Add a key, range, or prefix whose contents should live in the cache.
    ///
    /// Overlapping ranges are allowed but each maintains an independent copy of the overlap.
    pub fn cache(mut self, range: impl AsRange) -> Self {
        self.ranges.push(RangeSpec::from_range(range));
        self
    }

    /// Add a prefix whose contents should live in the cache.
    ///
    /// Equivalent to calling [`cache`][Self::cache] with a [`Prefix`] query.
    pub fn cache_prefix(self, prefix: impl AsKey) -> Self {
        self.cache(Prefix(prefix))
    }

    /// Build the cache client using the provided Tokio runtime handle.
    ///
    /// This is equivalent to calling [`build`][Self::build] from within a Tokio runtime, but allows
    /// you to specify the runtime explicitly.
    pub fn build_on(self, runtime: &tokio::runtime::Handle) -> CacheClient {
        let range_count = self.ranges.len();
        let shared = Arc::new(Shared {
            client: self.client,
            ranges: self.ranges,
            state: Mutex::new(std::iter::repeat_with(RangeState::default).take(range_count).collect()),
            last_known: AtomicI64::new(0),
            progress: Mutex::new(ProgressControl::default()),
        });

        let task_shared = Arc::clone(&shared);
        let task = runtime.spawn(coherence::run(task_shared));

        CacheClient(Arc::new(CacheClientInner {
            shared,
            task: task.abort_handle(),
        }))
    }

    /// Spawn the coherence task and return the cache handle.
    ///
    /// Construction never fails and does not wait for the cache to warm: the returned handle is
    /// usable immediately, and reads of a configured range pass through to the server until that
    /// range's initial seed completes in the background. With no configured ranges the cache is a
    /// plain passthrough.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a Tokio runtime, which is required to spawn the background
    /// coherence task. Use [`build_on`][Self::build_on] to provide a runtime handle explicitly.
    pub fn build(self) -> CacheClient {
        self.build_on(&tokio::runtime::Handle::current())
    }
}
