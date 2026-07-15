use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use etcdrs::client::{
    Compact, CompactFuture as RemoteCompactFuture, CountResponse, Delete, DeleteError,
    DeleteFuture as RemoteDeleteFuture, DeleteResponse, Get, GetFuture as RemoteGetFuture, GetResponse, List,
    ListFuture as RemoteListFuture, ListIterator, ListView as RemoteListView, Put, PutError,
    PutFuture as RemotePutFuture, PutResponse, Transaction, TransactionFuture as RemoteTransactionFuture,
};
use etcdrs::driver::KvDriver;
use etcdrs::{Client, GetError, KeyWithMetadata, Record, ResponseHeader, Revision};
use futures_core::Stream;

use super::range_spec::RangeSpec;
use super::{CacheClient, RangeState, Shared};

/// Minimum interval between watch progress requests fired by gated reads.
const PROGRESS_DEBOUNCE: Duration = Duration::from_millis(100);

impl Shared {
    /// Serve a read from range `index` if it can prove freshness, extracting the result with
    /// `read` under the state lock.
    ///
    /// The gate: a range serves iff it is seeded and its coherent revision has caught up to every
    /// revision observed through this client. A seeded-but-behind range additionally nudges the
    /// watch for a progress notification so it can prove freshness again soon; an unseeded range
    /// has no watch to nudge.
    fn try_serve<T>(&self, index: usize, read: impl FnOnce(&RangeState) -> T) -> Option<(ResponseHeader, T)> {
        let last_known = self.last_known.load(Ordering::Acquire);

        let state = self.state.lock().unwrap();
        let range = &state[index];
        match range.header {
            Some(header) if header.revision().get() >= last_known => Some((header, read(range))),
            behind => {
                let nudge = behind.is_some();
                drop(state);
                if nudge {
                    self.request_progress();
                }
                None
            }
        }
    }

    /// Serve a get from the cache, or `None` if it must pass through.
    ///
    /// A revision-pinned get always passes through: the cache holds only latest values, never
    /// history.
    fn try_serve_get(&self, get: &Get<()>) -> Option<GetResponse> {
        if get.revision().is_some() {
            return None;
        }
        let key = get.target_key();
        let index = self.ranges.iter().position(|range| range.contains_key(key))?;
        self.try_serve(index, |range| range.store.get(key).cloned())
            .map(|(header, record)| GetResponse::new(header, record))
    }

    /// The configured range that fully contains `query`, unless the query is revision-pinned.
    fn span_range_index(&self, query: &RangeSpec, pinned: Option<Revision>) -> Option<usize> {
        if pinned.is_some() {
            return None;
        }
        self.ranges.iter().position(|range| range.contains_span(query))
    }

    /// Serve a listing from the cache, or `None` if it must pass through. Records come back in
    /// key order, exactly as the server would return them.
    fn try_serve_records(&self, query: &RangeSpec, pinned: Option<Revision>) -> Option<(ResponseHeader, Vec<Record>)> {
        let index = self.span_range_index(query, pinned)?;
        self.try_serve(index, |range| {
            range
                .store
                .range::<[u8], _>(query.as_bounds())
                .map(|(_, record)| record.clone())
                .collect()
        })
    }

    /// Serve a count from the cache, or `None` if it must pass through.
    fn try_serve_count(&self, query: &RangeSpec, pinned: Option<Revision>) -> Option<CountResponse> {
        let index = self.span_range_index(query, pinned)?;
        self.try_serve(index, |range| range.store.range::<[u8], _>(query.as_bounds()).count())
            .map(|(header, count)| CountResponse::new(header, count))
    }

    /// Ask the watch for a progress notification so ranges left behind by an observed revision
    /// can prove freshness again, typically within one server round trip.
    ///
    /// Debounced, and a no-op while the watch is down — reads keep passing through in the
    /// meantime, which is always correct.
    fn request_progress(&self) {
        let mut progress = self.progress.lock().unwrap();
        let now = tokio::time::Instant::now();
        if progress
            .last_request
            .is_some_and(|last| now.duration_since(last) < PROGRESS_DEBOUNCE)
        {
            return;
        }
        if let Some(sender) = progress.sender.as_ref() {
            sender.request_progress();
            progress.last_request = Some(now);
        }
    }
}

/// The [`Future`] type returned by [`CacheClient`] operations.
///
/// Either resolves immediately with a cache-served response, or drives the wrapped client's
/// future `F` and maps its output into `T` (observing the response header for the freshness
/// gate along the way).
pub struct CacheFuture<F: Future, T = <F as Future>::Output> {
    inner: CacheFutureInner<F, T>,
}

enum CacheFutureInner<F: Future, T> {
    /// Resolved from the cache.
    Ready(Option<T>),
    /// Passing through to the wrapped client.
    Remote {
        future: F,
        shared: Arc<Shared>,
        map: fn(F::Output, &Shared) -> T,
    },
}

impl<F: Future, T> CacheFuture<F, T> {
    fn ready(output: T) -> Self {
        Self {
            inner: CacheFutureInner::Ready(Some(output)),
        }
    }

    fn remote(future: F, shared: Arc<Shared>, map: fn(F::Output, &Shared) -> T) -> Self {
        Self {
            inner: CacheFutureInner::Remote { future, shared, map },
        }
    }
}

impl<F, T> Future for CacheFuture<F, T>
where
    F: Future + Unpin,
    T: Unpin,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match &mut self.get_mut().inner {
            CacheFutureInner::Ready(output) => Poll::Ready(output.take().expect("CacheFuture polled after completion")),
            CacheFutureInner::Remote { future, shared, map } => {
                let output = ready!(Pin::new(future).poll(cx));
                Poll::Ready(map(output, shared))
            }
        }
    }
}

/// The list view returned by [`CacheClient`] list operations.
///
/// Mirrors [`etcdrs::client::ListView`]: either a snapshot cloned from a cached range, or the
/// wrapped client's view for queries that passed through.
pub struct CacheListView<R> {
    header: ResponseHeader,
    source: CacheListSource<R>,
}

enum CacheListSource<R> {
    /// A snapshot cloned from a cached range.
    Cached {
        records: Vec<Record>,
        convert: fn(Record) -> R,
    },
    /// The wrapped client's view, for queries that passed through.
    Remote(RemoteListView<R>),
}

impl<R> CacheListView<R> {
    /// The response header. For cache-served queries, its revision is the coherent revision of
    /// the range that served it.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The revision of the database this listing reads as of.
    ///
    /// This is the same as what you would find in the response [`header`][Self::header].
    pub fn revision(&self) -> Revision {
        self.header.revision()
    }

    /// Convert this view into a stream of records.
    pub fn into_stream(self) -> CacheListStream<R> {
        CacheListStream(match self.source {
            CacheListSource::Cached { records, convert } => CacheListStreamInner::Cached {
                records: records.into_iter(),
                convert,
            },
            CacheListSource::Remote(view) => CacheListStreamInner::Remote(view.into_stream()),
        })
    }
}

impl<R> etcdrs::driver::ListView<R> for CacheListView<R> {
    type Stream = CacheListStream<R>;

    fn header(&self) -> &ResponseHeader {
        &self.header
    }

    fn into_stream(self) -> CacheListStream<R> {
        self.into_stream()
    }
}

/// The stream of records produced by [`CacheListView::into_stream`].
pub struct CacheListStream<R>(CacheListStreamInner<R>);

enum CacheListStreamInner<R> {
    Cached {
        records: std::vec::IntoIter<Record>,
        convert: fn(Record) -> R,
    },
    Remote(ListIterator<R>),
}

impl<R> Stream for CacheListStream<R> {
    type Item = Result<R, GetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.get_mut().0 {
            CacheListStreamInner::Cached { records, convert } => {
                Poll::Ready(records.next().map(|record| Ok(convert(record))))
            }
            CacheListStreamInner::Remote(inner) => Pin::new(inner).poll_next(cx),
        }
    }
}

fn keep_record(record: Record) -> Record {
    record
}

fn drop_value(record: Record) -> KeyWithMetadata {
    record.without_value()
}

/// Serve a list from the cache when possible, otherwise delegate to the wrapped client via
/// `remote` and wrap its view.
#[allow(
    clippy::type_complexity,
    reason = "the type is the KvDriver ListViewFuture GAT spelled out"
)]
fn execute_list<R>(
    shared: Arc<Shared>,
    list: List<(), R>,
    convert: fn(Record) -> R,
    remote: fn(Client, List<(), R>) -> RemoteListFuture<Result<RemoteListView<R>, GetError>>,
) -> CacheFuture<RemoteListFuture<Result<RemoteListView<R>, GetError>>, Result<CacheListView<R>, GetError>> {
    let query = RangeSpec::from_target(list.target_range());
    if let Some((header, records)) = shared.try_serve_records(&query, list.revision()) {
        return CacheFuture::ready(Ok(CacheListView {
            header,
            source: CacheListSource::Cached { records, convert },
        }));
    }
    let future = remote(shared.client.clone(), list);
    CacheFuture::remote(future, shared, |output, shared| {
        output.map(|view| {
            shared.observe_header(view.header());
            CacheListView {
                header: *view.header(),
                source: CacheListSource::Remote(view),
            }
        })
    })
}

impl KvDriver for CacheClient {
    type GetFuture = CacheFuture<RemoteGetFuture>;
    type PutFuture<R> = CacheFuture<RemotePutFuture<Result<PutResponse<R>, PutError>>>;
    type DeleteFuture<R, P> = CacheFuture<RemoteDeleteFuture<Result<DeleteResponse<R, P>, DeleteError>>>;
    type ListView<R> = CacheListView<R>;
    type ListViewFuture<R> =
        CacheFuture<RemoteListFuture<Result<RemoteListView<R>, GetError>>, Result<CacheListView<R>, GetError>>;
    type CountFuture = CacheFuture<RemoteListFuture<Result<CountResponse, GetError>>>;
    type CommitFuture = CacheFuture<RemoteTransactionFuture>;
    type CompactFuture = CacheFuture<RemoteCompactFuture>;

    fn execute_get(self, get: Get<()>) -> Self::GetFuture {
        let shared = Arc::clone(&self.0.shared);
        if let Some(response) = shared.try_serve_get(&get) {
            return CacheFuture::ready(Ok(response));
        }
        let future = get.with_client(shared.client.clone()).into_future();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }

    fn execute_put<R>(self, put: Put<(), R>) -> Self::PutFuture<R> {
        let shared = Arc::clone(&self.0.shared);
        let future = put.with_client(shared.client.clone()).into_future();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }

    fn execute_delete<R, P>(self, delete: Delete<(), R, P>) -> Self::DeleteFuture<R, P> {
        let shared = Arc::clone(&self.0.shared);
        let future = delete.with_client(shared.client.clone()).into_future();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }

    fn execute_list_records(self, list: List<(), Record>) -> Self::ListViewFuture<Record> {
        execute_list(Arc::clone(&self.0.shared), list, keep_record, |client, list| {
            client.execute_list_records(list)
        })
    }

    fn execute_list_keys(self, list: List<(), KeyWithMetadata>) -> Self::ListViewFuture<KeyWithMetadata> {
        execute_list(Arc::clone(&self.0.shared), list, drop_value, |client, list| {
            client.execute_list_keys(list)
        })
    }

    fn execute_count(self, list: List<(), usize>) -> Self::CountFuture {
        let shared = Arc::clone(&self.0.shared);
        let query = RangeSpec::from_target(list.target_range());
        if let Some(response) = shared.try_serve_count(&query, list.revision()) {
            return CacheFuture::ready(Ok(response));
        }
        let future = list.with_client(shared.client.clone()).into_future();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }

    fn execute_transaction(self, txn: Transaction<()>) -> Self::CommitFuture {
        let shared = Arc::clone(&self.0.shared);
        let future = txn.with_client(shared.client.clone()).commit();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }

    fn execute_compact(self, compact: Compact<()>) -> Self::CompactFuture {
        let shared = Arc::clone(&self.0.shared);
        let future = compact.with_client(shared.client.clone()).into_future();
        CacheFuture::remote(future, shared, |output, shared| {
            if let Ok(response) = &output {
                shared.observe_header(response.header());
            }
            output
        })
    }
}
