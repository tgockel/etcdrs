use bytes::Bytes;
use futures_core::Stream;

use crate::{
    AsRange, GetError, GetErrorKind, Prefix, ResponseHeader, Revision, TargetRange,
    client::{Client, Idempotency},
    pb::{etcdserverpb, mvccpb},
    record::{AsKey, KeyWithMetadata, Record},
};

use std::{
    future::{Future, IntoFuture},
    marker,
    pin::Pin,
    task::{Context, Poll},
};

/// # Listing Ranges
impl Client {
    /// List the records associated with a `query`.
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let view = client
    ///     .list("a".."b") // from "a" up to but not including "b"
    ///     .await
    ///     .unwrap(); // <- do real error handling here
    /// let entries = view
    ///     .into_stream()
    ///     .map(Result::unwrap) // <- do real error handling here
    ///     .collect::<Vec<etcdrs::Record>>()
    ///     .await;
    /// # };
    /// ```
    ///
    /// List ranges use the Rust range syntax:
    ///
    /// ```no_run
    /// # let client: etcdrs::Client = todo!();
    /// let _ = client.list(..);        // <- all records in the database
    /// let _ = client.list("a"..="b"); // <- from "a" up to and including "b"
    /// let _ = client.list(.."taco");  // <- everything up to "taco"
    /// let _ = client.list("taco"..);  // <- "taco" and everything after
    /// let _ = client.list("apple");   // <- just "apple"
    /// ```
    ///
    /// You can specify a [prefix][crate::Prefix] query as well, but it is usually easier to use the
    /// [`list_prefix`][Self::list_prefix] method instead.
    ///
    /// ## Consistency
    /// Calling `.await` on a list operation locks your view of the database to the revision in the
    /// [`ListView`] returned. When the response spans multiple pages, the revision you view at is
    /// preserved so scans are temporarily consistent.
    pub fn list(&self, query: impl AsRange) -> List<Self, Record> {
        List::new(query).with_client(self.clone())
    }

    /// List the records associated with a prefix.
    ///
    /// This is the same as calling [`list`][Self::list] with a [`Prefix`] query.
    pub fn list_prefix(&self, prefix: impl AsKey) -> List<Self, Record> {
        self.list(Prefix(prefix))
    }
}

/// A [`Client::list`] or [`Client::list_prefix`] operation.
pub struct List<C = (), R = Record> {
    client: C,
    pub(crate) request: etcdserverpb::RangeRequest,
    _return: marker::PhantomData<fn() -> R>,
}

impl List<(), Record> {
    /// Create a new list operation with the specified `query`.
    ///
    /// This is equivalent to calling [`default`][Self::default] and then [`range`][Self::range].
    pub fn new(query: impl AsRange) -> Self {
        Self::default().range(query)
    }
}

impl Default for List<(), Record> {
    fn default() -> Self {
        Self {
            client: (),
            request: etcdserverpb::RangeRequest::default(),
            _return: marker::PhantomData,
        }
    }
}

impl<C, R> List<C, R> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by [`list`][`Client::list`].
    pub fn with_client<C2>(self, client: C2) -> List<C2, R> {
        List {
            client,
            request: self.request,
            _return: marker::PhantomData,
        }
    }

    /// The range this operation addresses.
    pub fn target_range(&self) -> TargetRange<'_> {
        TargetRange::from_wire(&self.request.key, &self.request.range_end)
    }

    /// The revision this operation reads from, or `None` for the server's current revision.
    pub fn revision(&self) -> Option<Revision> {
        Revision::new(self.request.revision)
    }

    /// The per-request page limit, or `None` when the server default is used.
    pub fn page_limit(&self) -> Option<usize> {
        usize::try_from(self.request.limit).ok().filter(|&limit| limit > 0)
    }

    /// Whether this operation returns keys and metadata without values.
    pub fn is_keys_only(&self) -> bool {
        self.request.keys_only
    }

    /// Whether this operation returns only a count.
    pub fn is_count_only(&self) -> bool {
        self.request.count_only
    }

    /// Decompose this operation into its client and a detached `List<(), R>`.
    pub(crate) fn into_parts(self) -> (C, List<(), R>) {
        (
            self.client,
            List {
                client: (),
                request: self.request,
                _return: marker::PhantomData,
            },
        )
    }

    fn _with_range(mut self, lower: Bytes, upper: Bytes) -> Self {
        self.request.key = lower;
        self.request.range_end = upper;
        self
    }

    /// ```
    /// # use etcdrs::client::List;
    /// List::default().prefix("foo/");
    /// List::default().prefix("foo/bar/");
    /// ```
    pub fn prefix(self, prefix: impl AsKey) -> Self {
        self.range(Prefix(prefix))
    }

    /// Filter the query by the specified `range`. The range specifiers work on any range which can be converted into a
    /// key via [`AsKey`].
    ///
    /// ```
    /// # use etcdrs::client::List;
    /// List::default().range("a".."b");  // from "a" up to but not including "b"
    /// List::default().range("b"..="c"); // from "b" up to and including "c"
    /// List::default().range(.."taco");  // everything up to "taco"
    /// List::default().range("taco"..);  // "taco" and everything after
    /// List::default().range(..);        // everything (which is the default)
    /// ```
    pub fn range(self, range: impl AsRange) -> Self {
        let (lower, upper) = range.as_boundaries();
        self._with_range(lower, upper)
    }

    /// Do not fetch the associated values.
    pub fn keys_only(mut self) -> List<C, KeyWithMetadata> {
        self.request.keys_only = true;
        self.request.count_only = false;
        List {
            client: self.client,
            request: self.request,
            _return: marker::PhantomData,
        }
    }

    /// Only fetch the count that the query would return.
    pub fn count_only(mut self) -> List<C, usize> {
        self.request.keys_only = false;
        self.request.count_only = true;
        List {
            client: self.client,
            request: self.request,
            _return: marker::PhantomData,
        }
    }

    /// Set a per-request limit of returned values.
    ///
    /// This is not a limit on the overall number of responses, but a limit on how many records will be fetched per
    /// request.
    pub fn limit(mut self, limit: usize) -> Self {
        self.request.limit = limit as i64;
        self
    }
}

fn stream_remaining_chunks(
    continuation: ListContinuation,
) -> impl Stream<Item = Result<etcdserverpb::RangeResponse, GetError>> {
    async_stream::stream! {
        let ListContinuation { client, mut request } = continuation;
        loop {
            let resp = match client.inner.wrap_unary_call(
                Idempotency::Immutable,
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.range(r).await,
                request.clone(),
            ).await {
                Ok(resp) => resp,
                Err(status) => {
                    let is_unavailable = status.code() == tonic::Code::Unavailable;
                    yield Err(GetError::from_status(status));
                    if is_unavailable {
                        continue;
                    } else {
                        break;
                    }
                }
            };

            let more = resp.more;
            if more {
                let Some(last_kv) = resp.kvs.last() else {
                    yield Err(GetError::new(
                        GetErrorKind::Unknown,
                        "`range` call has no results, but `more = true`...is something wrong with the server?",
                        None,
                    ));
                    break;
                };
                request.key = crate::range::successor(&last_kv.key).into();
            }
            yield Ok(resp);
            if !more {
                break;
            }
        }
    }
}

/// The response from awaiting a [`list`][Client::list] operation.
///
/// A `ListView` holds the response header from the first fetched batch and can be converted into a [`ListIterator`]
/// stream to consume all matching records (including those from subsequent pages).
pub struct ListView<R> {
    header: ResponseHeader,
    first_batch_kvs: Vec<mvccpb::KeyValue>,
    continuation: Option<ListContinuation>,
    convert: fn(mvccpb::KeyValue) -> R,
}

impl<R> ListView<R> {
    pub(crate) fn new(
        header: ResponseHeader,
        first_batch_kvs: Vec<mvccpb::KeyValue>,
        continuation: Option<ListContinuation>,
        convert: fn(mvccpb::KeyValue) -> R,
    ) -> Self {
        Self {
            header,
            first_batch_kvs,
            continuation,
            convert,
        }
    }

    /// The response header from the first fetched batch.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The revision of the database at the time the response was generated.
    ///
    /// This is the same as what you would find in the response [`header`][Self::header].
    pub fn revision(&self) -> Revision {
        self.header.revision()
    }

    /// Convert this view into a stream of records.
    ///
    /// The stream first yields items from the already-fetched first batch, then fetches and yields items from
    /// subsequent pages.
    pub fn into_stream(self) -> ListIterator<R> {
        let first_kvs = self.first_batch_kvs;

        let iter: Box<dyn Stream<Item = Result<mvccpb::KeyValue, GetError>> + Send> = match self.continuation {
            None => Box::new(async_stream::stream! {
                for kv in first_kvs {
                    yield Ok(kv);
                }
            }),
            Some(continuation) => {
                let chunk_stream = stream_remaining_chunks(continuation);
                Box::new(async_stream::stream! {
                    for kv in first_kvs {
                        yield Ok(kv);
                    }
                    for await chunk in chunk_stream {
                        match chunk {
                            Err(err) => yield Err(err),
                            Ok(chunk) => {
                                for kv in chunk.kvs {
                                    yield Ok(kv);
                                }
                            }
                        }
                    }
                })
            }
        };

        ListIterator {
            iter,
            convert: self.convert,
        }
    }
}

impl<R> crate::driver::ListView<R> for ListView<R> {
    type Stream = ListIterator<R>;

    fn header(&self) -> &ResponseHeader {
        &self.header
    }

    fn into_stream(self) -> ListIterator<R> {
        self.into_stream()
    }
}

pub struct ListIterator<R> {
    iter: Box<dyn Stream<Item = Result<mvccpb::KeyValue, GetError>> + Send>,
    convert: fn(mvccpb::KeyValue) -> R,
}

impl<R> ListIterator<R> {
    fn poll_next_impl(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<R>, GetError>> {
        let convert = self.as_ref().convert;
        let iter = unsafe { self.map_unchecked_mut(|s| s.iter.as_mut()) };
        iter.poll_next(cx).map(|item| match item {
            None => Ok(None),
            Some(Ok(raw)) => Ok(Some(convert(raw))),
            Some(Err(err)) => Err(err),
        })
    }
}

impl<R> futures_core::Stream for ListIterator<R> {
    type Item = Result<R, GetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<R> std::async_iter::AsyncIterator for ListIterator<R> {
    type Item = Result<R, GetError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<R> std::async_iter::IntoAsyncIterator for ListView<R>
where
    R: 'static,
{
    type Item = Result<R, GetError>;
    type IntoAsyncIter = ListIterator<R>;

    fn into_async_iter(self) -> Self::IntoAsyncIter {
        self.into_stream()
    }
}

/// The response from a [`list`][Client::list] operation with [`count_only`][List::count_only].
#[derive(Clone, Copy, Debug)]
pub struct CountResponse {
    header: ResponseHeader,
    count: usize,
}

impl CountResponse {
    /// Construct a new `CountResponse`.
    pub fn new(header: ResponseHeader, count: usize) -> Self {
        Self { header, count }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The number of keys matching the query.
    pub fn count(&self) -> usize {
        self.count
    }
}

/// The [`Future`] type returned by awaiting [`list`][Client::list] operations.
pub struct ListFuture<T>(Pin<Box<dyn Future<Output = T> + Send>>);

impl<T> ListFuture<T> {
    pub(crate) fn new(future: impl Future<Output = T> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl<T> Future for ListFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::KvDriver> IntoFuture for List<C, usize> {
    type Output = Result<CountResponse, GetError>;
    type IntoFuture = C::CountFuture;

    fn into_future(self) -> C::CountFuture {
        let (client, detached) = self.into_parts();
        client.execute_count(detached)
    }
}

impl<C: crate::driver::KvDriver> IntoFuture for List<C, Record> {
    type Output = Result<C::ListView<Record>, GetError>;
    type IntoFuture = C::ListViewFuture<Record>;

    fn into_future(self) -> C::ListViewFuture<Record> {
        let (client, detached) = self.into_parts();
        client.execute_list_records(detached)
    }
}

impl<C: crate::driver::KvDriver> IntoFuture for List<C, KeyWithMetadata> {
    type Output = Result<C::ListView<KeyWithMetadata>, GetError>;
    type IntoFuture = C::ListViewFuture<KeyWithMetadata>;

    fn into_future(self) -> C::ListViewFuture<KeyWithMetadata> {
        let (client, detached) = self.into_parts();
        client.execute_list_keys(detached)
    }
}

pub(crate) struct ListContinuation {
    pub(crate) client: Client,
    pub(crate) request: etcdserverpb::RangeRequest,
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<ListFuture<Result<CountResponse, GetError>>>();
        _assert_send::<ListFuture<Result<ListView<Record>, GetError>>>();
        _assert_send::<ListFuture<Result<ListView<KeyWithMetadata>, GetError>>>();
    }
};
