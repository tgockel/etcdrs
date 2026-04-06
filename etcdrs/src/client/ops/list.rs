use futures_core::Stream;

use std::sync::{Arc, Mutex};

use crate::{
    client::{key_with_metadata_from_pb, record_from_pb, Client},
    pb::{etcdserverpb, mvccpb},
    record::{AsKey, KeyWithMetadata, Record},
    AsRange, Prefix, ResponseHeader,
};
use std::{
    future::{Future, IntoFuture},
    marker,
    pin::Pin,
    task::{Context, Poll},
};

impl Client {
    /// List the records associated with a `query`.
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let entries = client
    ///     .list("a".."b") // from "a" up to but not including "b"
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

pub struct List<C = (), R = Record> {
    client: C,
    pub(crate) request: etcdserverpb::RangeRequest,
    _return: marker::PhantomData<R>,
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

    fn _with_range(mut self, lower: Vec<u8>, upper: Vec<u8>) -> Self {
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

impl<R> List<Client, R> {
    fn stream_result_chunks(
        self,
        shared_header: Arc<Mutex<Option<ResponseHeader>>>,
    ) -> impl Stream<Item = Result<etcdserverpb::RangeResponse, ListError>> {
        async_stream::stream! {
            let mut request = self.request;
            // if the user never set the range, fill it with 0
            if request.key.is_empty() && request.range_end.is_empty() {
                request.key = vec![0];
                request.range_end = vec![0];
            }
            let client_inner = self.client.inner;

            loop {
                let resp = match client_inner.wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.range(r).await,
                    request.clone()
                ).await {
                    Ok(resp) => resp,
                    Err(status) => {
                        let is_unavailable = status.code() == tonic::Code::Unavailable;
                        yield Err(ListError::from_status(status));
                        if is_unavailable {
                            // if we were unavailable, just try again
                            continue;
                        } else {
                            // other cases end the stream
                            break;
                        }
                    }
                };

                if let Some(pb_header) = &resp.header {
                    *shared_header.lock().unwrap() = Some(ResponseHeader::from_pb(*pb_header));
                }

                let more = resp.more;
                if more {
                    // Pin the revision from the first response so subsequent pages are consistent.
                    if request.revision == 0 {
                        if let Some(header) = &resp.header {
                            request.revision = header.revision;
                        }
                    }

                    // if there are more results, advance request.key to one past the end for the next query
                    let Some(last_kv) = resp.kvs.last() else {
                        // this should be unreachable, but a bad server implementation could land us here
                        yield Err(ListError::new(
                            ListErrorKind::InvalidResponse,
                            "`range` call has no results, but `more = true`...is something wrong with the server?",
                            None,
                        ));
                        break;
                    };
                    request.key = crate::range::successor(&last_kv.key);
                }
                yield Ok(resp);
                if !more {
                    break;
                }
            }
        }
    }

    fn stream_results(
        self,
        shared_header: Arc<Mutex<Option<ResponseHeader>>>,
    ) -> impl Stream<Item = Result<mvccpb::KeyValue, ListError>> {
        let chunk_stream = self.stream_result_chunks(shared_header);
        async_stream::stream! {
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
        }
    }
}

impl List<Client, Record> {
    pub fn into_stream(self) -> ListIterator<Record> {
        let shared_header = Arc::new(Mutex::new(None));
        ListIterator {
            header: Arc::clone(&shared_header),
            iter: Box::new(self.stream_results(shared_header)),
            convert: record_from_pb,
        }
    }
}

impl List<Client, KeyWithMetadata> {
    pub fn into_stream(self) -> ListIterator<KeyWithMetadata> {
        let shared_header = Arc::new(Mutex::new(None));
        ListIterator {
            header: Arc::clone(&shared_header),
            iter: Box::new(self.stream_results(shared_header)),
            convert: key_with_metadata_from_pb,
        }
    }
}

pub struct ListIterator<R> {
    header: Arc<Mutex<Option<ResponseHeader>>>,
    iter: Box<dyn Stream<Item = Result<mvccpb::KeyValue, ListError>>>,
    convert: fn(mvccpb::KeyValue) -> R,
}

impl<R> ListIterator<R> {
    /// Returns the [`ResponseHeader`] from the most recently fetched page.
    ///
    /// Returns `None` if no page has been fetched yet (the stream has not been polled).
    pub fn header(&self) -> Option<ResponseHeader> {
        *self.header.lock().unwrap()
    }

    /// Underlying implementation of `poll_next` used by trait-specific implementations.
    fn poll_next_impl(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<R>, ListError>> {
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
    type Item = Result<R, ListError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<R> std::async_iter::AsyncIterator for ListIterator<R> {
    type Item = Result<R, ListError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<C, R> std::async_iter::IntoAsyncIterator for List<C, R>
where
    Self: fallible_async_iterator::IntoFallibleAsyncIterator<
        Item = R,
        Error = ListError,
        IntoFallibleAsyncIter = ListIterator<R>,
    >,
{
    type Item = Result<R, ListError>;
    type IntoAsyncIter = ListIterator<R>;

    fn into_async_iter(self) -> Self::IntoAsyncIter {
        use fallible_async_iterator::IntoFallibleAsyncIterator;
        self.into_fallible_async_iter()
    }
}

impl List<Client, usize> {
    async fn call(self) -> Result<CountResponse, ListError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.range(r).await,
                self.request,
            )
            .await
            .map_err(ListError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("RangeResponse should have a valid header"));
        Ok(CountResponse { header, count: resp.count as usize })
    }
}

/// The response from a [`list`][Client::list] operation with [`count_only`][List::count_only].
#[derive(Clone, Copy, Debug)]
pub struct CountResponse {
    header: ResponseHeader,
    count: usize,
}

impl CountResponse {
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

impl<T> Future for ListFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for List<Client, usize> {
    type Output = Result<CountResponse, ListError>;
    type IntoFuture = ListFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        ListFuture(Box::pin(self.call()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListErrorKind {
    /// The server returned an invalid or unexpected response.
    InvalidResponse,
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`list`][Client::list] operation.
    pub struct ListError(ListErrorKind);
}

impl ListError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        Self::new(ListErrorKind::Transport, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<ListFuture<Result<CountResponse, ListError>>>();
    }
};
