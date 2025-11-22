use futures_core::Stream;

use crate::{
    client::{key_with_metadata_from_pb, record_from_pb, BoxedFuture, Client},
    error::{ErrorInner, ErrorKind},
    pb::{etcdserverpb, mvccpb},
    record::{AsKey, KeyWithMetadata, Record},
    AsRange, Prefix, Result,
};
use std::{
    future::IntoFuture,
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
    fn stream_result_chunks(self) -> impl Stream<Item = Result<etcdserverpb::RangeResponse>> {
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
                    etcdserverpb::kv_client::KvClient::range,
                    request.clone()
                ).await {
                    Ok(resp) => resp,
                    Err(err) => {
                        let err_kind = err.kind();
                        yield Err(err);
                        if err_kind == ErrorKind::Unavailable {
                            // if we were unavailable, just try again
                            continue;
                        } else {
                            // other cases end the stream
                            break;
                        }
                    }
                };

                let more = resp.more;
                if more {
                    // if there are more results, advance request.key to one past the end for the next query
                    let Some(last_kv) = resp.kvs.last() else {
                        // this should be unreachable, but a bad server implementation could land us here
                        yield Err(ErrorInner::with_static_message(
                            ErrorKind::Unknown,
                            "`range` call has no results, but `more = true`...is something wrong with the server?"
                        ).into());
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

    fn stream_results(self) -> impl Stream<Item = Result<mvccpb::KeyValue>> {
        let chunk_stream = self.stream_result_chunks();
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
        ListIterator {
            iter: Box::new(self.stream_results()),
            convert: record_from_pb,
        }
    }
}

impl List<Client, KeyWithMetadata> {
    pub fn into_stream(self) -> ListIterator<KeyWithMetadata> {
        ListIterator {
            iter: Box::new(self.stream_results()),
            convert: key_with_metadata_from_pb,
        }
    }
}

pub struct ListIterator<R> {
    iter: Box<dyn Stream<Item = Result<mvccpb::KeyValue>>>,
    convert: fn(mvccpb::KeyValue) -> R,
}

impl<R> ListIterator<R> {
    /// Underlying implementation of `poll_next` used by trait-specific implementations.
    fn poll_next_impl(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<R>, crate::Error>> {
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
    type Item = Result<R, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<R> std::async_iter::AsyncIterator for ListIterator<R> {
    type Item = Result<R, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl<C, R> std::async_iter::IntoAsyncIterator for List<C, R>
where
    Self: fallible_async_iterator::IntoFallibleAsyncIterator<
        Item = R,
        Error = crate::Error,
        IntoFallibleAsyncIter = ListIterator<R>,
    >,
{
    type Item = Result<R, crate::Error>;
    type IntoAsyncIter = ListIterator<R>;

    fn into_async_iter(self) -> Self::IntoAsyncIter {
        use fallible_async_iterator::IntoFallibleAsyncIterator;
        self.into_fallible_async_iter()
    }
}

impl List<Client, usize> {
    async fn call(self) -> Result<usize> {
        self.client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                etcdserverpb::kv_client::KvClient::range,
                self.request,
            )
            .await
            .map(|r| r.count as usize)
    }
}

impl IntoFuture for List<Client, usize> {
    type Output = Result<usize>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(self.call())
    }
}
