use fallible_async_iterator::FallibleAsyncIterator;
use futures_core::Stream;

use crate::{
    client::{key_with_metadata_from_pb, record_from_pb, BoxedFuture, Client},
    error::{ErrorInner, ErrorKind},
    pb::{etcdserverpb, mvccpb},
    record::{AsKey, KeyWithMetadata, Record},
    Result,
};
use std::{
    future::IntoFuture,
    marker,
    ops::{Bound, RangeBounds},
    pin::Pin,
    task::{Context, Poll},
};

pub struct List<C = (), R = Record> {
    client: C,
    request: etcdserverpb::RangeRequest,
    _return: marker::PhantomData<R>,
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
        let lower = prefix.as_key().to_owned();
        let upper = add_one(&lower);
        self._with_range(lower, upper)
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
    /// ```
    pub fn range<Range: RangeBounds<impl AsKey>>(self, range: Range) -> Self {
        let lower = match range.start_bound() {
            Bound::Included(val) => Vec::from(val.as_key()),
            Bound::Excluded(val) => successor(val.as_key()),
            Bound::Unbounded => vec![0],
        };
        let upper = match range.end_bound() {
            Bound::Included(val) => add_one(val.as_key()),
            Bound::Excluded(val) => Vec::from(val.as_key()),
            Bound::Unbounded => vec![0],
        };
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
                    request.key = successor(&last_kv.key)
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

impl fallible_async_iterator::IntoFallibleAsyncIterator for List<Client, Record> {
    type Item = Record;
    type Error = crate::Error;
    type IntoFallibleAsyncIter = ListIterator<Record>;

    fn into_fallible_async_iter(self) -> Self::IntoFallibleAsyncIter {
        ListIterator {
            iter: Box::new(self.stream_results()),
            convert: record_from_pb,
        }
    }
}

impl fallible_async_iterator::IntoFallibleAsyncIterator for List<Client, KeyWithMetadata> {
    type Item = KeyWithMetadata;
    type Error = crate::Error;
    type IntoFallibleAsyncIter = ListIterator<KeyWithMetadata>;

    fn into_fallible_async_iter(self) -> Self::IntoFallibleAsyncIter {
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

impl<R> FallibleAsyncIterator for ListIterator<R> {
    type Item = R;
    type Error = crate::Error;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<Self::Item>, Self::Error>> {
        let convert = self.as_ref().convert;
        let iter = unsafe { self.map_unchecked_mut(|s| s.iter.as_mut()) };
        iter.poll_next(cx).map(|item| match item {
            None => Ok(None),
            Some(Ok(raw)) => Ok(Some(convert(raw))),
            Some(Err(err)) => Err(err),
        })
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

/// Add one bit to the last element of `input`, carrying left on overflow.
///
/// This is used in range queries to specify "include this `input`".
fn add_one(input: &[u8]) -> Vec<u8> {
    let mut out = input.to_owned();
    while let Some(last) = out.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            break;
        } else {
            out.pop();
        }
    }
    // special case -- input was a string of 0xff, so put in a 0x00 which means to get until the end of the database
    if out.is_empty() {
        out.push(0);
    }
    out
}

/// Get the "next" key that follows `input`.
fn successor(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + 1);
    input.clone_into(&mut out);
    out.push(0);
    return out;
}

#[cfg(test)]
mod tests {
    use super::add_one;

    #[test]
    fn test_add_one() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"aa", b"ab"),
            (b"a\xff", b"b"),
            (b"\xff", b"\0"),
            (b"\xff\xff\xff", b"\0"),
        ];
        for (input, expected) in cases {
            let output = add_one(input);
            assert_eq!(*expected, output);
        }
    }
}
