use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{
    AsRange, Client, Prefix, ResponseHeader,
    client::{GetPreviousValue, record_from_pb},
    pb::etcdserverpb,
    record::{AsKey, Record},
};

impl Client {
    /// Delete the given `key` from the database.
    ///
    /// The value returned from `await`ing the operation is `true` if the key was deleted or `false` if the key was not
    /// found. In other words, it is not an error to try to delete a key that does not exist.
    ///
    /// ```no_run
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// let deleted = client
    ///     .delete("foo")
    ///     .await
    ///     .unwrap()
    ///     .deleted();
    /// println!("deleted? {deleted}");
    /// # };
    /// ```
    pub fn delete(&self, key: impl AsKey) -> Delete<Self, bool> {
        Delete::new(key).with_client(self.clone())
    }

    /// Delete all keys matching the given `range`.
    ///
    /// The value returned from `await`ing the operation is the number of keys that were deleted.
    ///
    /// ```no_run
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// // Delete a range of keys
    /// let count = client.delete_range("foo/a".."foo/z").await.unwrap().deleted();
    /// println!("deleted {count} keys");
    ///
    /// // Delete ranges use the Rust range syntax:
    /// let _ = client.delete_range(..);           // all keys
    /// let _ = client.delete_range("a"..="b");    // from "a" up to and including "b"
    /// let _ = client.delete_range(.."taco");     // everything up to "taco"
    /// let _ = client.delete_range("taco"..);     // "taco" and everything after
    /// # };
    /// ```
    ///
    /// You can specify a [prefix][crate::Prefix] query as well, but it is usually easier to use the
    /// [`delete_prefix`][Self::delete_prefix] method instead.
    pub fn delete_range(&self, range: impl AsRange) -> Delete<Self, usize> {
        Delete::with_range(range).with_client(self.clone())
    }

    /// Delete all keys matching the given `prefix`.
    ///
    /// This is the same as calling [`delete_range`][Self::delete_range] with a [`Prefix`] query.
    ///
    /// ```no_run
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// let count = client.delete_prefix("foo/").await.unwrap().deleted();
    /// println!("deleted {count} keys");
    /// # };
    /// ```
    pub fn delete_prefix(&self, prefix: impl AsKey) -> Delete<Self, usize> {
        self.delete_range(Prefix(prefix))
    }
}

#[derive(Clone, Debug)]
pub struct Delete<C, R, P = ()> {
    client: C,
    pub(crate) request: etcdserverpb::DeleteRangeRequest,
    _return: PhantomData<[(R, P); 0]>,
}

impl Delete<(), bool> {
    pub fn new(key: impl AsKey) -> Self {
        Self {
            client: (),
            request: etcdserverpb::DeleteRangeRequest {
                key: Bytes::copy_from_slice(key.as_key()),
                ..Default::default()
            },
            _return: PhantomData,
        }
    }
}

impl Delete<(), usize> {
    /// Create a new delete operation with the specified `range`.
    ///
    /// See [`delete_range`][Client::delete_range] for more information.
    pub fn with_range(range: impl AsRange) -> Self {
        let (key, range_end) = range.as_boundaries();
        Self {
            client: (),
            request: etcdserverpb::DeleteRangeRequest {
                key,
                range_end,
                ..Default::default()
            },
            _return: PhantomData,
        }
    }

    /// Create a new delete operation with the specified `prefix`.
    ///
    /// See [`delete_prefix`][Client::delete_prefix] for more information.
    pub fn with_prefix(prefix: impl AsKey) -> Self {
        Self::with_range(Prefix(prefix))
    }
}

impl<C, R, P> Delete<C, R, P> {
    pub fn with_client<C2>(self, client: C2) -> Delete<C2, R, P> {
        Delete {
            client,
            request: self.request,
            _return: PhantomData,
        }
    }

    /// Return the previous key-value.
    ///
    /// If you are deleting a single key, this means the previous value of the key will be returned.
    /// If you are deleting a range, this means the previous key-values in the range will be
    /// returned in a vector.
    ///
    /// > **WARNING**
    /// >
    /// > Getting the previous value of the key has no pagination mechanism. If you are deleting a
    /// > range (or a large key), the delete will succeed, but the response will fail with a
    /// > `RESOURCE_EXHAUSTED` "message too large" error. There is not really a good way to handle
    /// > this, so be careful when using this method.
    pub fn get_previous(self) -> Delete<C, R, GetPreviousValue> {
        let mut request = self.request;
        request.prev_kv = true;
        Delete {
            client: self.client,
            request,
            _return: PhantomData,
        }
    }
}

impl<R, P> Delete<Client, R, P> {
    async fn call(self) -> Result<(ResponseHeader, etcdserverpb::DeleteRangeResponse), DeleteError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.delete_range(r).await,
                self.request,
            )
            .await
            .map_err(DeleteError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("DeleteRangeResponse should have a valid header"));
        Ok((header, resp))
    }
}

/// The response from a [`delete`][Client::delete] or [`delete_range`][Client::delete_range]
/// operation.
#[derive(Clone, Debug)]
pub struct DeleteResponse<R, P = ()> {
    header: ResponseHeader,
    deleted: usize,
    previous: Vec<Record>,
    _marker: PhantomData<fn() -> (R, P)>,
}

impl<R, P> DeleteResponse<R, P> {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

impl<P> DeleteResponse<bool, P> {
    /// Returns `true` if the key was deleted, `false` if it did not exist.
    pub fn deleted(&self) -> bool {
        self.deleted > 0
    }
}

impl<P> DeleteResponse<usize, P> {
    /// The number of keys that were deleted.
    pub fn deleted(&self) -> usize {
        self.deleted
    }
}

impl DeleteResponse<bool, Record> {
    /// The previous value of the key, or `None` if it did not exist.
    pub fn previous(&self) -> Option<&Record> {
        self.previous.first()
    }

    /// Consume the response and return the previous value.
    pub fn into_previous(self) -> Option<Record> {
        self.previous.into_iter().next()
    }
}

impl DeleteResponse<usize, Record> {
    /// The previous values of the deleted keys.
    pub fn previous(&self) -> &[Record] {
        &self.previous
    }

    /// Consume the response and return the previous values.
    pub fn into_previous(self) -> Vec<Record> {
        self.previous
    }
}

/// The [`Future`] type returned by awaiting [`delete`][Client::delete] and
/// [`delete_range`][Client::delete_range] operations.
pub struct DeleteFuture<T>(Pin<Box<dyn Future<Output = T> + Send>>);

impl<T> Future for DeleteFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for Delete<Client, bool, ()> {
    type Output = Result<DeleteResponse<bool>, DeleteError>;
    type IntoFuture = DeleteFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        DeleteFuture(Box::pin(async move {
            let (header, resp) = self.call().await?;
            Ok(DeleteResponse {
                header,
                deleted: resp.deleted as usize,
                previous: Vec::new(),
                _marker: PhantomData,
            })
        }))
    }
}

impl IntoFuture for Delete<Client, bool, GetPreviousValue> {
    type Output = Result<DeleteResponse<bool, Record>, DeleteError>;
    type IntoFuture = DeleteFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        DeleteFuture(Box::pin(async move {
            let (header, resp) = self.call().await?;
            Ok(DeleteResponse {
                header,
                deleted: resp.deleted as usize,
                previous: resp.prev_kvs.into_iter().map(record_from_pb).collect(),
                _marker: PhantomData,
            })
        }))
    }
}

impl IntoFuture for Delete<Client, usize, ()> {
    type Output = Result<DeleteResponse<usize>, DeleteError>;
    type IntoFuture = DeleteFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        DeleteFuture(Box::pin(async move {
            let (header, resp) = self.call().await?;
            Ok(DeleteResponse {
                header,
                deleted: resp.deleted as usize,
                previous: Vec::new(),
                _marker: PhantomData,
            })
        }))
    }
}

impl IntoFuture for Delete<Client, usize, GetPreviousValue> {
    type Output = Result<DeleteResponse<usize, Record>, DeleteError>;
    type IntoFuture = DeleteFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        DeleteFuture(Box::pin(async move {
            let (header, resp) = self.call().await?;
            Ok(DeleteResponse {
                header,
                deleted: resp.deleted as usize,
                previous: resp.prev_kvs.into_iter().map(record_from_pb).collect(),
                _marker: PhantomData,
            })
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeleteErrorKind {
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`delete`][Client::delete] or [`delete_range`][Client::delete_range] operation.
    pub struct DeleteError(DeleteErrorKind);
}

impl DeleteError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        Self::new(DeleteErrorKind::Transport, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<DeleteFuture<Result<DeleteResponse<bool>, DeleteError>>>();
        _assert_send::<DeleteFuture<Result<DeleteResponse<bool, Record>, DeleteError>>>();
        _assert_send::<DeleteFuture<Result<DeleteResponse<usize>, DeleteError>>>();
        _assert_send::<DeleteFuture<Result<DeleteResponse<usize, Record>, DeleteError>>>();
    }
};
