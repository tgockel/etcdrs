use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{
    Client, LeaseId, ResponseHeader,
    client::record_from_pb,
    pb::etcdserverpb,
    record::{AsKey, AsValue, Record},
};

impl Client {
    /// Put a new value for the `key`.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// client
    ///     .put("foo")
    ///     .value("bar")
    ///     .await
    ///     .unwrap();
    /// # };
    /// ```
    pub fn put(&self, key: impl AsKey) -> Put<Self, ()> {
        Put::new(key).with_client(self.clone())
    }
}

#[derive(Clone, Debug)]
pub struct Put<C, R> {
    client: C,
    pub(crate) request: etcdserverpb::PutRequest,
    _return: PhantomData<fn() -> R>,
}

impl Put<(), ()> {
    pub fn new(key: impl AsKey) -> Self {
        Self {
            client: (),
            request: etcdserverpb::PutRequest {
                key: Bytes::copy_from_slice(key.as_key()),
                ..Default::default()
            },
            _return: PhantomData,
        }
    }
}

impl<C, R> Put<C, R> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by [`put`][`Client::put`] or when
    /// given to a transaction.
    pub fn with_client<C2>(self, client: C2) -> Put<C2, R> {
        Put {
            client,
            request: self.request,
            _return: PhantomData,
        }
    }

    /// Decompose this operation into its client and a detached `Put<(), R>`.
    pub(crate) fn into_parts(self) -> (C, Put<(), R>) {
        (
            self.client,
            Put {
                client: (),
                request: self.request,
                _return: PhantomData,
            },
        )
    }

    /// Return the previous key-value.
    pub fn get_previous(self) -> Put<C, GetPreviousValue> {
        let mut request = self.request;
        request.prev_kv = true;
        Put {
            client: self.client,
            request,
            _return: Default::default(),
        }
    }

    /// Set the record to `value`.
    pub fn value(mut self, value: impl AsValue) -> Self {
        self.request.value = Bytes::copy_from_slice(value.as_value());
        self.request.ignore_value = false;
        self
    }

    /// Update the key, leaving the current value in-place.
    pub fn ignore_value(mut self) -> Self {
        self.request.value.clear();
        self.request.ignore_value = true;
        self
    }

    pub fn lease(mut self, lease: LeaseId) -> Self {
        self.request.lease = lease.get();
        self.request.ignore_lease = false;
        self
    }

    /// Update the key using its current lease. If the key does not exist, `NotFound` will be returned.
    pub fn ignore_lease(mut self) -> Self {
        self.request.lease = 0;
        self.request.ignore_lease = true;
        self
    }
}

/// The response from a [`put`][Client::put] operation.
#[derive(Clone, Debug)]
pub struct PutResponse<R = ()> {
    header: ResponseHeader,
    previous: Option<Record>,
    _marker: PhantomData<R>,
}

impl<R> PutResponse<R> {
    /// Construct a new `PutResponse`.
    pub fn new(header: ResponseHeader, previous: Option<Record>) -> Self {
        Self {
            header,
            previous,
            _marker: PhantomData,
        }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

impl PutResponse<GetPreviousValue> {
    /// The previous value of the key, or `None` if the key did not exist before the put.
    pub fn previous(&self) -> Option<&Record> {
        self.previous.as_ref()
    }

    /// Consume the response and return the previous value.
    pub fn into_previous(self) -> Option<Record> {
        self.previous
    }
}

/// The [`Future`] type returned by awaiting [`put`][Client::put] operations.
pub struct PutFuture<T>(Pin<Box<dyn Future<Output = T> + Send>>);

impl<T> Future for PutFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl crate::driver::PutDriver for Client {
    type PutFuture<R> = PutFuture<Result<PutResponse<R>, PutError>>;

    fn execute_put<R>(self, put: Put<(), R>) -> Self::PutFuture<R> {
        let request = put.request;
        PutFuture(Box::pin(async move {
            let resp = self
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    async |c, r| c.put(r).await,
                    request,
                )
                .await
                .map_err(PutError::from_status)?;
            let header = ResponseHeader::from_pb(resp.header.expect("PutResponse should have a valid header"));
            let previous = resp.prev_kv.map(record_from_pb);
            Ok(PutResponse::new(header, previous))
        }))
    }
}

impl<C, R> IntoFuture for Put<C, R>
where
    C: crate::driver::PutDriver + Send + 'static,
    R: Send + 'static,
{
    type Output = Result<PutResponse<R>, PutError>;
    type IntoFuture = PutFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        PutFuture(Box::pin(async move { client.execute_put(detached).await }))
    }
}

/// Used in [`Put`]s to denote that the previous value should be returned.
pub struct GetPreviousValue;

/// An enumeration of the [`kind`][PutError::kind]s of errors that can occur from a [`put`][Client::put] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutErrorKind {
    /// The specified lease does not exist.
    ///
    /// This comes from the gRPC API as `NOT_FOUND`.
    LeaseNotFound,
    /// The key does not exist, but the operation requires it.
    ///
    /// This happens when using [`ignore_value`][Put::ignore_value] or [`ignore_lease`][Put::ignore_lease] on a key that
    /// does not exist in the store. This comes from the gRPC API as `INVALID_ARGUMENT` with "key not found".
    KeyNotFound,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and `INVALID_ARGUMENT` when the argument
    /// describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a [`put`][Client::put] operation.
    pub struct PutError(PutErrorKind);
}

impl PutError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::NotFound => PutErrorKind::LeaseNotFound,
            tonic::Code::Unauthenticated => PutErrorKind::Authentication,
            tonic::Code::PermissionDenied => PutErrorKind::Authentication,
            tonic::Code::InvalidArgument => {
                if status.message().contains("key not found") {
                    PutErrorKind::KeyNotFound
                } else {
                    // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
                    PutErrorKind::Authentication
                }
            }
            tonic::Code::ResourceExhausted => PutErrorKind::Exhausted,
            tonic::Code::DataLoss => PutErrorKind::DataLoss,
            tonic::Code::Unavailable => PutErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => PutErrorKind::Timeout,
            _ => PutErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PutFuture<Result<PutResponse, PutError>>>();
        _assert_send::<PutFuture<Result<PutResponse<GetPreviousValue>, PutError>>>();
    }
};
