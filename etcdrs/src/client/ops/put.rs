use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    client::record_from_pb,
    pb::etcdserverpb,
    record::{AsKey, AsValue, Record},
    Client, LeaseId, ResponseHeader,
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
    _return: PhantomData<R>,
}

impl Put<(), ()> {
    pub fn new(key: impl AsKey) -> Self {
        Self {
            client: (),
            request: etcdserverpb::PutRequest {
                key: key.as_key().to_owned(),
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
        self.request.value = value.as_value().into();
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

impl<R> Put<Client, R> {
    async fn call(self) -> Result<(ResponseHeader, Option<Record>), PutError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.put(r).await,
                self.request,
            )
            .await
            .map_err(PutError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("PutResponse should have a valid header"));
        Ok((header, resp.prev_kv.map(record_from_pb)))
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
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

impl PutResponse<Record> {
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

impl IntoFuture for Put<Client, GetPreviousValue> {
    type Output = Result<PutResponse<Record>, PutError>;
    type IntoFuture = PutFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        PutFuture(Box::pin(async move {
            let (header, previous) = self.call().await?;
            Ok(PutResponse { header, previous, _marker: PhantomData })
        }))
    }
}

impl IntoFuture for Put<Client, ()> {
    type Output = Result<PutResponse, PutError>;
    type IntoFuture = PutFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        PutFuture(Box::pin(async move {
            let (header, _) = self.call().await?;
            Ok(PutResponse { header, previous: None, _marker: PhantomData })
        }))
    }
}

/// Used in [`Put`]s to denote that the previous value should be returned.
pub struct GetPreviousValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PutErrorKind {
    /// The arguments were invalid.
    InvalidArgument,
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`put`][Client::put] operation.
    pub struct PutError(PutErrorKind);
}

impl PutError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::InvalidArgument => PutErrorKind::InvalidArgument,
            _ => PutErrorKind::Transport,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<PutFuture<Result<PutResponse, PutError>>>();
        _assert_send::<PutFuture<Result<PutResponse<Record>, PutError>>>();
    }
};
