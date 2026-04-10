use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{
    Client, ResponseHeader,
    client::record_from_pb,
    pb::etcdserverpb,
    record::{AsKey, Record},
};

impl Client {
    /// Get the contents of `key` from the database.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let response = client.get("path/to/foo").await.unwrap();
    /// if let Some(entry) = response.record() {
    ///     println!("found: {entry:?}");
    /// } else {
    ///     println!("not found");
    /// }
    /// # };
    /// ```
    pub fn get(&self, key: impl AsKey) -> Get<Self> {
        Get::new(key).with_client(self.clone())
    }
}

#[derive(Clone, Debug)]
pub struct Get<C> {
    pub(crate) client: C,
    pub(crate) request: etcdserverpb::RangeRequest,
}

impl Get<()> {
    pub fn new(key: impl AsKey) -> Self {
        Self {
            client: (),
            request: etcdserverpb::RangeRequest {
                key: Bytes::copy_from_slice(key.as_key()),
                ..Default::default()
            },
        }
    }
}

impl<C> Get<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by [`get`][`Client::get`].
    pub fn with_client<C2>(self, client: C2) -> Get<C2> {
        Get {
            client,
            request: self.request,
        }
    }
}

impl Get<Client> {
    async fn call(self) -> Result<GetResponse, GetError> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.range(r).await,
                self.request,
            )
            .await
            .map_err(GetError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("RangeResponse should have a valid header"));
        if resp.more || resp.kvs.len() > 1 {
            Err(GetError::new(
                GetErrorKind::Unknown,
                "call to get should have only 1 response",
                None,
            ))
        } else if let Some(r) = resp.kvs.into_iter().next() {
            Ok(GetResponse {
                header,
                record: Some(record_from_pb(r)),
            })
        } else {
            Ok(GetResponse { header, record: None })
        }
    }
}

/// The response from a [`get`][Client::get] operation.
#[derive(Clone, Debug)]
pub struct GetResponse {
    header: ResponseHeader,
    record: Option<Record>,
}

impl GetResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The record associated with the key, or `None` if the key does not exist.
    pub fn record(&self) -> Option<&Record> {
        self.record.as_ref()
    }

    /// Consume the response and return the record, or `None` if the key does not exist.
    pub fn into_record(self) -> Option<Record> {
        self.record
    }
}

/// The [`Future`] type returned by awaiting a [`get`][`Client::get`].
pub struct GetFuture(Pin<Box<dyn Future<Output = Result<GetResponse, GetError>> + Send>>);

impl Future for GetFuture {
    type Output = Result<GetResponse, GetError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for Get<Client> {
    type Output = Result<GetResponse, GetError>;
    type IntoFuture = GetFuture;

    fn into_future(self) -> Self::IntoFuture {
        GetFuture(Box::pin(self.call()))
    }
}

/// A enumeration of the [`kind`][GetError::kind]s of errors that can occur from a [`get`][Client::get] or
/// [`list`][Client::list] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GetErrorKind {
    /// The requested revision is older than the server has.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`.
    CompactedRevision,
    /// The requested revision is newer than the server has.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`.
    FutureRevision,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and `INVALID_ARGUMENT` when the argument
    /// describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes. These can also be emitted from the client if the server does something unexpected
    /// (for example, returning more than one record for a single-key get).
    Unknown,
}

define_op_error! {
    /// An error from a [`get`][Client::get] or [`list`][Client::list] operation.
    pub struct GetError(GetErrorKind);
}

impl GetError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::Unauthenticated => GetErrorKind::Authentication,
            tonic::Code::PermissionDenied => GetErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => GetErrorKind::Authentication,
            tonic::Code::ResourceExhausted => GetErrorKind::Exhausted,
            tonic::Code::OutOfRange => {
                if status.message().contains("compacted") {
                    GetErrorKind::CompactedRevision
                } else if status.message().contains("future") {
                    GetErrorKind::FutureRevision
                } else {
                    GetErrorKind::Unknown
                }
            }
            tonic::Code::Unavailable => GetErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => GetErrorKind::Timeout,
            _ => GetErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<GetFuture>();
    }
};
