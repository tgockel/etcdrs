use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    client::record_from_pb,
    pb::etcdserverpb,
    record::{AsKey, Record},
    Client, ResponseHeader,
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
                key: key.as_key().to_owned(),
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
                GetErrorKind::TooMany,
                "call to get should have only 1 response",
                None,
            ))
        } else if let Some(r) = resp.kvs.into_iter().next() {
            Ok(GetResponse { header, record: Some(record_from_pb(r)) })
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GetErrorKind {
    /// Too many results were returned for a single-key get.
    TooMany,
    /// A gRPC transport or unexpected error.
    Transport,
}

define_op_error! {
    /// An error from a [`get`][Client::get] operation.
    pub struct GetError(GetErrorKind);
}

impl GetError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        Self::new(GetErrorKind::Transport, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<GetFuture>();
    }
};
