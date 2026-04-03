use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    client::record_from_pb,
    error::ErrorInner,
    pb::etcdserverpb,
    record::{AsKey, Record},
    Client, ErrorKind, Result,
};

impl Client {
    /// Get the contents of `key` from the database.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// if let Some(entry) = client.get("path/to/foo").await.unwrap() {
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
    async fn call(self) -> Result<Option<Record>> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.range(r).await,
                self.request,
            )
            .await?;
        if resp.more || resp.kvs.len() > 1 {
            Err(ErrorInner::with_static_message(ErrorKind::TooMany, "call to get should have only 1 response").into())
        } else if let Some(r) = resp.kvs.into_iter().next() {
            Ok(Some(record_from_pb(r)))
        } else {
            Ok(None)
        }
    }
}

/// The [`Future`] type returned by awaiting a [`get`][`Client::get`].
pub struct GetFuture(Pin<Box<dyn Future<Output = Result<Option<Record>>> + Send>>);

impl Future for GetFuture {
    type Output = Result<Option<Record>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl IntoFuture for Get<Client> {
    type Output = Result<Option<Record>>;
    type IntoFuture = GetFuture;

    fn into_future(self) -> Self::IntoFuture {
        GetFuture(Box::pin(self.call()))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<GetFuture>();
    }
};
