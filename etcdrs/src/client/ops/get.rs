use std::future::IntoFuture;

use crate::{
    client::{record_from_pb, BoxedFuture},
    error::ErrorInner,
    pb::etcdserverpb,
    record::{AsKey, Record},
    Client, ErrorKind, Result,
};

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
                etcdserverpb::kv_client::KvClient::range,
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

impl IntoFuture for Get<Client> {
    type Output = Result<Option<Record>>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(self.call())
    }
}
