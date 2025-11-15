use std::{future::IntoFuture, marker::PhantomData};

use crate::{
    client::{record_from_pb, BoxedFuture, GetPreviousValue},
    pb::etcdserverpb,
    record::{AsKey, Record},
    Client, Result,
};

impl Client {
    /// Delete the given `key` from the database.
    ///
    /// The value returned from `await`ing the operation is `true` if the key was deleted or `false` if the key was not
    /// found. In other words, it is not an error to try to delete a key that does not exist.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let deleted = client
    ///     .delete("foo")
    ///     .await
    ///     .unwrap();
    /// println!("deleted? {deleted}");
    /// # };
    /// ```
    pub fn delete(&self, key: impl AsKey) -> Delete<Self, bool> {
        Delete::new(key).with_client(self.clone())
    }
}

#[derive(Clone, Debug)]
pub struct Delete<C, R, P = ()> {
    client: C,
    request: etcdserverpb::DeleteRangeRequest,
    _return: PhantomData<[(R, P); 0]>,
}

impl Delete<(), bool> {
    pub fn new(key: impl AsKey) -> Self {
        Self {
            client: (),
            request: etcdserverpb::DeleteRangeRequest {
                key: key.as_key().to_owned(),
                ..Default::default()
            },
            _return: PhantomData,
        }
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

impl IntoFuture for Delete<Client, bool, ()> {
    type Output = Result<bool>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(async move {
            let resp = self
                .client
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    etcdserverpb::kv_client::KvClient::delete_range,
                    self.request,
                )
                .await?;
            Ok(resp.deleted > 0)
        })
    }
}

impl IntoFuture for Delete<Client, bool, GetPreviousValue> {
    type Output = Result<Option<Record>>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(async move {
            let resp = self
                .client
                .inner
                .wrap_unary_call(
                    etcdserverpb::kv_client::KvClient::new,
                    etcdserverpb::kv_client::KvClient::delete_range,
                    self.request,
                )
                .await?;
            Ok(resp.prev_kvs.into_iter().next().map(record_from_pb))
        })
    }
}
