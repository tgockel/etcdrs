use std::{future::IntoFuture, marker::PhantomData};

use crate::{
    client::{record_from_pb, BoxedFuture},
    pb::etcdserverpb,
    record::{AsKey, AsValue, Record},
    Client, LeaseId, Result,
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
    request: etcdserverpb::PutRequest,
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
    async fn call(self) -> Result<Option<Record>> {
        let resp = self
            .client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                etcdserverpb::kv_client::KvClient::put,
                self.request,
            )
            .await?;
        Ok(resp.prev_kv.map(record_from_pb))
    }
}

impl IntoFuture for Put<Client, GetPreviousValue> {
    type Output = Result<Option<Record>>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(self.call())
    }
}

impl IntoFuture for Put<Client, ()> {
    type Output = Result<()>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(async move { self.call().await.map(|_| ()) })
    }
}

/// Used in [`Put`]s to denote that the previous value should be returned.
pub struct GetPreviousValue;
