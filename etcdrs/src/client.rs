use crate::{
    error::{Error, ErrorInner, ErrorKind},
    pb::{etcdserverpb, mvccpb},
    record::{AsKey, AsValue, Metadata, Record},
    LeaseId, Result, Revision, Version,
};
use std::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub fn new(host: &str) -> Result<Self, String> {
        let uri: tonic::transport::Uri = host.parse().map_err(|e| format!("{e:?}"))?;
        let endpoint = tonic::transport::Channel::builder(uri);
        let channel = endpoint.connect_lazy();
        Ok(Self {
            inner: Arc::new(ClientInner { channel }),
        })
    }

    fn get_impl(&self, key: Vec<u8>) -> Get {
        Get {
            client: self.inner.clone(),
            request: etcdserverpb::RangeRequest {
                key,
                ..Default::default()
            },
        }
    }

    pub fn get(&self, key: impl AsKey) -> Get {
        self.get_impl(key.as_key().into())
    }

    fn put_impl(&self, key: Vec<u8>) -> Put<()> {
        Put {
            client: self.inner.clone(),
            request: etcdserverpb::PutRequest {
                key,
                ..Default::default()
            },
            _return: Default::default(),
        }
    }

    pub fn put(&self, key: impl AsKey) -> Put<()> {
        self.put_impl(key.as_key().into())
    }
}

struct ClientInner {
    channel: tonic::transport::Channel,
}

pub struct BoxedFuture<T> {
    inner: Box<dyn Future<Output = T>>,
}

impl<T> BoxedFuture<T> {
    fn new(src: impl Future<Output = T> + 'static) -> Self {
        Self { inner: Box::new(src) }
    }
}

impl<T> Future for BoxedFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let pinner = unsafe { self.map_unchecked_mut(|this| this.inner.as_mut()) };
        pinner.poll(cx)
    }
}

pub struct Get {
    client: Arc<ClientInner>,
    request: etcdserverpb::RangeRequest,
}

impl Get {
    async fn call(self) -> Result<Option<Record>> {
        let resp = self
            .client
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

impl IntoFuture for Get {
    type Output = Result<Option<Record>>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(self.call())
    }
}

pub struct GetPreviousValue;

pub struct Put<R> {
    client: Arc<ClientInner>,
    request: etcdserverpb::PutRequest,
    _return: PhantomData<R>,
}

impl<R> Put<R> {
    async fn call(self) -> Result<Option<Record>> {
        let resp = self
            .client
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                etcdserverpb::kv_client::KvClient::put,
                self.request,
            )
            .await?;
        Ok(resp.prev_kv.map(record_from_pb))
    }

    /// Return the previous key-value.
    pub fn get_previous(self) -> Put<GetPreviousValue> {
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

impl IntoFuture for Put<GetPreviousValue> {
    type Output = Result<Option<Record>>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(self.call())
    }
}

impl IntoFuture for Put<()> {
    type Output = Result<()>;
    type IntoFuture = BoxedFuture<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        BoxedFuture::new(async move { self.call().await.map(|_| ()) })
    }
}

fn record_from_pb(r: mvccpb::KeyValue) -> Record {
    let metadata = Metadata {
        create_revision: Revision::new(r.create_revision).unwrap(),
        modified_revision: Revision::new(r.mod_revision).unwrap(),
        version: Version::new(r.version as u64),
        lease: LeaseId::new(r.lease),
    };
    Record::new(r.key, r.value, metadata)
}

impl ClientInner {
    async fn wrap_unary_call<
        'a,
        R,
        F: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
        GrpcClient: 'a,
        Request: Clone,
    >(
        &self,
        create_client: impl Fn(tonic::transport::Channel) -> GrpcClient,
        call: impl Fn(&'a mut GrpcClient, Request) -> F,
        request: Request,
    ) -> Result<R, Error> {
        // TODO: configurable
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            // TODO: Cycle channels if needed
            let channel = self.channel.clone();
            let response = {
                let mut client = create_client(channel);
                // safety -- this transmute is only needed to give `'a` some sort of lifetime. It is needed because Rust
                // does not support async closures as of 2024, so the `&mut self` on an async function needs some sort
                // of lifetime.
                let client_ref: &'a mut GrpcClient = unsafe { std::mem::transmute(&mut client) };
                call(client_ref, request.clone()).await
            };
            match response {
                Ok(r) => break Ok(r.into_inner()),
                Err(e) => {
                    use tonic::Code;
                    let err = match e.code() {
                        Code::Unavailable => {
                            if std::time::Instant::now() > deadline {
                                Error::new(ErrorKind::Unavailable, e.message())
                            } else {
                                continue;
                            }
                        }
                        Code::InvalidArgument => Error::new(ErrorKind::InvalidArgument, e.message()),
                        _ => ErrorInner::from_unknown(e).into(),
                    };
                    return Err(err);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::task::JoinSet;

    use crate::error::ErrorKind;

    #[tokio::test]
    async fn foo() {
        let mut joinset = JoinSet::new();
        let server = crate::fake::FakeServer::builder().build().unwrap();
        let client = server.lazy_client();
        joinset.spawn(async move { server.run().await });
        tokio::task::yield_now().await;

        let abc = client.get(b"abc").await.unwrap();
        assert!(abc.is_none());

        client.put(b"abc").value(b"def").await.unwrap();

        let abc = client.get(b"abc").await.unwrap().unwrap();
        assert_eq!(*b"def", **abc.value());
    }

    #[tokio::test]
    async fn get_with_dead_server() {
        // we make a server, but never start it
        let server = crate::fake::FakeServer::builder().build().unwrap();
        let client = server.lazy_client();

        let err = client.get(b"abc").await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unavailable);
    }
}
