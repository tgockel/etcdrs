use crate::{
    error::{Error, ErrorInner, ErrorKind},
    record::{AsKey, Metadata, Record},
    LeaseId, Result, Revision, Version,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod ops;
pub use ops::*;

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

    pub fn get(&self, key: impl AsKey) -> Get<Self> {
        Get::new(key).with_client(self.clone())
    }

    pub fn put(&self, key: impl AsKey) -> Put<Self, ()> {
        Put::new(key).with_client(self.clone())
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

fn record_from_pb(r: crate::pb::mvccpb::KeyValue) -> Record {
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
