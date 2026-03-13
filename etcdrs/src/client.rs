use crate::{
    error::{Error, ErrorInner, ErrorKind},
    record::{KeyWithMetadata, Metadata, Record},
    LeaseId, Result, Revision, Version,
};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod metrics;
pub use metrics::{MetricsCollector, RequestCount, RequestCounter};
mod ops;
pub use ops::*;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Create a client from a connection string.
    ///
    /// The connection string is a comma-separated list of endpoint URIs, following the etcd
    /// convention used by `etcdctl --endpoints`:
    ///
    /// ```text
    /// http://host1:2379,http://host2:2379,http://host3:2379
    /// ```
    ///
    /// A single URI is also accepted.
    pub fn new(connection_string: &str) -> Result<Self> {
        Self::builder().connection_string(connection_string)?.build()
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    uris: Vec<tonic::transport::Uri>,
    metrics: Option<Box<dyn MetricsCollector>>,
}

impl ClientBuilder {
    /// Parse a comma-separated connection string and add all endpoints.
    pub fn connection_string(mut self, connection_string: impl AsRef<str>) -> Result<Self> {
        for host in connection_string.as_ref().split(',') {
            let host = host.trim();
            if !host.is_empty() {
                self = self.add_connection(host)?;
            }
        }
        Ok(self)
    }

    /// Add a single endpoint.
    pub fn add_connection(mut self, host: impl AsRef<str>) -> Result<Self> {
        let uri = host
            .as_ref()
            .parse::<tonic::transport::Uri>()
            .map_err(|err| Error::new(ErrorKind::InvalidArgument, err.to_string()))?;
        self.uris.push(uri);
        Ok(self)
    }

    pub fn metrics(mut self, metrics: impl MetricsCollector + 'static) -> Self {
        self.metrics = Some(Box::new(metrics));
        self
    }

    pub fn build(self) -> Result<Client> {
        if self.uris.is_empty() {
            return Err(ErrorInner::with_static_message(ErrorKind::InvalidArgument, "no connection was added").into());
        }

        let channel = if self.uris.len() == 1 {
            let endpoint = tonic::transport::Channel::builder(self.uris.into_iter().next().unwrap());
            endpoint.connect_lazy()
        } else {
            let endpoints = self.uris.into_iter().map(tonic::transport::Channel::builder);
            tonic::transport::Channel::balance_list(endpoints)
        };

        Ok(Client {
            inner: Arc::new(ClientInner {
                channel,
                metrics: self.metrics,
            }),
        })
    }
}

struct ClientInner {
    channel: tonic::transport::Channel,
    metrics: Option<Box<dyn MetricsCollector>>,
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

fn metadata_from_pb(r: &crate::pb::mvccpb::KeyValue) -> Metadata {
    Metadata {
        create_revision: Revision::new(r.create_revision).unwrap(),
        modified_revision: Revision::new(r.mod_revision).unwrap(),
        version: Version::new(r.version as u64),
        lease: LeaseId::new(r.lease),
    }
}

fn record_from_pb(r: crate::pb::mvccpb::KeyValue) -> Record {
    let metadata = metadata_from_pb(&r);
    Record::new(r.key, r.value, metadata)
}

fn key_with_metadata_from_pb(r: crate::pb::mvccpb::KeyValue) -> KeyWithMetadata {
    let metadata = metadata_from_pb(&r);
    KeyWithMetadata::new(r.key, metadata)
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
            let metric = metrics::MetricsSpan::new(&self.metrics);

            let response = {
                let mut client = create_client(self.channel.clone());
                // safety -- this transmute is only needed to give `'a` some sort of lifetime. It is needed because Rust
                // does not support async closures as of 2024, so the `&mut self` on an async function needs some sort
                // of lifetime.
                let client_ref: &'a mut GrpcClient = unsafe { std::mem::transmute(&mut client) };
                call(client_ref, request.clone()).await
            };
            metric.complete(response.is_ok());
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
                        Code::FailedPrecondition => Error::new(ErrorKind::FailedPrecondition, e.message()),
                        Code::NotFound => Error::new(ErrorKind::NotFound, e.message()),
                        _ => ErrorInner::from_unknown(e).into(),
                    };
                    return Err(err);
                }
            }
        }
    }
}
