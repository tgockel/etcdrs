use crate::{
    error::{Error, ErrorInner, ErrorKind},
    record::{KeyWithMetadata, Metadata, Record},
    ConnectionId, LeaseId, Result, Revision, Version,
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

    /// Create a client which will connect to the specified `host`.
    pub fn new(host: &str) -> Result<Self> {
        Self::builder().add_connection(host)?.build()
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    uri: Option<tonic::transport::Uri>,
    metrics: Option<Box<dyn MetricsCollector>>,
}

impl ClientBuilder {
    pub fn add_connection(mut self, host: impl AsRef<str>) -> Result<Self> {
        if self.uri.is_some() {
            return Err(ErrorInner::with_static_message(
                ErrorKind::Unknown,
                "only one connection is supported right now",
            )
            .into());
        }

        let uri = host
            .as_ref()
            .parse::<tonic::transport::Uri>()
            .map_err(|err| Error::new(ErrorKind::InvalidArgument, err.to_string()))?;
        self.uri = Some(uri);
        Ok(self)
    }

    pub fn metrics(mut self, metrics: impl MetricsCollector + 'static) -> Self {
        self.metrics = Some(Box::new(metrics));
        self
    }

    pub fn build(self) -> Result<Client> {
        let Some(uri) = self.uri else {
            return Err(ErrorInner::with_static_message(ErrorKind::InvalidArgument, "no connection was added").into());
        };

        let endpoint = tonic::transport::Channel::builder(uri);
        let channel = endpoint.connect_lazy();
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
            // TODO: Cycle channels if needed
            let (connection_id, channel) = self.get_connection()?;
            let metric = metrics::MetricsSpan::new(connection_id, &self.metrics);

            let response = {
                let mut client = create_client(channel);
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

    fn get_connection(&self) -> Result<(ConnectionId, tonic::transport::Channel)> {
        // TODO: choose a channel based on some sort of criteria
        Ok((ConnectionId::new(1).unwrap(), self.channel.clone()))
    }
}
