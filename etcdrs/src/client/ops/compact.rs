use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Client, ResponseHeader, Revision, pb::etcdserverpb};

/// # Compaction
impl Client {
    /// Compact the key-value store's revision history up to `revision`.
    ///
    /// Compaction discards superseded key revisions older than `revision`, freeing space in the
    /// store. Reading history (for example, watching with a `start_revision`) from before the
    /// compacted revision will fail afterwards. Use [`physical`][Compact::physical] to wait until
    /// the compaction is physically applied to the backend database.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let revision = client.put("foo").value("bar").await.unwrap().header().revision();
    /// client.compact(revision).physical().await.unwrap();
    /// # };
    /// ```
    pub fn compact(&self, revision: Revision) -> Compact<Self> {
        Compact::new(revision).with_client(self.clone())
    }
}

/// A [`Client::compact`] operation.
#[derive(Clone, Debug)]
#[must_use = "Compact does nothing unless you `await` it"]
pub struct Compact<C> {
    client: C,
    pub(crate) request: etcdserverpb::CompactionRequest,
}

impl Compact<()> {
    pub fn new(revision: Revision) -> Self {
        Self {
            client: (),
            request: etcdserverpb::CompactionRequest {
                revision: revision.get(),
                ..Default::default()
            },
        }
    }
}

impl<C> Compact<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`compact`][`Client::compact`].
    pub fn with_client<C2>(self, client: C2) -> Compact<C2> {
        Compact {
            client,
            request: self.request,
        }
    }

    /// Wait for the compaction to be physically applied to the database.
    ///
    /// By default, the server responds as soon as the compaction is logically applied. With this
    /// set, the response is not sent until the compacted entries are actually removed from the
    /// backend database and the space is reclaimable.
    pub fn physical(mut self) -> Self {
        self.request.physical = true;
        self
    }

    /// The revision this operation will compact up to.
    pub fn revision(&self) -> Revision {
        Revision::new(self.request.revision).expect("compaction revision should be non-zero")
    }

    /// Whether this operation waits for the compaction to be physically applied.
    pub fn is_physical(&self) -> bool {
        self.request.physical
    }

    /// Decompose this operation into its client and a detached `Compact<()>`.
    pub(crate) fn into_parts(self) -> (C, Compact<()>) {
        (
            self.client,
            Compact {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The response from a [`compact`][Client::compact] operation.
#[derive(Clone, Copy, Debug)]
pub struct CompactResponse {
    header: ResponseHeader,
}

impl CompactResponse {
    /// Construct a new `CompactResponse`.
    pub fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The [`Future`] type returned by awaiting a [`compact`][`Client::compact`].
pub struct CompactFuture(Pin<Box<dyn Future<Output = Result<CompactResponse, CompactError>> + Send>>);

impl CompactFuture {
    pub(crate) fn new(future: impl Future<Output = Result<CompactResponse, CompactError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for CompactFuture {
    type Output = Result<CompactResponse, CompactError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::KvDriver> IntoFuture for Compact<C> {
    type Output = Result<CompactResponse, CompactError>;
    type IntoFuture = C::CompactFuture;

    fn into_future(self) -> C::CompactFuture {
        let (client, detached) = self.into_parts();
        client.execute_compact(detached)
    }
}

/// An enumeration of the [`kind`][CompactError::kind]s of errors that can occur from a
/// [`compact`][Client::compact] operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactErrorKind {
    /// The requested revision has already been compacted.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`. Because the client retries requests when
    /// the connection drops, a compaction that succeeded on the server but whose response was lost
    /// can also surface as this error on retry.
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
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a [`compact`][Client::compact] operation.
    pub struct CompactError(CompactErrorKind);
}

impl CompactError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::Unauthenticated => CompactErrorKind::Authentication,
            tonic::Code::PermissionDenied => CompactErrorKind::Authentication,
            // NOTE: Other "invalid arguments" won't be returned because we won't send bad arguments
            tonic::Code::InvalidArgument => CompactErrorKind::Authentication,
            tonic::Code::ResourceExhausted => CompactErrorKind::Exhausted,
            tonic::Code::OutOfRange => {
                if status.message().contains("compacted") {
                    CompactErrorKind::CompactedRevision
                } else if status.message().contains("future") {
                    CompactErrorKind::FutureRevision
                } else {
                    CompactErrorKind::Unknown
                }
            }
            tonic::Code::DataLoss => CompactErrorKind::DataLoss,
            tonic::Code::Unavailable => CompactErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => CompactErrorKind::Timeout,
            _ => CompactErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<CompactFuture>();
    }
};
