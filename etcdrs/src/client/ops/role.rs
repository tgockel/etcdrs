use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Client, ResponseHeader, pb::etcdserverpb};

impl Client {
    /// Add a new role to the cluster.
    pub fn role_add(&self, name: &str) -> RoleAdd<Self> {
        RoleAdd::new(name).with_client(self.clone())
    }
}

/// The response from a [`role_add`][Client::role_add] operation.
#[derive(Clone, Copy, Debug)]
pub struct RoleAddResponse {
    header: ResponseHeader,
}

impl RoleAddResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// A [`Client::role_add`] operation.
#[derive(Clone)]
#[must_use = "RoleAdd does nothing unless you `await` it"]
pub struct RoleAdd<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthRoleAddRequest,
}

impl RoleAdd<()> {
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthRoleAddRequest { name: name.into() },
        }
    }
}

impl<C> RoleAdd<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleAdd<C2> {
        RoleAdd {
            client,
            request: self.request,
        }
    }

    pub fn name(&self) -> &str {
        &self.request.name
    }

    pub(crate) fn into_parts(self) -> (C, RoleAdd<()>) {
        (
            self.client,
            RoleAdd {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct RoleAddFuture(Pin<Box<dyn Future<Output = Result<RoleAddResponse, RoleError>> + Send>>);

impl RoleAddFuture {
    pub(crate) fn new(future: impl Future<Output = Result<RoleAddResponse, RoleError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleAddFuture {
    type Output = Result<RoleAddResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleAdd<C> {
    type Output = Result<RoleAddResponse, RoleError>;
    type IntoFuture = C::RoleAddFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_add(detached)
    }
}

/// An enumeration of the [`kind`][RoleError::kind]s of errors that can occur from role management
/// operations ([`role_add`][Client::role_add]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoleErrorKind {
    /// The role already exists.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "already exists".
    RoleAlreadyExists,
    /// The role was not found.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "role name not found".
    RoleNotFound,
    /// Authentication is not enabled.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "not enabled".
    AuthNotEnabled,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED` and
    /// `INVALID_ARGUMENT` when the argument describes an authentication error.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish
    /// between the two, as the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless
    /// the etcd server has changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a role management operation ([`role_add`][Client::role_add]).
    pub struct RoleError(RoleErrorKind);
}

impl RoleError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("already exists") {
                    RoleErrorKind::RoleAlreadyExists
                } else if msg.contains("role name not found") {
                    RoleErrorKind::RoleNotFound
                } else if msg.contains("not enabled") {
                    RoleErrorKind::AuthNotEnabled
                } else {
                    RoleErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => RoleErrorKind::Authentication,
            tonic::Code::PermissionDenied => RoleErrorKind::Authentication,
            tonic::Code::InvalidArgument => RoleErrorKind::Authentication,
            tonic::Code::ResourceExhausted => RoleErrorKind::Exhausted,
            tonic::Code::Unavailable => RoleErrorKind::Unavailable,
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => RoleErrorKind::Timeout,
            _ => RoleErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}
