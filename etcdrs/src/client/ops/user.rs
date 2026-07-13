use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Client, ResponseHeader, pb::etcdserverpb};

impl Client {
    /// Add a new user to the cluster.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// client.user_add("admin")
    ///     .password("secret")
    ///     .await
    ///     .expect("failed to add user");
    /// # };
    /// ```
    pub fn user_add(&self, name: &str) -> UserAdd<Self> {
        UserAdd::new(name).with_client(self.clone())
    }

    /// Grant a role to a user.
    pub fn user_grant_role(&self, user: &str, role: &str) -> UserGrantRole<Self> {
        UserGrantRole::new(user, role).with_client(self.clone())
    }
}

/// A [`Client::user_add`] operation.
#[derive(Clone)]
#[must_use = "UserAdd does nothing unless you `await` it"]
pub struct UserAdd<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserAddRequest,
}

impl UserAdd<()> {
    #[allow(clippy::new_without_default)]
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserAddRequest {
                name: name.into(),
                ..Default::default()
            },
        }
    }
}

impl<C> UserAdd<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`user_add`][`Client::user_add`].
    pub fn with_client<C2>(self, client: C2) -> UserAdd<C2> {
        UserAdd {
            client,
            request: self.request,
        }
    }

    /// Set the password for the new user.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.request.password = password.into();
        self.request.options = None;
        self
    }

    /// Create the user without a password.
    ///
    /// Users without a password cannot authenticate directly but can be used with certificate-based
    /// authentication.
    pub fn no_password(mut self) -> Self {
        self.request.password = String::new();
        self.request.options = Some(crate::pb::authpb::UserAddOptions { no_password: true });
        self
    }

    /// The user name to create.
    pub fn name(&self) -> &str {
        &self.request.name
    }

    /// The configured password, if this user is password-authenticated.
    pub fn configured_password(&self) -> Option<&str> {
        (!self.is_no_password()).then_some(self.request.password.as_str())
    }

    /// Whether the user is created without a password.
    pub fn is_no_password(&self) -> bool {
        self.request.options.as_ref().is_some_and(|options| options.no_password)
    }

    pub(crate) fn into_parts(self) -> (C, UserAdd<()>) {
        (
            self.client,
            UserAdd {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The [`Future`] type returned by awaiting a [`user_add`][Client::user_add].
pub struct UserAddFuture(Pin<Box<dyn Future<Output = Result<UserAddResponse, UserError>> + Send>>);

impl UserAddFuture {
    pub(crate) fn new(future: impl Future<Output = Result<UserAddResponse, UserError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserAddFuture {
    type Output = Result<UserAddResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserAdd<C> {
    type Output = Result<UserAddResponse, UserError>;
    type IntoFuture = C::UserAddFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_add(detached)
    }
}

/// A [`Client::user_grant_role`] operation.
#[derive(Clone)]
#[must_use = "UserGrantRole does nothing unless you `await` it"]
pub struct UserGrantRole<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserGrantRoleRequest,
}

impl UserGrantRole<()> {
    pub fn new(user: &str, role: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserGrantRoleRequest {
                user: user.into(),
                role: role.into(),
            },
        }
    }
}

impl<C> UserGrantRole<C> {
    pub fn with_client<C2>(self, client: C2) -> UserGrantRole<C2> {
        UserGrantRole {
            client,
            request: self.request,
        }
    }

    pub fn user(&self) -> &str {
        &self.request.user
    }

    pub fn role(&self) -> &str {
        &self.request.role
    }

    pub(crate) fn into_parts(self) -> (C, UserGrantRole<()>) {
        (
            self.client,
            UserGrantRole {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct UserGrantRoleFuture(Pin<Box<dyn Future<Output = Result<UserGrantRoleResponse, UserError>> + Send>>);

impl UserGrantRoleFuture {
    pub(crate) fn new(future: impl Future<Output = Result<UserGrantRoleResponse, UserError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserGrantRoleFuture {
    type Output = Result<UserGrantRoleResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserGrantRole<C> {
    type Output = Result<UserGrantRoleResponse, UserError>;
    type IntoFuture = C::UserGrantRoleFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_grant_role(detached)
    }
}

/// The response from a [`user_add`][Client::user_add] operation.
#[derive(Clone, Copy, Debug)]
pub struct UserAddResponse {
    header: ResponseHeader,
}

impl UserAddResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The response from a [`user_grant_role`][Client::user_grant_role] operation.
#[derive(Clone, Copy, Debug)]
pub struct UserGrantRoleResponse {
    header: ResponseHeader,
}

impl UserGrantRoleResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// An enumeration of the [`kind`][UserError::kind]s of errors that can occur from user management
/// operations ([`user_add`][Client::user_add], [`user_grant_role`][Client::user_grant_role]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserErrorKind {
    /// The user already exists.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "already exists".
    UserAlreadyExists,
    /// The user was not found.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "user name not found".
    UserNotFound,
    /// The role was not found (when granting a role to a user).
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
    /// An error from a user management operation ([`user_add`][Client::user_add],
    /// [`user_grant_role`][Client::user_grant_role]).
    pub struct UserError(UserErrorKind);
}

impl UserError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("already exists") {
                    UserErrorKind::UserAlreadyExists
                } else if msg.contains("user name not found") {
                    UserErrorKind::UserNotFound
                } else if msg.contains("role name not found") {
                    UserErrorKind::RoleNotFound
                } else if msg.contains("not enabled") {
                    UserErrorKind::AuthNotEnabled
                } else {
                    UserErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => UserErrorKind::Authentication,
            tonic::Code::PermissionDenied => UserErrorKind::Authentication,
            tonic::Code::InvalidArgument => UserErrorKind::Authentication,
            tonic::Code::ResourceExhausted => UserErrorKind::Exhausted,
            tonic::Code::Unavailable => UserErrorKind::Unavailable,
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => UserErrorKind::Timeout,
            _ => UserErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<UserAddFuture>();
    }
};
