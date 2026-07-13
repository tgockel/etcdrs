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

    /// Get detailed information about a user.
    ///
    /// ```no_run
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let response = client.user_get("admin").await.unwrap();
    /// println!("roles: {:?}", response.roles());
    /// # };
    /// ```
    pub fn user_get(&self, name: &str) -> UserGet<Self> {
        UserGet::new(name).with_client(self.clone())
    }

    /// List the names of all users.
    pub fn user_list(&self) -> UserList<Self> {
        UserList::new().with_client(self.clone())
    }

    /// Delete a user.
    pub fn user_delete(&self, name: &str) -> UserDelete<Self> {
        UserDelete::new(name).with_client(self.clone())
    }

    /// Change a user's password.
    pub fn user_change_password(&self, name: &str, new_password: impl Into<String>) -> UserChangePassword<Self> {
        UserChangePassword::new(name, new_password).with_client(self.clone())
    }

    /// Revoke a role from a user.
    pub fn user_revoke_role(&self, user: &str, role: &str) -> UserRevokeRole<Self> {
        UserRevokeRole::new(user, role).with_client(self.clone())
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

/// A [`Client::user_get`] operation.
#[derive(Clone)]
#[must_use = "UserGet does nothing unless you `await` it"]
pub struct UserGet<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserGetRequest,
}

impl UserGet<()> {
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserGetRequest { name: name.into() },
        }
    }
}

impl<C> UserGet<C> {
    pub fn with_client<C2>(self, client: C2) -> UserGet<C2> {
        UserGet {
            client,
            request: self.request,
        }
    }

    /// The user name to look up.
    pub fn name(&self) -> &str {
        &self.request.name
    }

    pub(crate) fn into_parts(self) -> (C, UserGet<()>) {
        (
            self.client,
            UserGet {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct UserGetFuture(Pin<Box<dyn Future<Output = Result<UserGetResponse, UserError>> + Send>>);

impl UserGetFuture {
    pub(crate) fn new(future: impl Future<Output = Result<UserGetResponse, UserError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserGetFuture {
    type Output = Result<UserGetResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserGet<C> {
    type Output = Result<UserGetResponse, UserError>;
    type IntoFuture = C::UserGetFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_get(detached)
    }
}

/// A [`Client::user_list`] operation.
#[derive(Clone)]
#[must_use = "UserList does nothing unless you `await` it"]
pub struct UserList<C> {
    client: C,
}

impl UserList<()> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { client: () }
    }
}

impl<C> UserList<C> {
    pub fn with_client<C2>(self, client: C2) -> UserList<C2> {
        UserList { client }
    }

    pub(crate) fn into_parts(self) -> (C, UserList<()>) {
        (self.client, UserList { client: () })
    }
}

pub struct UserListFuture(Pin<Box<dyn Future<Output = Result<UserListResponse, UserError>> + Send>>);

impl UserListFuture {
    pub(crate) fn new(future: impl Future<Output = Result<UserListResponse, UserError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserListFuture {
    type Output = Result<UserListResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserList<C> {
    type Output = Result<UserListResponse, UserError>;
    type IntoFuture = C::UserListFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_list(detached)
    }
}

/// A [`Client::user_delete`] operation.
#[derive(Clone)]
#[must_use = "UserDelete does nothing unless you `await` it"]
pub struct UserDelete<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserDeleteRequest,
}

impl UserDelete<()> {
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserDeleteRequest { name: name.into() },
        }
    }
}

impl<C> UserDelete<C> {
    pub fn with_client<C2>(self, client: C2) -> UserDelete<C2> {
        UserDelete {
            client,
            request: self.request,
        }
    }

    /// The user name to delete.
    pub fn name(&self) -> &str {
        &self.request.name
    }

    pub(crate) fn into_parts(self) -> (C, UserDelete<()>) {
        (
            self.client,
            UserDelete {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct UserDeleteFuture(Pin<Box<dyn Future<Output = Result<UserDeleteResponse, UserError>> + Send>>);

impl UserDeleteFuture {
    pub(crate) fn new(future: impl Future<Output = Result<UserDeleteResponse, UserError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserDeleteFuture {
    type Output = Result<UserDeleteResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserDelete<C> {
    type Output = Result<UserDeleteResponse, UserError>;
    type IntoFuture = C::UserDeleteFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_delete(detached)
    }
}

/// A [`Client::user_change_password`] operation.
#[derive(Clone)]
#[must_use = "UserChangePassword does nothing unless you `await` it"]
pub struct UserChangePassword<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserChangePasswordRequest,
}

impl UserChangePassword<()> {
    pub fn new(name: &str, new_password: impl Into<String>) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserChangePasswordRequest {
                name: name.into(),
                password: new_password.into(),
                ..Default::default()
            },
        }
    }
}

impl<C> UserChangePassword<C> {
    pub fn with_client<C2>(self, client: C2) -> UserChangePassword<C2> {
        UserChangePassword {
            client,
            request: self.request,
        }
    }

    /// The user whose password will be changed.
    pub fn name(&self) -> &str {
        &self.request.name
    }

    /// The new password.
    pub fn new_password(&self) -> &str {
        &self.request.password
    }

    pub(crate) fn into_parts(self) -> (C, UserChangePassword<()>) {
        (
            self.client,
            UserChangePassword {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct UserChangePasswordFuture(
    Pin<Box<dyn Future<Output = Result<UserChangePasswordResponse, UserError>> + Send>>,
);

impl UserChangePasswordFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<UserChangePasswordResponse, UserError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserChangePasswordFuture {
    type Output = Result<UserChangePasswordResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserChangePassword<C> {
    type Output = Result<UserChangePasswordResponse, UserError>;
    type IntoFuture = C::UserChangePasswordFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_change_password(detached)
    }
}

/// A [`Client::user_revoke_role`] operation.
#[derive(Clone)]
#[must_use = "UserRevokeRole does nothing unless you `await` it"]
pub struct UserRevokeRole<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthUserRevokeRoleRequest,
}

impl UserRevokeRole<()> {
    pub fn new(user: &str, role: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthUserRevokeRoleRequest {
                name: user.into(),
                role: role.into(),
            },
        }
    }
}

impl<C> UserRevokeRole<C> {
    pub fn with_client<C2>(self, client: C2) -> UserRevokeRole<C2> {
        UserRevokeRole {
            client,
            request: self.request,
        }
    }

    /// The user the role will be revoked from.
    pub fn user(&self) -> &str {
        &self.request.name
    }

    /// The role to revoke.
    pub fn role(&self) -> &str {
        &self.request.role
    }

    pub(crate) fn into_parts(self) -> (C, UserRevokeRole<()>) {
        (
            self.client,
            UserRevokeRole {
                client: (),
                request: self.request,
            },
        )
    }
}

pub struct UserRevokeRoleFuture(Pin<Box<dyn Future<Output = Result<UserRevokeRoleResponse, UserError>> + Send>>);

impl UserRevokeRoleFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<UserRevokeRoleResponse, UserError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for UserRevokeRoleFuture {
    type Output = Result<UserRevokeRoleResponse, UserError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for UserRevokeRole<C> {
    type Output = Result<UserRevokeRoleResponse, UserError>;
    type IntoFuture = C::UserRevokeRoleFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_user_revoke_role(detached)
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

/// The response from a [`user_get`][Client::user_get] operation.
#[derive(Clone, Debug)]
pub struct UserGetResponse {
    header: ResponseHeader,
    roles: Vec<String>,
}

impl UserGetResponse {
    pub(crate) fn new(header: ResponseHeader, roles: Vec<String>) -> Self {
        Self { header, roles }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The roles granted to the user.
    pub fn roles(&self) -> &[String] {
        &self.roles
    }

    /// Consume the response and return the roles granted to the user.
    pub fn into_roles(self) -> Vec<String> {
        self.roles
    }
}

/// The response from a [`user_list`][Client::user_list] operation.
#[derive(Clone, Debug)]
pub struct UserListResponse {
    header: ResponseHeader,
    users: Vec<String>,
}

impl UserListResponse {
    pub(crate) fn new(header: ResponseHeader, users: Vec<String>) -> Self {
        Self { header, users }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The names of all users.
    pub fn users(&self) -> &[String] {
        &self.users
    }

    /// Consume the response and return the user names.
    pub fn into_users(self) -> Vec<String> {
        self.users
    }
}

/// The response from a [`user_delete`][Client::user_delete] operation.
#[derive(Clone, Copy, Debug)]
pub struct UserDeleteResponse {
    header: ResponseHeader,
}

impl UserDeleteResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The response from a [`user_change_password`][Client::user_change_password] operation.
#[derive(Clone, Copy, Debug)]
pub struct UserChangePasswordResponse {
    header: ResponseHeader,
}

impl UserChangePasswordResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The response from a [`user_revoke_role`][Client::user_revoke_role] operation.
#[derive(Clone, Copy, Debug)]
pub struct UserRevokeRoleResponse {
    header: ResponseHeader,
}

impl UserRevokeRoleResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// An enumeration of the [`kind`][UserError::kind]s of errors that can occur from user management
/// operations ([`user_add`][Client::user_add], [`user_get`][Client::user_get],
/// [`user_list`][Client::user_list], [`user_delete`][Client::user_delete],
/// [`user_change_password`][Client::user_change_password],
/// [`user_grant_role`][Client::user_grant_role],
/// [`user_revoke_role`][Client::user_revoke_role]).
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
    /// The role is not granted to the user (when revoking a role).
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "role is not granted to the
    /// user".
    RoleNotGranted,
    /// The operation is not permitted on this user while authentication is enabled.
    ///
    /// Deleting the root user or revoking its root role while authentication is enabled is
    /// rejected. This comes from the gRPC API as `INVALID_ARGUMENT` with "invalid auth
    /// management".
    InvalidAuthManagement,
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
    /// [`user_get`][Client::user_get], [`user_list`][Client::user_list],
    /// [`user_delete`][Client::user_delete],
    /// [`user_change_password`][Client::user_change_password],
    /// [`user_grant_role`][Client::user_grant_role],
    /// [`user_revoke_role`][Client::user_revoke_role]).
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
                } else if msg.contains("not granted") {
                    UserErrorKind::RoleNotGranted
                } else if msg.contains("not enabled") {
                    UserErrorKind::AuthNotEnabled
                } else {
                    UserErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => UserErrorKind::Authentication,
            tonic::Code::PermissionDenied => UserErrorKind::Authentication,
            tonic::Code::InvalidArgument => {
                if status.message().contains("invalid auth management") {
                    UserErrorKind::InvalidAuthManagement
                } else {
                    UserErrorKind::Authentication
                }
            }
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
        _assert_send::<UserGetFuture>();
        _assert_send::<UserListFuture>();
        _assert_send::<UserDeleteFuture>();
        _assert_send::<UserChangePasswordFuture>();
        _assert_send::<UserRevokeRoleFuture>();
    }
};
