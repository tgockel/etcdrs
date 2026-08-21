use std::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;

use crate::{
    Client, ResponseHeader,
    pb::{authpb, etcdserverpb},
    range::{AsRange, TargetRange},
};

/// # Role Management
impl Client {
    /// Add a new role to the cluster.
    pub fn role_add(&self, name: &str) -> RoleAdd<Self> {
        RoleAdd::new(name).with_client(self.clone())
    }

    /// Get a role, including the [permissions][Permission] granted to it.
    pub fn role_get(&self, name: &str) -> RoleGet<Self> {
        RoleGet::new(name).with_client(self.clone())
    }

    /// List the names of all roles.
    pub fn role_list(&self) -> RoleList<Self> {
        RoleList::new().with_client(self.clone())
    }

    /// Delete a role.
    pub fn role_delete(&self, name: &str) -> RoleDelete<Self> {
        RoleDelete::new(name).with_client(self.clone())
    }

    /// Grant a [`Permission`] to a role.
    ///
    /// A permission couples an access type (read, write, or read-write) with a set of keys. The
    /// key set uses the same range syntax as [`list`][Client::list] queries: a single key, a
    /// bounded range, a [`Prefix`][crate::Prefix], or all keys (`..`).
    ///
    /// ```no_run
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// use etcdrs::{Permission, Prefix};
    ///
    /// // Read-write access to every key prefixed with "app/"
    /// client
    ///     .role_grant_permission("app", Permission::read_write(Prefix("app/")))
    ///     .await
    ///     .expect("failed to grant permission");
    ///
    /// // Read access to the single key "config"
    /// client
    ///     .role_grant_permission("app", Permission::read("config"))
    ///     .await
    ///     .expect("failed to grant permission");
    /// # };
    /// ```
    pub fn role_grant_permission(&self, name: &str, permission: Permission) -> RoleGrantPermission<Self> {
        RoleGrantPermission::new(name, permission).with_client(self.clone())
    }

    /// Revoke a previously granted permission from a role by its key set.
    ///
    /// The `range` must address the same key set the permission was
    /// [granted][Client::role_grant_permission] over. Revoking a permission that was not granted
    /// fails with [`RoleErrorKind::PermissionNotGranted`].
    pub fn role_revoke_permission(&self, name: &str, range: impl AsRange) -> RoleRevokePermission<Self> {
        RoleRevokePermission::new(name, range).with_client(self.clone())
    }
}

/// The type of access a [`Permission`] grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PermissionType {
    /// The permission grants read access.
    Read,
    /// The permission grants write access.
    Write,
    /// The permission grants both read and write access.
    ReadWrite,
}

/// A permission grantable to a role, covering a single key, a range, a prefix, or all keys.
///
/// The covered keys are expressed with the same [`AsRange`] expressions used by
/// [`list`][Client::list] and [`delete_range`][Client::delete_range]:
///
/// ```
/// use etcdrs::{Permission, Prefix};
///
/// Permission::read("config");                 // a single key
/// Permission::write("a".."b");                // a range
/// Permission::read_write(Prefix("app/"));     // a prefix
/// Permission::read(..);                       // all keys
/// # ;
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Permission {
    perm_type: PermissionType,
    key: Bytes,
    range_end: Bytes,
}

impl Permission {
    /// Create a permission granting read access over `range`.
    pub fn read(range: impl AsRange) -> Self {
        Self::new(PermissionType::Read, range)
    }

    /// Create a permission granting write access over `range`.
    pub fn write(range: impl AsRange) -> Self {
        Self::new(PermissionType::Write, range)
    }

    /// Create a permission granting read and write access over `range`.
    pub fn read_write(range: impl AsRange) -> Self {
        Self::new(PermissionType::ReadWrite, range)
    }

    fn new(perm_type: PermissionType, range: impl AsRange) -> Self {
        let (key, range_end) = range.as_boundaries();
        Self {
            perm_type,
            key,
            range_end,
        }
    }

    /// The type of access this permission grants.
    pub fn permission_type(&self) -> PermissionType {
        self.perm_type
    }

    /// The keys this permission covers.
    pub fn target_range(&self) -> TargetRange<'_> {
        TargetRange::from_wire(&self.key, &self.range_end)
    }

    pub(crate) fn from_pb(pb: authpb::Permission) -> Self {
        let perm_type = match authpb::permission::Type::try_from(pb.perm_type)
            .expect("etcd should return a known permission type")
        {
            authpb::permission::Type::Read => PermissionType::Read,
            authpb::permission::Type::Write => PermissionType::Write,
            authpb::permission::Type::Readwrite => PermissionType::ReadWrite,
        };
        Self {
            perm_type,
            key: pb.key,
            range_end: pb.range_end,
        }
    }

    pub(crate) fn into_pb(self) -> authpb::Permission {
        let perm_type = match self.perm_type {
            PermissionType::Read => authpb::permission::Type::Read,
            PermissionType::Write => authpb::permission::Type::Write,
            PermissionType::ReadWrite => authpb::permission::Type::Readwrite,
        };
        authpb::Permission {
            perm_type: perm_type as i32,
            key: self.key,
            range_end: self.range_end,
        }
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

/// A [`Client::role_get`] operation.
#[derive(Clone)]
#[must_use = "RoleGet does nothing unless you `await` it"]
pub struct RoleGet<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthRoleGetRequest,
}

impl RoleGet<()> {
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthRoleGetRequest { role: name.into() },
        }
    }
}

impl<C> RoleGet<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleGet<C2> {
        RoleGet {
            client,
            request: self.request,
        }
    }

    /// The role name to look up.
    pub fn name(&self) -> &str {
        &self.request.role
    }

    pub(crate) fn into_parts(self) -> (C, RoleGet<()>) {
        (
            self.client,
            RoleGet {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The response from a [`role_get`][Client::role_get] operation.
#[derive(Clone, Debug)]
pub struct RoleGetResponse {
    header: ResponseHeader,
    permissions: Vec<Permission>,
}

impl RoleGetResponse {
    pub(crate) fn new(header: ResponseHeader, permissions: Vec<Permission>) -> Self {
        Self { header, permissions }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The permissions granted to the role.
    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }

    /// Consume the response and return the permissions granted to the role.
    pub fn into_permissions(self) -> Vec<Permission> {
        self.permissions
    }
}

pub struct RoleGetFuture(Pin<Box<dyn Future<Output = Result<RoleGetResponse, RoleError>> + Send>>);

impl RoleGetFuture {
    pub(crate) fn new(future: impl Future<Output = Result<RoleGetResponse, RoleError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleGetFuture {
    type Output = Result<RoleGetResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleGet<C> {
    type Output = Result<RoleGetResponse, RoleError>;
    type IntoFuture = C::RoleGetFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_get(detached)
    }
}

/// A [`Client::role_list`] operation.
#[derive(Clone)]
#[must_use = "RoleList does nothing unless you `await` it"]
pub struct RoleList<C> {
    client: C,
}

impl RoleList<()> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self { client: () }
    }
}

impl<C> RoleList<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleList<C2> {
        RoleList { client }
    }

    pub(crate) fn into_parts(self) -> (C, RoleList<()>) {
        (self.client, RoleList { client: () })
    }
}

/// The response from a [`role_list`][Client::role_list] operation.
#[derive(Clone, Debug)]
pub struct RoleListResponse {
    header: ResponseHeader,
    roles: Vec<String>,
}

impl RoleListResponse {
    pub(crate) fn new(header: ResponseHeader, roles: Vec<String>) -> Self {
        Self { header, roles }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The names of all roles.
    pub fn roles(&self) -> &[String] {
        &self.roles
    }

    /// Consume the response and return the role names.
    pub fn into_roles(self) -> Vec<String> {
        self.roles
    }
}

pub struct RoleListFuture(Pin<Box<dyn Future<Output = Result<RoleListResponse, RoleError>> + Send>>);

impl RoleListFuture {
    pub(crate) fn new(future: impl Future<Output = Result<RoleListResponse, RoleError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleListFuture {
    type Output = Result<RoleListResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleList<C> {
    type Output = Result<RoleListResponse, RoleError>;
    type IntoFuture = C::RoleListFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_list(detached)
    }
}

/// A [`Client::role_delete`] operation.
#[derive(Clone)]
#[must_use = "RoleDelete does nothing unless you `await` it"]
pub struct RoleDelete<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthRoleDeleteRequest,
}

impl RoleDelete<()> {
    pub fn new(name: &str) -> Self {
        Self {
            client: (),
            request: etcdserverpb::AuthRoleDeleteRequest { role: name.into() },
        }
    }
}

impl<C> RoleDelete<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleDelete<C2> {
        RoleDelete {
            client,
            request: self.request,
        }
    }

    /// The role name to delete.
    pub fn name(&self) -> &str {
        &self.request.role
    }

    pub(crate) fn into_parts(self) -> (C, RoleDelete<()>) {
        (
            self.client,
            RoleDelete {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The response from a [`role_delete`][Client::role_delete] operation.
#[derive(Clone, Copy, Debug)]
pub struct RoleDeleteResponse {
    header: ResponseHeader,
}

impl RoleDeleteResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

pub struct RoleDeleteFuture(Pin<Box<dyn Future<Output = Result<RoleDeleteResponse, RoleError>> + Send>>);

impl RoleDeleteFuture {
    pub(crate) fn new(future: impl Future<Output = Result<RoleDeleteResponse, RoleError>> + Send + 'static) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleDeleteFuture {
    type Output = Result<RoleDeleteResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleDelete<C> {
    type Output = Result<RoleDeleteResponse, RoleError>;
    type IntoFuture = C::RoleDeleteFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_delete(detached)
    }
}

/// A [`Client::role_grant_permission`] operation.
#[derive(Clone)]
#[must_use = "RoleGrantPermission does nothing unless you `await` it"]
pub struct RoleGrantPermission<C> {
    client: C,
    pub(crate) name: String,
    pub(crate) permission: Permission,
}

impl RoleGrantPermission<()> {
    pub fn new(name: &str, permission: Permission) -> Self {
        Self {
            client: (),
            name: name.into(),
            permission,
        }
    }
}

impl<C> RoleGrantPermission<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleGrantPermission<C2> {
        RoleGrantPermission {
            client,
            name: self.name,
            permission: self.permission,
        }
    }

    /// The role the permission will be granted to.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The permission to grant.
    pub fn permission(&self) -> &Permission {
        &self.permission
    }

    pub(crate) fn into_parts(self) -> (C, RoleGrantPermission<()>) {
        (
            self.client,
            RoleGrantPermission {
                client: (),
                name: self.name,
                permission: self.permission,
            },
        )
    }
}

/// The response from a [`role_grant_permission`][Client::role_grant_permission] operation.
#[derive(Clone, Copy, Debug)]
pub struct RoleGrantPermissionResponse {
    header: ResponseHeader,
}

impl RoleGrantPermissionResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

pub struct RoleGrantPermissionFuture(
    Pin<Box<dyn Future<Output = Result<RoleGrantPermissionResponse, RoleError>> + Send>>,
);

impl RoleGrantPermissionFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<RoleGrantPermissionResponse, RoleError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleGrantPermissionFuture {
    type Output = Result<RoleGrantPermissionResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleGrantPermission<C> {
    type Output = Result<RoleGrantPermissionResponse, RoleError>;
    type IntoFuture = C::RoleGrantPermissionFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_grant_permission(detached)
    }
}

/// A [`Client::role_revoke_permission`] operation.
#[derive(Clone)]
#[must_use = "RoleRevokePermission does nothing unless you `await` it"]
pub struct RoleRevokePermission<C> {
    client: C,
    pub(crate) request: etcdserverpb::AuthRoleRevokePermissionRequest,
}

impl RoleRevokePermission<()> {
    pub fn new(name: &str, range: impl AsRange) -> Self {
        let (key, range_end) = range.as_boundaries();
        Self {
            client: (),
            request: etcdserverpb::AuthRoleRevokePermissionRequest {
                role: name.into(),
                key,
                range_end,
            },
        }
    }
}

impl<C> RoleRevokePermission<C> {
    pub fn with_client<C2>(self, client: C2) -> RoleRevokePermission<C2> {
        RoleRevokePermission {
            client,
            request: self.request,
        }
    }

    /// The role the permission will be revoked from.
    pub fn name(&self) -> &str {
        &self.request.role
    }

    /// The keys of the permission to revoke.
    pub fn target_range(&self) -> TargetRange<'_> {
        TargetRange::from_wire(&self.request.key, &self.request.range_end)
    }

    pub(crate) fn into_parts(self) -> (C, RoleRevokePermission<()>) {
        (
            self.client,
            RoleRevokePermission {
                client: (),
                request: self.request,
            },
        )
    }
}

/// The response from a [`role_revoke_permission`][Client::role_revoke_permission] operation.
#[derive(Clone, Copy, Debug)]
pub struct RoleRevokePermissionResponse {
    header: ResponseHeader,
}

impl RoleRevokePermissionResponse {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        Self { header }
    }

    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

pub struct RoleRevokePermissionFuture(
    Pin<Box<dyn Future<Output = Result<RoleRevokePermissionResponse, RoleError>> + Send>>,
);

impl RoleRevokePermissionFuture {
    pub(crate) fn new(
        future: impl Future<Output = Result<RoleRevokePermissionResponse, RoleError>> + Send + 'static,
    ) -> Self {
        Self(Box::pin(future))
    }
}

impl Future for RoleRevokePermissionFuture {
    type Output = Result<RoleRevokePermissionResponse, RoleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl<C: crate::driver::AuthDriver> IntoFuture for RoleRevokePermission<C> {
    type Output = Result<RoleRevokePermissionResponse, RoleError>;
    type IntoFuture = C::RoleRevokePermissionFuture;

    fn into_future(self) -> Self::IntoFuture {
        let (client, detached) = self.into_parts();
        client.execute_role_revoke_permission(detached)
    }
}

/// An enumeration of the [`kind`][RoleError::kind]s of errors that can occur from role management
/// operations ([`role_add`][Client::role_add], [`role_get`][Client::role_get],
/// [`role_list`][Client::role_list], [`role_delete`][Client::role_delete],
/// [`role_grant_permission`][Client::role_grant_permission],
/// [`role_revoke_permission`][Client::role_revoke_permission]).
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
    /// The permission is not granted to the role (when revoking a permission).
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "permission is not granted to
    /// the role".
    PermissionNotGranted,
    /// The operation is not permitted on this role while authentication is enabled.
    ///
    /// Deleting the root role while authentication is enabled is rejected. This comes from the
    /// gRPC API as `INVALID_ARGUMENT` with "invalid auth management".
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
    /// An error from a role management operation ([`role_add`][Client::role_add],
    /// [`role_get`][Client::role_get], [`role_list`][Client::role_list],
    /// [`role_delete`][Client::role_delete],
    /// [`role_grant_permission`][Client::role_grant_permission],
    /// [`role_revoke_permission`][Client::role_revoke_permission]).
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
                } else if msg.contains("not granted") {
                    RoleErrorKind::PermissionNotGranted
                } else if msg.contains("not enabled") {
                    RoleErrorKind::AuthNotEnabled
                } else {
                    RoleErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => RoleErrorKind::Authentication,
            tonic::Code::PermissionDenied => RoleErrorKind::Authentication,
            tonic::Code::InvalidArgument => {
                if status.message().contains("invalid auth management") {
                    RoleErrorKind::InvalidAuthManagement
                } else {
                    RoleErrorKind::Authentication
                }
            }
            tonic::Code::ResourceExhausted => RoleErrorKind::Exhausted,
            tonic::Code::Unavailable => RoleErrorKind::Unavailable,
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => RoleErrorKind::Timeout,
            _ => RoleErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

const _: () = {
    fn _assert_send<T: Send>() {}
    fn _check() {
        _assert_send::<RoleAddFuture>();
        _assert_send::<RoleGetFuture>();
        _assert_send::<RoleListFuture>();
        _assert_send::<RoleDeleteFuture>();
        _assert_send::<RoleGrantPermissionFuture>();
        _assert_send::<RoleRevokePermissionFuture>();
    }
};
