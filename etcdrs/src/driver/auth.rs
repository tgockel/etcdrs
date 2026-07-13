use std::future::Future;

use crate::client::{
    AuthDisable, AuthDisableResponse, AuthEnable, AuthEnableResponse, AuthError, AuthStatus, AuthStatusResponse,
    Authenticate, AuthenticateResponse, RoleAdd, RoleAddResponse, RoleDelete, RoleDeleteResponse, RoleError, RoleGet,
    RoleGetResponse, RoleGrantPermission, RoleGrantPermissionResponse, RoleList, RoleListResponse,
    RoleRevokePermission, RoleRevokePermissionResponse, UserAdd, UserAddResponse, UserChangePassword,
    UserChangePasswordResponse, UserDelete, UserDeleteResponse, UserError, UserGet, UserGetResponse, UserGrantRole,
    UserGrantRoleResponse, UserList, UserListResponse, UserRevokeRole, UserRevokeRoleResponse,
};

/// Driver for authentication, user, and role operations.
pub trait AuthDriver {
    type AuthEnableFuture: Future<Output = Result<AuthEnableResponse, AuthError>> + Send;
    type AuthDisableFuture: Future<Output = Result<AuthDisableResponse, AuthError>> + Send;
    type AuthStatusFuture: Future<Output = Result<AuthStatusResponse, AuthError>> + Send;
    type AuthenticateFuture: Future<Output = Result<AuthenticateResponse, AuthError>> + Send;
    type UserAddFuture: Future<Output = Result<UserAddResponse, UserError>> + Send;
    type UserGetFuture: Future<Output = Result<UserGetResponse, UserError>> + Send;
    type UserListFuture: Future<Output = Result<UserListResponse, UserError>> + Send;
    type UserDeleteFuture: Future<Output = Result<UserDeleteResponse, UserError>> + Send;
    type UserChangePasswordFuture: Future<Output = Result<UserChangePasswordResponse, UserError>> + Send;
    type UserGrantRoleFuture: Future<Output = Result<UserGrantRoleResponse, UserError>> + Send;
    type UserRevokeRoleFuture: Future<Output = Result<UserRevokeRoleResponse, UserError>> + Send;
    type RoleAddFuture: Future<Output = Result<RoleAddResponse, RoleError>> + Send;
    type RoleGetFuture: Future<Output = Result<RoleGetResponse, RoleError>> + Send;
    type RoleListFuture: Future<Output = Result<RoleListResponse, RoleError>> + Send;
    type RoleDeleteFuture: Future<Output = Result<RoleDeleteResponse, RoleError>> + Send;
    type RoleGrantPermissionFuture: Future<Output = Result<RoleGrantPermissionResponse, RoleError>> + Send;
    type RoleRevokePermissionFuture: Future<Output = Result<RoleRevokePermissionResponse, RoleError>> + Send;

    fn execute_auth_enable(self, op: AuthEnable<()>) -> Self::AuthEnableFuture;
    fn execute_auth_disable(self, op: AuthDisable<()>) -> Self::AuthDisableFuture;
    fn execute_auth_status(self, op: AuthStatus<()>) -> Self::AuthStatusFuture;
    fn execute_authenticate(self, op: Authenticate<()>) -> Self::AuthenticateFuture;
    fn execute_user_add(self, op: UserAdd<()>) -> Self::UserAddFuture;
    fn execute_user_get(self, op: UserGet<()>) -> Self::UserGetFuture;
    fn execute_user_list(self, op: UserList<()>) -> Self::UserListFuture;
    fn execute_user_delete(self, op: UserDelete<()>) -> Self::UserDeleteFuture;
    fn execute_user_change_password(self, op: UserChangePassword<()>) -> Self::UserChangePasswordFuture;
    fn execute_user_grant_role(self, op: UserGrantRole<()>) -> Self::UserGrantRoleFuture;
    fn execute_user_revoke_role(self, op: UserRevokeRole<()>) -> Self::UserRevokeRoleFuture;
    fn execute_role_add(self, op: RoleAdd<()>) -> Self::RoleAddFuture;
    fn execute_role_get(self, op: RoleGet<()>) -> Self::RoleGetFuture;
    fn execute_role_list(self, op: RoleList<()>) -> Self::RoleListFuture;
    fn execute_role_delete(self, op: RoleDelete<()>) -> Self::RoleDeleteFuture;
    fn execute_role_grant_permission(self, op: RoleGrantPermission<()>) -> Self::RoleGrantPermissionFuture;
    fn execute_role_revoke_permission(self, op: RoleRevokePermission<()>) -> Self::RoleRevokePermissionFuture;
}
