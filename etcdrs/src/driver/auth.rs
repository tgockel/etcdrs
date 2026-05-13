use std::future::Future;

use crate::client::{
    AuthDisable, AuthDisableResponse, AuthEnable, AuthEnableResponse, AuthError, Authenticate, AuthenticateResponse,
    RoleAdd, RoleAddResponse, RoleError, UserAdd, UserAddResponse, UserError, UserGrantRole, UserGrantRoleResponse,
};

/// Driver for authentication, user, and role operations.
pub trait AuthDriver {
    type AuthEnableFuture: Future<Output = Result<AuthEnableResponse, AuthError>> + Send;
    type AuthDisableFuture: Future<Output = Result<AuthDisableResponse, AuthError>> + Send;
    type AuthenticateFuture: Future<Output = Result<AuthenticateResponse, AuthError>> + Send;
    type UserAddFuture: Future<Output = Result<UserAddResponse, UserError>> + Send;
    type UserGrantRoleFuture: Future<Output = Result<UserGrantRoleResponse, UserError>> + Send;
    type RoleAddFuture: Future<Output = Result<RoleAddResponse, RoleError>> + Send;

    fn execute_auth_enable(self, op: AuthEnable<()>) -> Self::AuthEnableFuture;
    fn execute_auth_disable(self, op: AuthDisable<()>) -> Self::AuthDisableFuture;
    fn execute_authenticate(self, op: Authenticate<()>) -> Self::AuthenticateFuture;
    fn execute_user_add(self, op: UserAdd<()>) -> Self::UserAddFuture;
    fn execute_user_grant_role(self, op: UserGrantRole<()>) -> Self::UserGrantRoleFuture;
    fn execute_role_add(self, op: RoleAdd<()>) -> Self::RoleAddFuture;
}
