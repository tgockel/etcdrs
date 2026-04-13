use crate::{Client, ResponseHeader, client::ClientInner, pb::etcdserverpb};

impl Client {
    /// Enable authentication on the cluster.
    ///
    /// A root user must exist before authentication can be enabled. Create one with
    /// [`user_add`][Client::user_add], then assign the `root` role with
    /// [`user_grant_role`][Client::user_grant_role].
    pub async fn auth_enable(&self) -> Result<AuthEnableResponse, AuthError> {
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::auth_client::AuthClient::new,
                async |c, r| c.auth_enable(r).await,
                etcdserverpb::AuthEnableRequest {},
            )
            .await
            .map_err(AuthError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("AuthEnableResponse should have a valid header"));
        Ok(AuthEnableResponse { header })
    }

    /// Disable authentication on the cluster.
    pub async fn auth_disable(&self) -> Result<AuthDisableResponse, AuthError> {
        let resp = self
            .inner
            .wrap_unary_call(
                etcdserverpb::auth_client::AuthClient::new,
                async |c, r| c.auth_disable(r).await,
                etcdserverpb::AuthDisableRequest {},
            )
            .await
            .map_err(AuthError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("AuthDisableResponse should have a valid header"));
        Ok(AuthDisableResponse { header })
    }

    /// Authenticate with the cluster using the credentials provided to
    /// [`ClientBuilder::credentials`][crate::client::ClientBuilder::credentials], returning a
    /// token.
    ///
    /// This method is useful for checking if the credentials you provided are valid. Other methods
    /// can still fail with authentication errors (especially if configuration changes), but usage
    /// of those might be deep in some other code path.
    ///
    /// Returns [`AuthErrorKind::InvalidCredentials`] if no credentials were configured on the
    /// client.
    pub async fn authenticate(&self) -> Result<AuthenticateResponse, AuthError> {
        let Some(auth) = &self.inner.auth else {
            return Err(AuthError::new(
                AuthErrorKind::InvalidCredentials,
                "no credentials configured on client",
                None,
            ));
        };
        let Some(creds) = &auth.credentials else {
            return Err(AuthError::new(
                AuthErrorKind::InvalidCredentials,
                "no credentials configured on client",
                None,
            ));
        };
        let resp = self
            .inner
            .authenticate(&creds.username, &creds.password)
            .await
            .map_err(AuthError::from_status)?;
        let header = ResponseHeader::from_pb(resp.header.expect("AuthenticateResponse should have a valid header"));
        Ok(AuthenticateResponse {
            header,
            token: resp.token,
        })
    }
}

/// The response from an [`auth_enable`][Client::auth_enable] operation.
#[derive(Clone, Copy, Debug)]
pub struct AuthEnableResponse {
    header: ResponseHeader,
}

impl AuthEnableResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The response from an [`auth_disable`][Client::auth_disable] operation.
#[derive(Clone, Copy, Debug)]
pub struct AuthDisableResponse {
    header: ResponseHeader,
}

impl AuthDisableResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }
}

/// The response from an [`authenticate`][Client::authenticate] operation.
#[derive(Clone, Debug)]
pub struct AuthenticateResponse {
    header: ResponseHeader,
    token: String,
}

impl AuthenticateResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// The auth token that can be used for authenticated requests.
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Consume the response and return the auth token.
    pub fn into_token(self) -> String {
        self.token
    }
}

/// An enumeration of the [`kind`][AuthError::kind]s of errors that can occur from auth control
/// operations ([`auth_enable`][Client::auth_enable], [`auth_disable`][Client::auth_disable],
/// [`authenticate`][Client::authenticate]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthErrorKind {
    /// Authentication is already enabled.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "already enabled".
    AlreadyEnabled,
    /// Authentication is not enabled.
    ///
    /// This comes from the gRPC API as `FAILED_PRECONDITION` with "not enabled".
    NotEnabled,
    /// The root user has not been created yet.
    ///
    /// A root user must exist before authentication can be enabled. This comes from the gRPC API as
    /// `FAILED_PRECONDITION` with "root user".
    RootUserRequired,
    /// The provided credentials are invalid.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED`, `PERMISSION_DENIED`, or
    /// `INVALID_ARGUMENT` when the argument describes an authentication error.
    InvalidCredentials,
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
    /// An error from an auth control operation ([`auth_enable`][Client::auth_enable],
    /// [`auth_disable`][Client::auth_disable], [`authenticate`][Client::authenticate]).
    pub struct AuthError(AuthErrorKind);
}

impl AuthError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::FailedPrecondition => {
                let msg = status.message();
                if msg.contains("already enabled") {
                    AuthErrorKind::AlreadyEnabled
                } else if msg.contains("not enabled") {
                    AuthErrorKind::NotEnabled
                } else if msg.contains("root user") {
                    AuthErrorKind::RootUserRequired
                } else {
                    AuthErrorKind::Unknown
                }
            }
            tonic::Code::Unauthenticated => AuthErrorKind::InvalidCredentials,
            tonic::Code::PermissionDenied => AuthErrorKind::InvalidCredentials,
            tonic::Code::InvalidArgument => AuthErrorKind::InvalidCredentials,
            tonic::Code::ResourceExhausted => AuthErrorKind::Exhausted,
            tonic::Code::Unavailable => AuthErrorKind::Unavailable,
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => AuthErrorKind::Timeout,
            _ => AuthErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}

// === Authentication Handling ===

impl ClientInner {
    /// Execute the `Authenticate` RPC, returning the raw protobuf response.
    ///
    /// This bypasses [`wrap_unary_call`][Self::wrap_unary_call] to avoid circular token refresh. It
    /// is used by both the public [`Client::authenticate`] method and the internal token refresh
    /// path.
    pub(crate) async fn authenticate(
        &self,
        name: &str,
        password: &str,
    ) -> Result<etcdserverpb::AuthenticateResponse, tonic::Status> {
        let mut client = etcdserverpb::auth_client::AuthClient::new(self.channel.clone());
        client
            .authenticate(etcdserverpb::AuthenticateRequest {
                name: name.into(),
                password: password.into(),
            })
            .await
            .map(|r| r.into_inner())
    }

    /// Refresh the cached auth token using stored credentials.
    ///
    /// `stale_generation` is the generation observed when the caller read the token that turned out
    /// to be stale. If another caller has already refreshed (advancing the generation), this call
    /// returns `Ok(())` without making a redundant `Authenticate` RPC.
    pub(crate) async fn refresh_auth_token(&self, stale_generation: u64) -> Result<(), tonic::Status> {
        let auth = self
            .auth
            .as_ref()
            .ok_or_else(|| tonic::Status::unauthenticated("no auth configured"))?;
        let _guard = auth.refresh.lock().await;
        // Another caller may have refreshed while we waited for the lock.
        if auth.token.read().await.0 != stale_generation {
            return Ok(());
        }
        let creds = auth.credentials.as_ref().ok_or_else(|| {
            tonic::Status::unauthenticated("auth token expired and no credentials available for re-authentication")
        })?;
        let resp = self.authenticate(&creds.username, &creds.password).await?;
        let token_value = resp
            .token
            .parse::<tonic::metadata::AsciiMetadataValue>()
            .map_err(|_| tonic::Status::internal("server returned an invalid auth token"))?;
        *auth.token.write().await = (stale_generation + 1, Some(token_value));
        Ok(())
    }
}
