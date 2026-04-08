use std::future::Future;

use crate::client::{Get, GetError, GetResponse};

/// Driver for [`get`][crate::Client::get] operations.
pub trait GetDriver {
    /// The future returned by [`execute_get`][Self::execute_get].
    type GetFuture: Future<Output = Result<GetResponse, GetError>> + Send;

    /// Execute a get operation.
    fn execute_get(self, get: Get<()>) -> Self::GetFuture;
}
