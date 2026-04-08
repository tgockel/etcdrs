use std::future::Future;

use crate::client::{Put, PutError, PutResponse};

/// Driver for [`put`][crate::Client::put] operations.
pub trait PutDriver {
    /// The future returned by [`execute_put`][Self::execute_put].
    type PutFuture<R>: Future<Output = Result<PutResponse<R>, PutError>> + Send;

    /// Execute a put operation.
    fn execute_put<R>(self, put: Put<(), R>) -> Self::PutFuture<R>;
}
