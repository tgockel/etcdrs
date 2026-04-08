use std::future::Future;

use crate::client::{Delete, DeleteError, DeleteResponse};

/// Driver for [`delete`][crate::Client::delete] and [`delete_range`][crate::Client::delete_range] operations.
pub trait DeleteDriver {
    /// The future returned by [`execute_delete`][Self::execute_delete].
    type DeleteFuture<R, P>: Future<Output = Result<DeleteResponse<R, P>, DeleteError>> + Send;

    /// Execute a delete operation.
    ///
    /// Returns `(header, deleted_count, previous_records)`.
    /// The `R` and `P` type parameters are phantom markers and do not affect execution.
    fn execute_delete<R, P>(self, delete: Delete<(), R, P>) -> Self::DeleteFuture<R, P>;
}
