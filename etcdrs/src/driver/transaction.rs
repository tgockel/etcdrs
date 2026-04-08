use std::future::Future;

use crate::client::{Transaction, TransactionError, TransactionResponse};

/// Driver for [`transaction`][crate::Client::transaction] operations.
pub trait TransactionDriver {
    /// The future returned by [`execute_transaction`][Self::execute_transaction].
    type CommitFuture: Future<Output = Result<TransactionResponse, TransactionError>> + Send;

    /// Execute a transaction.
    fn execute_transaction(self, txn: Transaction<()>) -> Self::CommitFuture;
}
