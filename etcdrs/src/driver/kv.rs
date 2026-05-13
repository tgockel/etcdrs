use std::future::Future;

use futures_core::Stream;

use crate::{
    GetError, ResponseHeader, Revision,
    client::{
        CountResponse, Delete, DeleteError, DeleteResponse, Get, GetResponse, List, Put, PutError, PutResponse,
        Transaction, TransactionError, TransactionResponse,
    },
    record::{KeyWithMetadata, Record},
};

/// Driver for key-value operations.
pub trait KvDriver {
    /// The future returned by [`execute_get`][Self::execute_get].
    type GetFuture: Future<Output = Result<GetResponse, GetError>> + Send;

    /// The future returned by [`execute_put`][Self::execute_put].
    type PutFuture<R>: Future<Output = Result<PutResponse<R>, PutError>> + Send;

    /// The future returned by [`execute_delete`][Self::execute_delete].
    type DeleteFuture<R, P>: Future<Output = Result<DeleteResponse<R, P>, DeleteError>> + Send;

    /// The view returned by list operations.
    type ListView<R>: ListView<R>;

    /// The future returned by list operations.
    type ListViewFuture<R>: Future<Output = Result<Self::ListView<R>, GetError>> + Send;

    /// The future returned by [`execute_count`][Self::execute_count].
    type CountFuture: Future<Output = Result<CountResponse, GetError>> + Send;

    /// The future returned by [`execute_transaction`][Self::execute_transaction].
    type CommitFuture: Future<Output = Result<TransactionResponse, TransactionError>> + Send;

    /// Execute a get operation.
    fn execute_get(self, get: Get<()>) -> Self::GetFuture;

    /// Execute a put operation.
    fn execute_put<R>(self, put: Put<(), R>) -> Self::PutFuture<R>;

    /// Execute a delete operation.
    fn execute_delete<R, P>(self, delete: Delete<(), R, P>) -> Self::DeleteFuture<R, P>;

    /// Execute a list operation that returns full records (key + value + metadata).
    fn execute_list_records(self, list: List<(), Record>) -> Self::ListViewFuture<Record>;

    /// Execute a list operation that returns keys with metadata only (no values).
    fn execute_list_keys(self, list: List<(), KeyWithMetadata>) -> Self::ListViewFuture<KeyWithMetadata>;

    /// Execute a count-only list operation.
    fn execute_count(self, list: List<(), usize>) -> Self::CountFuture;

    /// Execute a transaction.
    fn execute_transaction(self, txn: Transaction<()>) -> Self::CommitFuture;
}

pub trait ListView<R> {
    type Stream: Stream<Item = Result<R, GetError>> + Send;

    fn header(&self) -> &ResponseHeader;

    fn revision(&self) -> Revision {
        self.header().revision()
    }

    fn into_stream(self) -> Self::Stream;
}
