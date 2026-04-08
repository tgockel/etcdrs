use std::future::Future;

use futures_core::Stream;

use crate::{
    GetError, ResponseHeader, Revision,
    client::{CountResponse, List},
    record::{KeyWithMetadata, Record},
};

/// Driver for [`list`][crate::Client::list] and [`list_prefix`][crate::Client::list_prefix] operations.
pub trait ListDriver {
    type ListView<R>: ListView<R>;

    type ListViewFuture<R>: Future<Output = Result<Self::ListView<R>, GetError>> + Send;

    /// The future returned by [`execute_count`][Self::execute_count].
    type CountFuture: Future<Output = Result<CountResponse, GetError>> + Send;

    /// Execute a list operation that returns full records (key + value + metadata).
    fn execute_list_records(self, list: List<(), Record>) -> Self::ListViewFuture<Record>;

    /// Execute a list operation that returns keys with metadata only (no values).
    fn execute_list_keys(self, list: List<(), KeyWithMetadata>) -> Self::ListViewFuture<KeyWithMetadata>;

    /// Execute a count-only list operation.
    fn execute_count(self, list: List<(), usize>) -> Self::CountFuture;
}

pub trait ListView<R> {
    type Stream: Stream<Item = Result<R, GetError>> + Send;

    fn header(&self) -> &ResponseHeader;

    fn revision(&self) -> Revision {
        self.header().revision()
    }

    fn into_stream(self) -> Self::Stream;
}
