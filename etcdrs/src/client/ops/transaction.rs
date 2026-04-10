use bytes::Bytes;

use crate::{
    Client, LeaseId, Record, ResponseHeader, Revision,
    client::{Delete, Get, GetPreviousValue, List, Put, record_from_pb},
    pb::etcdserverpb,
    record::{AsKey, AsValue},
};

impl Client {
    /// Create a new transaction operation.
    ///
    /// ```no_run
    /// # use etcdrs::{client::*, Revision};
    /// # async {
    /// let client: etcdrs::Client = todo!();
    /// let result = client.transaction()
    ///     // The transaction will only succeed if these conditions are met
    ///     .when([
    ///         // non-exist checks our that put operation created the key
    ///         TransactionCheck::not_present("foo"),
    ///         // see that the "bar" key is the same as we expected
    ///         TransactionCheck::modified_revision(
    ///             "bar",
    ///             TransactionCheckOp::Equal,
    ///             Revision::new(10).unwrap()
    ///         ),
    ///     ])
    ///     // If the checks pass, run these operations
    ///     .and_then([
    ///         // Use the operation builders to define the operations
    ///         Put::new("foo").value("something").into(),
    ///         // You can also call them on the client if you prefer
    ///         client.put("bar").value("something else").get_previous().into(),
    ///     ])
    ///     // If the checks fail, run these operations
    ///     .or_else([
    ///         Get::new("foo").into(),
    ///         // Get a count of all the keys in the database
    ///         List::new(..).count_only().into(),
    ///     ])
    ///     // Commit the transaction
    ///     .commit()
    ///     .await
    ///     .unwrap();
    ///
    /// // An Ok result just means the transaction was attempted, it does not mean it
    /// // succeeded.
    /// if result.succeeded() {
    ///     println!("transaction was successful");
    ///     let TransactionOpResponse::Get(prev_bar) = &result.responses()[1] else {
    ///         unreachable!("success[1] was a Get");
    ///     };
    ///     println!("previous value of bar: {prev_bar:?}");
    /// } else {
    ///     // The checks failed, so you usually want to grab the latest records so
    ///     // you can retry the operation.
    ///     println!("transaction was not successful");
    ///     let TransactionOpResponse::Get(foo) = &result.responses()[0] else {
    ///         unreachable!("failure[0] was a Get");
    ///     };
    ///     println!("did foo exist? {}", foo.is_some());
    ///     let TransactionOpResponse::Count(count) = &result.responses()[1] else {
    ///         unreachable!("failure[1] was a Count");
    ///     };
    ///     println!("count of keys: {count}");
    /// }
    /// # };
    /// ```
    pub fn transaction(&self) -> Transaction<Self> {
        Transaction::default().with_client(self.clone())
    }
}

/// A [`transaction`][Client::transaction] operation.
#[derive(Clone, Debug)]
#[must_use = "transaction operations are not executed without calling `commit`"]
pub struct Transaction<C> {
    client: C,
    request: etcdserverpb::TxnRequest,
    success_kinds: Vec<TransactionOpKind>,
    failure_kinds: Vec<TransactionOpKind>,
}

impl Default for Transaction<()> {
    fn default() -> Self {
        Self {
            client: (),
            request: etcdserverpb::TxnRequest::default(),
            success_kinds: Vec::new(),
            failure_kinds: Vec::new(),
        }
    }
}

impl<C> Transaction<C> {
    /// Attach a `client` to this operation.
    ///
    /// You typically do not have to call this function, as it is called automatically by
    /// [`transaction`][Client::transaction].
    pub fn with_client<C2>(self, client: C2) -> Transaction<C2> {
        Transaction {
            client,
            request: self.request,
            success_kinds: self.success_kinds,
            failure_kinds: self.failure_kinds,
        }
    }

    /// Clear the operations ([`when`][Self::when], [`and_then`][Self::and_then], [`or_else`][Self::or_else]).
    pub fn clear(&mut self) {
        self.request.compare.clear();
        self.request.success.clear();
        self.success_kinds.clear();
        self.request.failure.clear();
        self.failure_kinds.clear();
    }

    /// Get a [`clear`][Self::clear]ed version of this operation.
    pub fn cleared(mut self) -> Self {
        self.clear();
        self
    }

    /// Only succeed if the given `checks` pass.
    pub fn when(mut self, checks: impl IntoIterator<Item = TransactionCheck>) -> Self {
        self.request
            .compare
            .extend(checks.into_iter().map(|check| check.request));
        self
    }

    /// Perform the given `operations` if the checks pass.
    pub fn and_then(mut self, operations: impl IntoIterator<Item = TransactionOp>) -> Self {
        for operation in operations {
            Self::add_op(&mut self.request.success, &mut self.success_kinds, operation);
        }
        self
    }

    /// Conveniently [`and_then`][Self::and_then] with a single operation.
    pub fn and_then_do(self, operation: impl Into<TransactionOp>) -> Self {
        self.and_then(Some(operation.into()))
    }

    /// Perform the given `operations` if the checks fail.
    pub fn or_else(mut self, operations: impl IntoIterator<Item = TransactionOp>) -> Self {
        for operation in operations {
            Self::add_op(&mut self.request.failure, &mut self.failure_kinds, operation);
        }
        self
    }

    /// Conveniently [`or_else`][Self::or_else] with a single operation.
    pub fn or_else_do(self, operation: impl Into<TransactionOp>) -> Self {
        self.or_else(Some(operation.into()))
    }

    fn add_op(op_vec: &mut Vec<etcdserverpb::RequestOp>, kind_vec: &mut Vec<TransactionOpKind>, op: TransactionOp) {
        assert_eq!(op_vec.len(), kind_vec.len());
        op_vec.push(op.request);
        kind_vec.push(op.kind);
    }
}

impl Transaction<Client> {
    /// Attempt to commit the transaction to the database.
    ///
    /// **Remember that an `Ok` result does not mean the transaction [`succeeded`][TransactionResponse::succeeded]**, it
    /// only means the attempt made a round trip to the server.
    pub async fn commit(self) -> Result<TransactionResponse, TransactionError> {
        self.client
            .inner
            .wrap_unary_call(
                etcdserverpb::kv_client::KvClient::new,
                async |c, r| c.txn(r).await,
                self.request,
            )
            .await
            .map(|response| TransactionResponse::from_pb(response, &self.success_kinds, &self.failure_kinds))
            .map_err(TransactionError::from_status)
    }
}

#[derive(Clone, Debug)]
#[must_use = "TransactionCheck does nothing on its own; it should be used in a transaction"]
pub struct TransactionCheck {
    request: etcdserverpb::Compare,
}

impl TransactionCheck {
    fn new(
        key: &[u8],
        operator: TransactionCheckOp,
        target: etcdserverpb::compare::CompareTarget,
        target_union: etcdserverpb::compare::TargetUnion,
    ) -> Self {
        Self {
            request: etcdserverpb::Compare {
                result: operator.as_pb() as _,
                target: target as _,
                key: Bytes::copy_from_slice(key),
                range_end: Default::default(),
                target_union: Some(target_union),
            },
        }
    }

    pub fn modified_revision(key: impl AsKey, operator: TransactionCheckOp, revision: Revision) -> Self {
        Self::new(
            key.as_key(),
            operator,
            etcdserverpb::compare::CompareTarget::Mod,
            etcdserverpb::compare::TargetUnion::ModRevision(revision.get()),
        )
    }

    pub fn create_revision(key: impl AsKey, operator: TransactionCheckOp, revision: Revision) -> Self {
        Self::new(
            key.as_key(),
            operator,
            etcdserverpb::compare::CompareTarget::Create,
            etcdserverpb::compare::TargetUnion::CreateRevision(revision.get()),
        )
    }

    /// Check that the given `key` exists.
    pub fn present(key: impl AsKey) -> Self {
        Self::new(
            key.as_key(),
            TransactionCheckOp::NotEqual,
            etcdserverpb::compare::CompareTarget::Create,
            etcdserverpb::compare::TargetUnion::CreateRevision(0),
        )
    }

    /// Check that the given `key` does not exist.
    pub fn not_present(key: impl AsKey) -> Self {
        Self::new(
            key.as_key(),
            TransactionCheckOp::Equal,
            etcdserverpb::compare::CompareTarget::Create,
            etcdserverpb::compare::TargetUnion::CreateRevision(0),
        )
    }

    pub fn value(key: impl AsKey, operator: TransactionCheckOp, value: impl AsValue) -> Self {
        Self::new(
            key.as_key(),
            operator,
            etcdserverpb::compare::CompareTarget::Value,
            etcdserverpb::compare::TargetUnion::Value(Bytes::copy_from_slice(value.as_value())),
        )
    }

    pub fn lease(key: impl AsKey, operator: TransactionCheckOp, lease: Option<LeaseId>) -> Self {
        Self::new(
            key.as_key(),
            operator,
            etcdserverpb::compare::CompareTarget::Lease,
            etcdserverpb::compare::TargetUnion::Lease(lease.map(|x| x.get()).unwrap_or_default()),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionCheckOp {
    Equal,
    Greater,
    Less,
    NotEqual,
}

impl TransactionCheckOp {
    fn as_pb(&self) -> etcdserverpb::compare::CompareResult {
        match self {
            TransactionCheckOp::Equal => etcdserverpb::compare::CompareResult::Equal,
            TransactionCheckOp::Greater => etcdserverpb::compare::CompareResult::Greater,
            TransactionCheckOp::Less => etcdserverpb::compare::CompareResult::Less,
            TransactionCheckOp::NotEqual => etcdserverpb::compare::CompareResult::NotEqual,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TransactionOp {
    request: etcdserverpb::RequestOp,
    kind: TransactionOpKind,
}

impl<C> From<Put<C, ()>> for TransactionOp {
    fn from(value: Put<C, ()>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestPut(value.request)),
            },
            kind: TransactionOpKind::Put,
        }
    }
}

impl<C> From<Put<C, GetPreviousValue>> for TransactionOp {
    fn from(value: Put<C, GetPreviousValue>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestPut(value.request)),
            },
            kind: TransactionOpKind::PutPrevious,
        }
    }
}

impl<C> From<Delete<C, bool>> for TransactionOp {
    fn from(value: Delete<C, bool>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestDeleteRange(value.request)),
            },
            kind: TransactionOpKind::Delete,
        }
    }
}

impl<C> From<Delete<C, bool, GetPreviousValue>> for TransactionOp {
    fn from(value: Delete<C, bool, GetPreviousValue>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestDeleteRange(value.request)),
            },
            kind: TransactionOpKind::DeletePrevious,
        }
    }
}

impl<C> From<Delete<C, usize>> for TransactionOp {
    fn from(value: Delete<C, usize>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestDeleteRange(value.request)),
            },
            kind: TransactionOpKind::DeleteRange,
        }
    }
}

impl<C> From<Delete<C, usize, GetPreviousValue>> for TransactionOp {
    fn from(value: Delete<C, usize, GetPreviousValue>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestDeleteRange(value.request)),
            },
            kind: TransactionOpKind::DeleteRangePrevious,
        }
    }
}

impl<C> From<Get<C>> for TransactionOp {
    fn from(value: Get<C>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestRange(value.request)),
            },
            kind: TransactionOpKind::Get,
        }
    }
}

impl<C> From<List<C, usize>> for TransactionOp {
    fn from(value: List<C, usize>) -> Self {
        Self {
            request: etcdserverpb::RequestOp {
                request: Some(etcdserverpb::request_op::Request::RequestRange(value.request)),
            },
            kind: TransactionOpKind::Count,
        }
    }
}

#[derive(Clone, Debug)]
#[must_use = "TransactionResponse should be checked for success"]
pub struct TransactionResponse {
    header: ResponseHeader,
    succeeded: bool,
    responses: Vec<TransactionOpResponse>,
}

impl TransactionResponse {
    /// The response header containing cluster metadata and the store revision.
    pub fn header(&self) -> &ResponseHeader {
        &self.header
    }

    /// Key-value store revision when the transaction was applied.
    ///
    /// This is a convenience for `self.header().revision()`.
    pub fn revision(&self) -> Revision {
        self.header.revision()
    }

    pub fn succeeded(&self) -> bool {
        self.succeeded
    }

    /// Look at the responses from the transaction.
    ///
    /// If the transaction [`succeeded`][Self::succeeded], the responses will from the operations defined in the
    /// [`and_then`][Transaction::and_then] segment; otherwise they will be from [`or_else`][Transaction::or_else].
    pub fn responses(&self) -> &[TransactionOpResponse] {
        &self.responses
    }

    /// Take the [`responses`][Self::responses] from the transaction.
    pub fn take_responses(self) -> Vec<TransactionOpResponse> {
        self.responses
    }

    fn from_pb(
        response: etcdserverpb::TxnResponse,
        success_kinds: &[TransactionOpKind],
        failure_kinds: &[TransactionOpKind],
    ) -> Self {
        let header = ResponseHeader::from_pb(response.header.expect("TxnResponse should have a valid header"));

        Self {
            header,
            succeeded: response.succeeded,
            responses: response
                .responses
                .into_iter()
                .zip(if response.succeeded {
                    success_kinds
                } else {
                    failure_kinds
                })
                .filter_map(|(response, &kind)| TransactionOpResponse::from_pb(response, kind))
                .collect(),
        }
    }
}

/// The result of a [`transaction`][Client::transaction] operation.
#[derive(Clone, Debug)]
pub enum TransactionOpResponse {
    /// The result of a [`Get`]. The record is returned if it exists or `None` if it does not.
    Get(Option<Record>),
    /// The result of a [`List`] where [`count_only`][List::count_only] was called.
    Count(usize),
    /// The result of a [`Put`].
    Put,
    /// The result of a [`Put`] where [`get_previous`][Put::get_previous] was called. The previous value is returned if
    /// there was one or `None` if the record did not exist.
    PutPrevious(Option<Record>),
    /// The result of a [`Delete`]. The boolean is `true` if the key was deleted or `false` if the key was not found.
    Delete(bool),
    /// The result of a [`Delete`] where [`get_previous`][Delete::get_previous] was called. The previous value is
    /// returned if there was one or `None` if the key did not exist.
    DeletePrevious(Option<Record>),
    /// The result of a [`Client::delete_range`] or [`Client::delete_prefix`]. The value is the number of keys that were
    /// deleted.
    DeleteRange(usize),
    /// The result of a [`Client::delete_range`] or [`Client::delete_prefix`] where
    /// [`get_previous`][Delete::get_previous] was called. The previous key-values are returned.
    DeleteRangePrevious(Vec<Record>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionOpKind {
    Get,
    Count,
    Put,
    PutPrevious,
    Delete,
    DeletePrevious,
    DeleteRange,
    DeleteRangePrevious,
}

impl TransactionOpResponse {
    fn from_pb(response: etcdserverpb::ResponseOp, expected_kind: TransactionOpKind) -> Option<Self> {
        Some(match response.response? {
            etcdserverpb::response_op::Response::ResponsePut(response) => {
                match expected_kind {
                    TransactionOpKind::Put => Self::Put,
                    TransactionOpKind::PutPrevious => Self::PutPrevious(response.prev_kv.map(record_from_pb)),
                    // TODO: Log?
                    _ => Self::Put,
                }
            }
            etcdserverpb::response_op::Response::ResponseRange(response) => {
                match expected_kind {
                    TransactionOpKind::Get => {
                        let record = response.kvs.into_iter().next().map(record_from_pb);
                        Self::Get(record)
                    }
                    TransactionOpKind::Count => Self::Count(response.count as usize),
                    // TODO: Log?
                    _ => return None,
                }
            }
            etcdserverpb::response_op::Response::ResponseDeleteRange(response) => {
                match expected_kind {
                    TransactionOpKind::Delete => Self::Delete(response.deleted > 0),
                    TransactionOpKind::DeletePrevious => {
                        Self::DeletePrevious(response.prev_kvs.into_iter().next().map(record_from_pb))
                    }
                    TransactionOpKind::DeleteRange => Self::DeleteRange(response.deleted as usize),
                    TransactionOpKind::DeleteRangePrevious => {
                        Self::DeleteRangePrevious(response.prev_kvs.into_iter().map(record_from_pb).collect())
                    }
                    // TODO: Log?
                    _ => return None,
                }
            }
            // Not possible to write.
            etcdserverpb::response_op::Response::ResponseTxn(_response) => return None,
        })
    }
}

/// An enumeration of the [`kind`][TransactionError::kind]s of errors that can occur from a
/// [`transaction`][Client::transaction] commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionErrorKind {
    /// The request structure is invalid.
    ///
    /// This comes from the gRPC API as `INVALID_ARGUMENT`. Common causes include too many operations, duplicate keys
    /// in the same branch, or invalid sub-operation arguments.
    InvalidArgument,
    /// A sub-operation requested a revision older than the server has.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`.
    CompactedRevision,
    /// A sub-operation requested a revision newer than the server has.
    ///
    /// This comes from the gRPC API as `OUT_OF_RANGE`.
    FutureRevision,
    /// A sub-put references a lease that does not exist.
    ///
    /// This comes from the gRPC API as `NOT_FOUND`.
    LeaseNotFound,
    /// There is an authentication or authorization error.
    ///
    /// This comes from the gRPC API as `UNAUTHENTICATED` or `PERMISSION_DENIED`.
    Authentication,
    /// The server or transport is resource-exhausted.
    ///
    /// This comes from the gRPC API as `RESOURCE_EXHAUSTED`.
    Exhausted,
    /// The server has lost data.
    ///
    /// This comes from the gRPC API as `DATA_LOSS`.
    DataLoss,
    /// The server is not ready to serve that request.
    ///
    /// This comes from the gRPC API as `UNAVAILABLE`.
    Unavailable,
    /// The request timed out.
    ///
    /// This comes from the gRPC API as `CANCELLED` or `DEADLINE_EXCEEDED`. We do not distinguish between the two, as
    /// the source of the timeout is usually not important.
    Timeout,
    /// An error that is not covered by any other error kind.
    ///
    /// All uncovered gRPC errors are mapped to this kind of error. They should not happen unless the etcd server has
    /// changed its error codes.
    Unknown,
}

define_op_error! {
    /// An error from a [`transaction`][Client::transaction] commit.
    pub struct TransactionError(TransactionErrorKind);
}

impl TransactionError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        let kind = match status.code() {
            tonic::Code::InvalidArgument => TransactionErrorKind::InvalidArgument,
            tonic::Code::NotFound => TransactionErrorKind::LeaseNotFound,
            tonic::Code::Unauthenticated => TransactionErrorKind::Authentication,
            tonic::Code::PermissionDenied => TransactionErrorKind::Authentication,
            tonic::Code::OutOfRange => {
                if status.message().contains("compacted") {
                    TransactionErrorKind::CompactedRevision
                } else if status.message().contains("future") {
                    TransactionErrorKind::FutureRevision
                } else {
                    TransactionErrorKind::Unknown
                }
            }
            tonic::Code::ResourceExhausted => TransactionErrorKind::Exhausted,
            tonic::Code::DataLoss => TransactionErrorKind::DataLoss,
            tonic::Code::Unavailable => TransactionErrorKind::Unavailable,
            // Don't care who timed us out
            tonic::Code::Cancelled | tonic::Code::DeadlineExceeded => TransactionErrorKind::Timeout,
            _ => TransactionErrorKind::Unknown,
        };
        Self::new(kind, "", Some(status))
    }
}
