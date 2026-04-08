Core client operations.

# Client API

The client API consists of two major pieces: the main [`Client`] structure, associated operation
types ([`Get`], [`Put`], [`List`], ...), related in-flight [`Future`][std::future::Future] types
([`GetFuture`], [`PutFuture`], [`ListFuture`], ...), and the

## Operations

For every operation, there is an associated structure with a fluent API, which can be built with the
methods. These are generic over a client type `C`, which starts as `()` and can be set by a
`with_client(...)` method. By convention, clients call `with_client` for you on their convenience
methods (e.g.: [`Client::get`] returns a [`Get<Client>`][Get]). If the `C` type knows how to drive
the operation, it can be `await`ed on to execute.

Operation types are self-contained entities. [Transactions][Client::transaction] utilize this by
allowing operation structures to be passed to its builder functions.

### Phantom Return Types

For many operations, the etcd gRPC API presents a single endpoint with a return type that varies by
the request argument. For example, the [etcdserverpb.KV/Put][etcd-grpc-put] operation might return
the previously-stored value if `prev_kv` is `true`. Instead of always providing this value and
returning `None`, the [`response.previous()`][PutResponse::previous] function is only available if
[`get_previous()`][Put::get_previous] was called on the operation builder. It does this by changing
the phantom `R` type; if you see an `R`, this is what it is used for.

Another example of `R` is in the [`List`] operation. It starts as a [`Record`], which means we
should fetch all the records. This client also implements key-scanning and count operations using
the same underlying [etcdserverpb.KV/Range][etcd-grpc-range] API. Calling
[`keys_only`][List::keys_only] changes the `R` to [`KeyWithMetadata`] and does not bother fetching
associated values. Calling [`count_only`][List::count_only] changes `R` to `usize`, which disables
the standard [`into_stream`][List::into_stream] operation, but does allow it to be `.await`ed.

[etcd-grpc-put]:   https://etcd.io/docs/v3.6/learning/api/#put
[etcd-grpc-range]: https://etcd.io/docs/v3.6/learning/api/#range
