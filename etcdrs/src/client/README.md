Core client operations.

# Client API

The client API consists of the main [`Client`] structure, associated operation types ([`Get`],
[`Put`], [`List`], [`Delete`], [`Watch`][WatchBuilder], [`Transaction`]), and the
[driver traits][crate::driver] that execute them.

## Operations

For every operation, there is an associated structure with a fluent API. These are generic over a
client type `C`, which starts as `()` and can be set by a `with_client(...)` method. By convention,
clients call `with_client` for you on their convenience methods (e.g.: [`Client::get`] returns a
[`Get<Client>`][Get]). If the `C` type knows how to drive the operation, it can be `await`ed on to
execute.

Operation types are self-contained entities. [Transactions][Client::transaction] utilize this by
allowing operation structures to be passed to its builder functions.

### Transactions

A [`Transaction`] groups multiple operations into an atomic unit with conditional execution. Checks
are added with [`when`][Transaction::when] using conditions like [`TransactionCheck::present`] or
[`TransactionCheck::modified_revision`]. Operations for the success and failure branches are added
with [`and_then`][Transaction::and_then] and [`or_else`][Transaction::or_else].

Operation structs convert into [`TransactionOp`] via [`Into`], so the same builders used for
standalone operations work inside transactions:

```rust,no_run
# async {
# let client: etcdrs::Client = todo!();
use etcdrs::client::{TransactionCheck, TransactionCheckOp};

let response = client.transaction()
    .when([TransactionCheck::present("my-key")])
    .and_then([
        client.put("my-key").value("new-value").into(),
    ])
    .or_else([
        client.put("my-key").value("default-value").into(),
    ])
    .commit()
    .await
    .expect("transaction failed");

if response.succeeded() {
    println!("key existed, updated");
}
# };
```

### Watch

[`WatchBuilder`] sets up real-time monitoring of keys, prefixes, or ranges. Calling
[`start`][WatchBuilder::start] returns a [`Watcher`], which is a `Stream` of
[`WatchEvent`]s. Events can be filtered with [`puts`][Watch::puts] and [`deletes`][Watch::deletes],
and [`get_previous`][Watch::get_previous] includes the record from before each mutation.

A [`Watcher`] can be split into a [`WatchSender`] and [`WatchStream`] for independent control and
consumption, and new watches can be added or cancelled on an active watcher.

### Leases

[`Client::grant_lease`] returns a [`GrantLease`] builder for creating leases with a requested TTL.
Leases are revoked with [`Client::revoke_lease`]. [`Client::lease_time_to_live`] queries the
remaining and originally-granted TTL of a lease (with [`with_keys`][LeaseTimeToLive::with_keys],
also its attached keys), and [`Client::leases`] lists all active lease IDs. To keep leases alive,
[`Client::lease_keeper`] opens a bidirectional stream that can be split into a [`KeepAliveSender`]
and [`KeepAliveStream`] for concurrent keep-alive requests and response consumption.

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
the standard [`into_stream`][ListView::into_stream] operation, but does allow it to be `.await`ed.

[etcd-grpc-put]:   https://etcd.io/docs/v3.6/learning/api/#put
[etcd-grpc-range]: https://etcd.io/docs/v3.6/learning/api/#range
