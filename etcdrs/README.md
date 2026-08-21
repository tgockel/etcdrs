# etcdrs

An async Rust client for [etcd](https://etcd.io/), built on
[tonic](https://docs.rs/tonic) and [tokio](https://docs.rs/tokio).

## Design

### Operations as Objects

Every etcd operation ([`Get`][client::Get], [`Put`][client::Put], [`Delete`][client::Delete],
[`List`][client::List], [`Watch`][client::WatchBuilder]) is a standalone struct with a fluent
builder API. Operations can be awaited directly for one-shot use, or composed into atomic
[`Transaction`][client::Transaction]s without changing how they are built:

```rust,no_run
use futures::StreamExt;
# async {
let client = etcdrs::Client::new("http://localhost:2379").unwrap();

// Standalone
client.put("greeting/hello").value("world").await.unwrap();
let record = client.get("greeting/hello").await.unwrap();

// Ranged reads stream; a page limit is what makes later pages fetch as you consume them
let mut entries = client.list_prefix("greeting/").limit(100).await.unwrap().into_stream();
while let Some(entry) = entries.next().await {
    println!("{:?}", entry.unwrap());
}

// The same builders work inside a transaction
client.transaction()
    .when([etcdrs::client::TransactionCheck::present("greeting/hello")])
    .and_then([client.put("greeting/hello").value("updated").into()])
    .commit()
    .await
    .unwrap();
# };
```

A [`List`][client::List] resolves to a [`ListView`][client::ListView], which
[`into_stream`][client::ListView::into_stream] turns into a `Stream`. Without a
[`limit`][client::List::limit] etcd answers the whole range in one response, and the stream simply
walks it; with one, each request returns a page and the stream fetches the next as you consume it,
pinning the revision so a scan stays consistent across them. On a nightly toolchain with
[`nightly-async-iterator`](#feature-flags), `ListView` also implements `IntoAsyncIterator`, so it
can be driven with `for await` instead.

### Type-Safe Responses

Builder methods change phantom type parameters, altering the response type at compile time.
Calling `.get_previous()` on a [`Put`][client::Put] makes `PutResponse::previous()` available;
calling `.keys_only()` on a [`List`][client::List] changes the stream item type to
[`KeyWithMetadata`]; calling `.count_only()` yields a `usize` instead of a stream.

### Cheap Client Cloning

[`Client`] wraps an [`Arc`][std::sync::Arc] internally. Cloning is cheap and all methods take
`&self`, so a single client can be shared across tasks without additional synchronization.

### Pluggable Backends

The [`driver`] module defines service-boundary traits like [`KvDriver`][driver::KvDriver],
[`LeaseDriver`][driver::LeaseDriver], and [`WatchDriver`][driver::WatchDriver] that abstract how
operations are executed. The default [`Client`] implements all drivers via gRPC. Custom types can
wrap a client to build caching layers, mock clients, instrumentation, or proxies.

### Authentication

[`ClientBuilder`][client::ClientBuilder] supports username/password
[`credentials`][client::ClientBuilder::credentials] with automatic re-authentication when the
server returns `UNAUTHENTICATED`. Tokens are cached and refreshed transparently.

## Feature Flags

- **`generate`** -- re-generate protobuf bindings from the etcd proto definitions (requires
  `protoc`).
- **`nightly-async-iterator`** -- enable [`AsyncIterator`][std::async_iter::AsyncIterator]
  implementations for streaming types (requires nightly Rust).
