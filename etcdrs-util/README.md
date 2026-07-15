# etcdrs-util

Higher-level utilities built on [`etcdrs`](https://docs.rs/etcdrs).

## Key-Value Caching

The [`cache::CacheClient`] mirrors the key-value operations of a [`Client`][etcdrs::Client],
serving reads of configured key ranges from a local in-memory store kept coherent by a watch.
Reads the cache cannot prove fresh pass through to the server, preserving read-your-writes
semantics without ever blocking:

```rust,no_run
# async {
use etcdrs::Prefix;
use etcdrs_util::cache::CacheClient;

let client: etcdrs::Client = todo!();
let cache = CacheClient::builder(client).cache(Prefix("config/")).build();

// Served locally once warm; passes through (and re-warms) when freshness can't be proven.
let response = cache.get("config/max-connections").await.expect("get failed");
# };
```

Requires the **`cache`** feature (enabled by default).

## Lease Pooling

The [`lease_pool::LeasePool`] manages etcd leases automatically, grouping leases by TTL and keeping
them alive in a background task. Instead of manually granting, tracking, and renewing leases, a pool
handles all of this with a single call:

```rust,no_run
# async {
use std::time::Duration;
use etcdrs_util::lease_pool::LeasePool;

let client: etcdrs::Client = todo!();
let pool = LeasePool::new(client.clone());

let lease_id = pool.get_lease(Duration::from_secs(30)).await.expect("get_lease failed");
client.put("ephemeral-key").value("value").lease(lease_id).await.expect("put failed");
# };
```

Requires the **`lease-pool`** feature (enabled by default).

## Feature Flags

- **`cache`** (default) -- enables the [`cache`] module.
- **`lease-pool`** (default) -- enables the [`lease_pool`] module.
