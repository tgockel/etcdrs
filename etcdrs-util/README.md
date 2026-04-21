# etcdrs-util

Higher-level utilities built on [`etcdrs`](https://docs.rs/etcdrs).

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

- **`lease-pool`** (default) -- enables the [`lease_pool`] module.
