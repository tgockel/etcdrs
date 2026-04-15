# Lease Pooling

A `LeasePool` manages etcd leases on behalf of the caller, automatically keeping them alive and
grouping leases with the same TTL to reduce the number of active leases on the server.

## Usage

```rust,no_run
use std::time::Duration;
use etcdrs_util::lease_pool::LeasePool;

# async {
let client: etcdrs::Client = todo!();
let pool = LeasePool::new(client.clone());

// Get a lease with a 30-second TTL. If one already exists, it is reused.
let lease_id = pool.get_lease(Duration::from_secs(30)).await.expect("get_lease failed");

// Use the lease for ephemeral keys
client.put("my-key")
    .value("my-value")
    .lease(lease_id)
    .await
    .expect("put failed");

// Explicitly grant a lease with a unique ID (not pooled)
let lease_id = pool.grant_lease(Duration::from_secs(60)).await.expect("grant_lease failed");

// Revoke a lease (removes it from the pool and the server)
pool.revoke_lease(lease_id).await.expect("revoke_lease failed");
# };
```

## Public API

### `LeasePool::new(client: etcdrs::Client) -> Self`

Create a new lease pool backed by the given client. This spawns a background task that manages the
keep-alive stream.

### `pub async fn get_lease(&self, ttl: Duration) -> LeaseId`

Get a lease with the requested TTL. The TTL is rounded to the nearest whole second (since etcd
only supports second-granularity TTLs). If a lease with that TTL already exists in the pool, its
`LeaseId` is returned. Otherwise, a new lease is granted from the server and added to the pool.

### `pub async fn grant_lease(&self, ttl: Duration) -> LeaseId`

Grant a new lease from the server, even if one with the same TTL already exists. The lease is
added to the pool and kept alive automatically.

### `pub async fn revoke_lease(&self, lease_id: LeaseId)`

Revoke a lease, removing it from the pool and sending a revoke request to the server. If the
lease was a pooled lease (obtained via `get_lease`), it is removed from the pool and future calls
to `get_lease` with that TTL will create a new lease.

## TTL Grouping

Since etcd TTLs have second granularity, the pool groups leases by their whole-second TTL value.
A call to `get_lease(Duration::from_secs(30))` and `get_lease(Duration::from_millis(30_200))`
will return the same `LeaseId`, since both round to 30 seconds. This reduces the number of active
leases on the server and the number of keep-alive messages needed.

## Keep-Alive Mechanism

The pool runs a background task that maintains a `LeaseKeeper` stream. It periodically sends
keep-alive requests for all active leases and consumes the responses.

### Scheduling

The keep-alive schedule is driven by the server's actual TTL values from `KeepAliveResponse`,
not the originally-requested TTLs. For a lease whose server-reported TTL is `T`, the pool sends
the next keep-alive at roughly `T / 3`. This provides two retry windows before expiration if a
keep-alive fails or is delayed.

### Error Recovery

If the keep-alive stream terminates with an error, the background task:

1. Reconnects by creating a new `LeaseKeeper` from the client.
2. Immediately sends keep-alive requests for all tracked leases to refresh their TTLs.
3. Resumes normal scheduling based on the new server-reported TTLs.

Leases that have expired during the outage (server reports `ttl: None`) are removed from the
pool. Future `get_lease` calls for that TTL will create a new lease.
