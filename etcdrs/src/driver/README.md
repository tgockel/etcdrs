Driver traits used to execute operations.

# Driver Traits

The `driver` module defines a trait for each category of etcd operation. Together, these traits
abstract *how* an operation is executed, separating the operation's [`client`][crate::client]
builder API from the backend that carries it out.

The [`Client`][crate::Client] type of this library implements every driver via gRPC. Other types
can implement one or more drivers to participate in the same operation pipeline. A simple use is a
read-through cache that implements [`GetDriver`] and forwards everything else to an inner `Client`.

## Available Traits

| Trait | Method | Operation |
|-------|--------|-----------|
| [`GetDriver`] | `execute_get` | Single-key fetch |
| [`PutDriver`] | `execute_put` | Set a key-value pair |
| [`DeleteDriver`] | `execute_delete` | Delete one key or a range |
| [`ListDriver`] | `execute_list_records`, `execute_list_keys`, `execute_count` | Range queries and scans |
| [`TransactionDriver`] | `execute_transaction` | Atomic multi-operation transactions |
| [`WatchDriver`] | `start_watch` | Real-time key monitoring |
| [`LeaseDriver`] | `execute_grant_lease`, `execute_revoke_lease` | Lease lifecycle management |

[`ListDriver`] also defines the [`ListView`] associated type, which abstracts the paginated stream
returned by list operations.

## How a driver is called

Operation builders are generic over their backend. `Get<Client>` is the *attached* form that you
`.await`; `Get<()>` is the *detached* form handed to a driver. Each operation has a blanket
`impl<C: XxxDriver> IntoFuture for Xxx<C>` (see `client/ops/*.rs`) that splits the attached form
into `(client, detached)` and calls `client.execute_get(detached)` (or the equivalent method for
other drivers). The driver method owns the backend (`self` is taken by value) so it can move the
backend into the returned future.

## Implementing a driver

A minimal read-only cache that implements [`GetDriver`]:

```rust,no_run
use std::future::Future;
use std::pin::Pin;

use etcdrs::client::{Get, GetResponse, GetError};
use etcdrs::driver::GetDriver;

struct MyCacheClient { /* ... */ }

impl GetDriver for MyCacheClient {
    type GetFuture = Pin<Box<dyn Future<Output = Result<GetResponse, GetError>> + Send>>;

    fn execute_get(self, get: Get<()>) -> Self::GetFuture {
        Box::pin(async move {
            // Check local cache, fall back to an inner client, etc.
            todo!()
        })
    }
}
```

Because the returned future captures `self`, the backend type must be `Send + 'static` for
`GetFuture: Send` to hold. If you try to hold an `Rc` or a non-`'static` borrow in the driver, the
bound on `GetFuture` will fail at the trait impl.

With this impl in place, `Get<MyCacheClient>` is `.await`-able (via the blanket `IntoFuture` impl
on `Get<C>`), and `my_cache.get("k").await` works the same way `client.get("k").await` does.
