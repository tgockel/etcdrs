Driver traits used to execute operations.

# Driver Traits

Drivers are the advanced extension point behind the client operation builders. They separate *what*
operation was built from *how* that operation is executed.

The default [`Client`][crate::Client] implements every driver via gRPC. Custom backends can wrap a
client to add caching, testing, instrumentation, or proxy behavior.

## Available Traits

| Trait | Operation family |
|-------|------------------|
| [`KvDriver`] | Get, put, delete, list, count, and transactions |
| [`LeaseDriver`] | Lease grant, revoke, and keep-alive |
| [`WatchDriver`] | Watch stream creation |
| [`AuthDriver`] | Auth control, user management, and role management |
| [`ClusterDriver`] | Cluster membership |
| [`Driver`] | A type implementing every driver family |

`KvDriver` defines the [`ListView`] associated type, which abstracts the paginated stream returned by
list operations.

## How a driver is called

Operation builders are generic over their backend. `Get<Client>` is the attached form that you
`.await`; `Get<()>` is the detached form handed to a driver. Blanket `IntoFuture` impls split an
attached operation into `(driver, detached_operation)` and call the matching driver method.

Driver methods take `self` by value. Driver implementations are expected to be cheap handles,
usually `Clone`/`Arc` backed, so the handle can move into the returned associated future without
borrowing lifetimes.
