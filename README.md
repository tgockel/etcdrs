etcDRS
======

A drag reduction system for `etcd`.
Really, an `etcd` client implementation written in Rust.
It comes with an in-memory implementation for quickly writing unit tests without spinning up an external process.

F.A.Q.
------

### Why not `etcd-client`?

A `tonic::transport::Channel` is cheap to clone, but an `etcd_client::Client` is not.
