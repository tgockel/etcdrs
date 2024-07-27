etcDRS
======

A drag reduction system for `etcd`.
Really, an `etcd` client implementation written in Rust.
It comes with an in-memory implementation for quickly writing unit tests without spinning up an external process.

F.A.Q.
------

### Why not `etcd-client`?

One of the major things I do not like about the `etcd-client` library is the use of `&mut self` for every API.
This is inherited from the [Tonic Build][tonic-build] generated bindings.
A `tonic::transport::Channel` is cheap to clone, but an `etcd_client::Client` is not.

[tonic-build]: https://docs.rs/tonic-build/latest/tonic_build/
