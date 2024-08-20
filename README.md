etcDRS
======

A drag reduction system for `etcd`.
Really, an `etcd` client implementation written in Rust.

> **NOTE: Status and Quality**
>
> This library is alpha-quality and a work-in-progress.

F.A.Q.
------

### Why not `etcd-client`?

One of the major things I do not like about the `etcd-client` library is the use of `&mut self` for every API.
This is inherited from the [Tonic Build][tonic-build] generated bindings.
A `tonic::transport::Channel` is cheap to clone, but an `etcd_client::Client` is not.

[tonic-build]: https://docs.rs/tonic-build/latest/tonic_build/
