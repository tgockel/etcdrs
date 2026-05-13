# etcdrs

An async [etcd](https://etcd.io/) client library for Rust, built on [tonic](https://docs.rs/tonic)
and [tokio](https://docs.rs/tokio).

## Overview

etcdrs provides a Rust-native interface to the etcd v3 gRPC API. Every operation is modeled as a
first-class struct with a fluent builder API: the same `Get`, `Put`, `Delete`, and `List` objects
you use for standalone calls can be composed into atomic transactions. Pluggable driver traits
decouple operations from the transport, and the client is cheaply cloneable across tasks.

## Crates

### [`etcdrs`](etcdrs/) -- Core Client

The main client library. Provides `Client`, all operation builders (`Get`, `Put`, `Delete`,
`List`, `Watch`, `Transaction`), lease management, and authentication. Supports type-safe response
variants via phantom types and custom backends via service-boundary driver traits.

### [`etcdrs-util`](etcdrs-util/) -- Utilities

Higher-level abstractions built on `etcdrs`. Currently provides `LeasePool`, which manages etcd
leases with automatic keep-alive and TTL-based grouping to reduce the number of active leases on
the server. Feature-gated behind `lease-pool` (enabled by default).

### [`etcdrs-test`](etcdrs-test/) -- Test Infrastructure

Utilities for spawning real etcd server instances and clusters in integration tests. Provides
`rstest` fixtures for single-node and multi-node configurations, backed by a vendored etcd binary
via `etcd-bin-vendored`.

## Building

```sh
cargo build --workspace
```

To re-generate the protobuf bindings from the etcd proto definitions:

```sh
cargo build -p etcdrs --features generate
```
