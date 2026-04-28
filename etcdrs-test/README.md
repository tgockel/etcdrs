Integration test infrastructure for the `etcdrs` workspace.

This crate provides utilities for spawning real etcd server instances and clusters in tests,
along with [`rstest`](https://docs.rs/rstest) fixtures for common configurations.

# Test Fixtures

With the `rstest` feature enabled, this crate provides two fixtures:

- `etcd_server` -- a single-node [`EtcdServer`] with a temporary data directory and random ports.
- `etcd_cluster` -- a multi-node [`EtcdCluster`] (3 peers by default) for testing replication and
  failover.

```rust,ignore
use etcdrs_test::{etcd_server, EtcdServer};
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn my_test(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    // ...
}
```

# Manual Configuration

For more control, build an [`EtcdServerConfig`] or [`EtcdClusterConfig`] directly:

- [`EtcdServerConfig::new_single_temporary`] creates a single-node config with random ports and a
  temporary data directory.
- [`EtcdClusterConfig::with_generated_peers`] creates a multi-node cluster config with the
  specified number of peers.

Call [`.start()`][EtcdServerConfig::start] to launch the server or cluster process.

# etcd Binary Resolution

The etcd binary is resolved in the following order:

1. The `ETCD` environment variable, if set and non-empty.
2. A vendored binary from [`etcd-bin-vendored`](https://docs.rs/etcd-bin-vendored) (covers most
   platforms with no manual install).
3. `etcd` on the system `PATH`.

In most cases, the vendored binary handles resolution automatically and no configuration is needed.

# Environment Variables

- **`ETCD`** -- path to the etcd binary (see [etcd Binary Resolution](#etcd-binary-resolution)).
- **`ETCDRS_KEEP_TEST_DIR`** -- set to `1` or `true` to prevent automatic cleanup of etcd data
  directories after a test passes. By default, data directories under `/tmp` are removed when a
  test completes successfully; they are always kept when a test fails so you can inspect the state.
  Any other value emits a warning and is treated as `false`.

# Feature Flags

- **`rstest`** -- enables the `etcd_server` and `etcd_cluster` fixtures.
