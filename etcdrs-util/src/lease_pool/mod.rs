#![doc = include_str!("README.md")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use etcdrs::{Client, GrantLeaseError, LeaseId, RevokeLeaseError};
use futures::StreamExt;

/// A pool that manages etcd leases, automatically keeping them alive and grouping leases with the
/// same TTL.
///
/// See the [module-level documentation](self) for details.
#[derive(Clone)]
pub struct LeasePool(Arc<LeasePoolInner>);

struct LeasePoolInner {
    shared: Arc<Shared>,
    task: tokio::task::AbortHandle,
}

impl Drop for LeasePoolInner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Shared {
    client: Client,
    state: Mutex<PoolState>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct PoolState {
    /// TTL in whole seconds to the pooled [`LeaseId`] for that TTL.
    pooled: HashMap<u64, LeaseId>,
    /// All leases being kept alive, keyed by their [`LeaseId`].
    tracked: HashMap<LeaseId, TrackedLease>,
}

struct TrackedLease {
    /// The originally-requested TTL in whole seconds.
    requested_ttl_secs: u64,
    /// Whether this lease was obtained via [`LeasePool::get_lease`] (pooled) or
    /// [`LeasePool::grant_lease`] (standalone).
    pooled: bool,
    /// When the next keep-alive should be sent.
    next_keepalive: tokio::time::Instant,
}

impl LeasePool {
    /// Create a new lease pool backed by the given client.
    ///
    /// This spawns a background task that maintains the keep-alive stream.
    pub fn new(client: Client) -> Self {
        let shared = Arc::new(Shared {
            client,
            state: Mutex::new(PoolState::default()),
            notify: tokio::sync::Notify::new(),
        });

        let task_shared = Arc::clone(&shared);
        let task = tokio::spawn(keepalive_loop(task_shared));

        Self(Arc::new(LeasePoolInner {
            shared,
            task: task.abort_handle(),
        }))
    }

    /// Get a lease with the requested TTL, reusing an existing pooled lease if one exists.
    ///
    /// The TTL is truncated to whole seconds (matching etcd's granularity). If a lease with that
    /// TTL already exists in the pool, its [`LeaseId`] is returned. Otherwise, a new lease is
    /// granted from the server and added to the pool.
    pub async fn get_lease(&self, ttl: Duration) -> Result<LeaseId, GrantLeaseError> {
        let ttl_secs = ttl.as_secs();

        // Fast path: reuse an existing pooled lease.
        {
            let state = self.0.shared.state.lock().unwrap();
            if let Some(&lease_id) = state.pooled.get(&ttl_secs) {
                return Ok(lease_id);
            }
        }

        // Slow path: grant a new lease from the server.
        let response = self.0.shared.client.grant_lease().ttl(ttl).await?;
        let lease_id = response.lease_id;
        let server_ttl_secs = response.ttl.map_or(ttl_secs, |d| d.as_secs());

        {
            let mut state = self.0.shared.state.lock().unwrap();
            // Another task may have raced us; use theirs and let ours expire naturally.
            if let Some(&existing) = state.pooled.get(&ttl_secs) {
                return Ok(existing);
            }
            state.pooled.insert(ttl_secs, lease_id);
            state.tracked.insert(
                lease_id,
                TrackedLease {
                    requested_ttl_secs: ttl_secs,
                    pooled: true,
                    next_keepalive: tokio::time::Instant::now() + keepalive_interval(server_ttl_secs),
                },
            );
        }

        self.0.shared.notify.notify_one();
        Ok(lease_id)
    }

    /// Grant a new lease from the server, even if a pooled lease with the same TTL already exists.
    ///
    /// The lease is tracked and kept alive automatically, but is not part of the TTL-grouped pool.
    pub async fn grant_lease(&self, ttl: Duration) -> Result<LeaseId, GrantLeaseError> {
        let ttl_secs = ttl.as_secs();
        let response = self.0.shared.client.grant_lease().ttl(ttl).await?;
        let lease_id = response.lease_id;
        let server_ttl_secs = response.ttl.map_or(ttl_secs, |d| d.as_secs());

        {
            let mut state = self.0.shared.state.lock().unwrap();
            state.tracked.insert(
                lease_id,
                TrackedLease {
                    requested_ttl_secs: ttl_secs,
                    pooled: false,
                    next_keepalive: tokio::time::Instant::now() + keepalive_interval(server_ttl_secs),
                },
            );
        }

        self.0.shared.notify.notify_one();
        Ok(lease_id)
    }

    /// Revoke a lease, removing it from the pool and sending a revoke request to the server.
    ///
    /// If the lease was a pooled lease (obtained via [`get_lease`][Self::get_lease]), it is removed
    /// from the pool and future calls to `get_lease` with that TTL will create a new one.
    pub async fn revoke_lease(&self, lease_id: LeaseId) -> Result<(), RevokeLeaseError> {
        {
            let mut state = self.0.shared.state.lock().unwrap();
            if let Some(tracked) = state.tracked.remove(&lease_id)
                && tracked.pooled
            {
                state.pooled.remove(&tracked.requested_ttl_secs);
            }
        }

        self.0.shared.client.revoke_lease(lease_id).await?;
        Ok(())
    }
}

/// Compute the keep-alive interval for a given TTL.
///
/// Targets `ttl / 3`, with a minimum of 1 second to avoid spinning on very short TTLs.
fn keepalive_interval(ttl_secs: u64) -> Duration {
    Duration::from_secs((ttl_secs / 3).max(1))
}

/// Background task that maintains the keep-alive stream and sends periodic keep-alive requests for
/// all tracked leases.
async fn keepalive_loop(shared: Arc<Shared>) {
    loop {
        // Wait until there are leases to track before opening a stream.
        if shared.state.lock().unwrap().tracked.is_empty() {
            shared.notify.notified().await;
            continue;
        }

        let (sender, mut stream) = shared.client.lease_keeper().into_parts();

        // On (re)connect, immediately refresh all tracked leases.
        {
            let state = shared.state.lock().unwrap();
            for &lease_id in state.tracked.keys() {
                sender.keep_alive(lease_id);
            }
        }

        loop {
            let next_wakeup = {
                let state = shared.state.lock().unwrap();
                state
                    .tracked
                    .values()
                    .map(|t| t.next_keepalive)
                    .min()
                    .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(60))
            };

            tokio::select! {
                _ = tokio::time::sleep_until(next_wakeup) => {
                    let now = tokio::time::Instant::now();
                    let mut state = shared.state.lock().unwrap();
                    for (&lease_id, tracked) in state.tracked.iter_mut() {
                        if tracked.next_keepalive <= now {
                            sender.keep_alive(lease_id);
                            // Tentative schedule; updated when the server responds with actual TTL.
                            tracked.next_keepalive =
                                now + keepalive_interval(tracked.requested_ttl_secs);
                        }
                    }
                }
                response = stream.next() => {
                    match response {
                        Some(Ok(resp)) => {
                            let mut state = shared.state.lock().unwrap();
                            match resp.ttl {
                                Some(ttl) => {
                                    if let Some(tracked) = state.tracked.get_mut(&resp.lease_id) {
                                        tracked.next_keepalive = tokio::time::Instant::now()
                                            + keepalive_interval(ttl.as_secs());
                                    }
                                }
                                None => {
                                    // Lease expired on the server.
                                    if let Some(tracked) = state.tracked.remove(&resp.lease_id)
                                        && tracked.pooled
                                    {
                                        state.pooled.remove(&tracked.requested_ttl_secs);
                                    }
                                }
                            }
                        }
                        Some(Err(_)) | None => break,
                    }
                }
                _ = shared.notify.notified() => continue,
            }
        }

        // Brief delay before reconnecting to avoid a tight retry loop.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
