# Key-Value Caching

A [`CacheClient`] mirrors the key-value operations of an [`etcdrs::Client`], serving reads of
configured key ranges from a local, in-memory store that a background task keeps coherent with the
server through a watch. Reads of keys potentially-stale keys or outside the configured ranges
transparently pass through to the server, so a read through the cache is never slower than one
through the wrapped client.

## Usage

```rust,no_run
use etcdrs::Prefix;
use etcdrs_util::cache::CacheClient;

# async {
let client: etcdrs::Client = todo!();
let cache = CacheClient::builder(client)
    .cache("leader")            // a single key
    .cache("jobs/a".."jobs/m")  // an arbitrary range
    .cache_prefix("config/")    // every key under a prefix
    .build();

// Reads of cached ranges are served locally once the range is seeded and fresh.
let response = cache.get("config/max-connections").await.expect("get failed");
println!("value: {:?}", response.record().map(|r| r.value()));

// Writes always go to the server; the change arrives in the cache via the watch.
cache.put("config/max-connections").value("100").await.expect("put failed");

// This get is guaranteed to observe the put above (read-your-writes): the cache
// knows its store is behind, so the read passes through until the watch catches
// up.
let _fresh = cache.get("config/max-connections").await.expect("get failed");
# };
```

Requires the **`cache`** feature (enabled by default). [`CacheClientBuilder::build`] spawns a
background task, so it must be called within a Tokio runtime.

## Coherence Model

The cache tracks two kinds of revision, with different strengths:

- **Coherent revision** (per range): the store revision the range's cached contents are exact as
  of. Only the watch advances it. A watch event or progress notification at revision `R`
  guarantees everything in that watch's range has its latest value as of `R`.
- **Last-known revision** (global): the highest store revision ever observed through this
  `CacheClient`; from the response header of any operation executed through it, from seed
  snapshots, and from watch events. Seeing revision `R` from an operation only proves the store
  has reached `R`; the corresponding changes arrive in the cache later, through the watch.

Both are exposed for introspection via [`CacheClient::coherent_revision`] and
[`CacheClient::last_known_revision`], and they meet in a single serving rule (the *gate*):

> A range serves a read if and only if it is seeded and its coherent revision has caught up to
> the last-known revision. Otherwise the read passes through to the server.

Consequences:

- **Read-your-writes.** A write through the `CacheClient` advances the last-known revision before
  its future resolves, so a subsequent read of a cached range either sees the watch-applied change
  or passes through to the server, never the pre-write value.
- **Bounded latency.** The cache never blocks a read waiting for the watch; a read that cannot be
  proven fresh costs one ordinary server round trip.
- **Re-warming.** When a gated read passes through, the cache asks the watch for a progress
  notification (debounced). One notification re-proves freshness for every range on the stream, so
  quiet ranges return to local serving about one round trip after the disturbance.
- **External writers do not gate.** Writes made by other clients bump nothing here; they simply
  arrive through the watch within its normal propagation delay. The gate defends the revisions
  *this* client has observed, not global freshness.

Cache-served responses reuse the [`ResponseHeader`][etcdrs::ResponseHeader] observed from the
watch or seed, so `header().revision()` always reports the coherent revision the data is exact as
of.

## Serving Rules

| Operation | Served from the cache when...                                                        |
|-----------|--------------------------------------------------------------------------------------|
| [`get`][CacheClient::get] | the key lies in a configured range and the gate passes; a missing key is an authoritative `None` |
| [`list`][CacheClient::list] / [`list_prefix`][CacheClient::list_prefix] (including `keys_only` and `count_only`) | the query is fully contained in a **single** configured range and the gate passes |
| any read pinned with [`at_revision`][etcdrs::client::Get::at_revision] or a list revision | never — the cache holds only latest values |
| [`put`][CacheClient::put], [`delete`][CacheClient::delete] (all forms), [`transaction`][CacheClient::transaction], [`compact`][CacheClient::compact] | never: writes always execute on the server and never mutate the local store |

[`limit`][etcdrs::client::List::limit] on a cache-served list is ignored: it is a per-page fetch
size, and the client-side stream paginates to completion anyway, so the returned set is identical.

## Failure and Recovery

The background task seeds each configured range with a revision-pinned list, then watches all
ranges on one stream, each watch resuming from just past that range's coherent revision. Recovery
is automatic:

- **Seeding failures** retry every 100ms; reads of an unseeded range pass through, so startup
  costs at most one ordinary round trip per read.
- **Stream loss** (network failure, server restart): the task re-establishes the watch and resumes
  every range from its coherent revision; nothing is lost or reapplied. While disconnected,
  ranges keep serving their last coherent snapshot (labeled with its revision) until an operation
  observes a newer revision, at which point reads pass through like an uncached client.
- **Compaction** past a watch's resume point: that range is re-listed at the current revision and
  its watch re-added; other ranges are unaffected.

Dropping the last `CacheClient` handle aborts the background task, which cancels its watches.

## Caveats

- **Memory**: the cache holds a full copy of every configured range (keys, latest values, and
  metadata), growing with the keyspace. Overlapping configured ranges are allowed, but each keeps
  an independent copy of the overlap.
- **Write-heavy workloads**: every revision observed through this client gates all ranges until a
  progress notification lands (about one round trip). Under constant write churn through the same
  `CacheClient`, reads degrade toward passthrough. Read-mostly workloads are the target.
- **Multi-key transactions**: events of one revision are applied atomically per watch response,
  but a fragmented response (etcd fragments at roughly 1.5MB) may split one revision across
  network reads, briefly exposing a prefix of that revision's changes. Pinned-revision reads
  (which always pass through) are the escape hatch for strict multi-key snapshots.
- **Stream-wide progress** correctness relies on etcd ≥ 3.4.26 / 3.5.9; older servers could send
  a progress notification before delivering all prior events, which would let the cache serve
  stale data as fresh.
- Cached ranges are fixed at construction. Dynamic add/remove is future work, as is serving
  pinned reads at revisions the cache can prove (a present key with
  `modified_revision <= R <= coherent`).
