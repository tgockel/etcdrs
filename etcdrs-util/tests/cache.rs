use std::sync::Arc;
use std::time::Duration;

use etcdrs::client::{GetResponse, Put, RequestCounter};
use etcdrs::{Client, Prefix};
use etcdrs_test::{EtcdServer, etcd_server};
use etcdrs_util::cache::CacheClient;
use futures::StreamExt;
use rstest::rstest;

/// A client whose unary requests are counted, for proving reads are served locally. Watch-stream
/// traffic (including progress requests) does not pass through the unary metrics hook, so the
/// counter isolates exactly the request kinds the cache is supposed to save.
fn counting_client(server: &EtcdServer) -> (Client, Arc<RequestCounter>) {
    let metrics = Arc::new(RequestCounter::default());
    let client = Client::builder()
        .add_connection(server.connect_string())
        .unwrap()
        .metrics(metrics.clone())
        .build()
        .unwrap();
    (client, metrics)
}

/// Poll `condition` until it returns `Some`, panicking after 30 seconds.
///
/// The deadline is deliberately generous (matching the harness's server-readiness wait): after a
/// server restart, each failed watch re-establishment attempt can burn ~5 seconds, and heavily
/// parallel test runs multiply that. A passing condition returns immediately regardless.
async fn eventually<T>(mut condition: impl AsyncFnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = condition().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within 30 seconds"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until a get for `key` is served from the cache (no unary request issued) with the
/// `expected` value (`None` = key absent), returning that response.
async fn eventually_cached(
    cache: &CacheClient,
    metrics: &RequestCounter,
    key: &str,
    expected: Option<&[u8]>,
) -> GetResponse {
    eventually(async || {
        let before = metrics.get().requested();
        let response = cache.get(key).await.expect("get failed");
        let served_locally = metrics.get().requested() == before;
        let value_matches = response.record().map(|record| record.value().as_ref()) == expected;
        (served_locally && value_matches).then_some(response)
    })
    .await
}

#[rstest]
#[tokio::test]
async fn cached_get_serves_without_server_request(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();

    let response = eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;
    let record = response.record().unwrap();
    assert!(response.header().revision() >= record.metadata().modified_revision);

    let before = metrics.get().requested();
    for _ in 0..5 {
        let response = cache.get("foo/a").await.unwrap();
        assert_eq!(response.record().unwrap().value(), &b"v1"[..]);
    }
    assert_eq!(metrics.get().requested(), before);
}

#[rstest]
#[tokio::test]
async fn unseeded_reads_pass_through_until_warm(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();

    // Immediately after build the range is (very likely) not seeded yet; the read must still be
    // correct, at ordinary passthrough cost.
    let response = cache.get("foo/a").await.unwrap();
    assert_eq!(response.record().unwrap().value(), &b"v1"[..]);

    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;
    assert!(cache.coherent_revision(Prefix("foo/")).is_some());
    assert!(cache.coherent_revision(Prefix("other/")).is_none());
    assert!(cache.last_known_revision().is_some());
}

#[rstest]
#[tokio::test]
async fn watch_updates_become_visible(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    // An external overwrite and an external new key both arrive via the watch.
    external.put("foo/a").value("v2").await.unwrap();
    external.put("foo/new").value("n1").await.unwrap();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v2")).await;
    eventually_cached(&cache, &metrics, "foo/new", Some(b"n1")).await;
}

#[rstest]
#[tokio::test]
async fn watch_delete_removes_from_cache(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();
    external.put("foo/b").value("v2").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    external.delete("foo/a").await.unwrap();

    // The deletion becomes an authoritative locally-served None; the sibling is untouched.
    eventually_cached(&cache, &metrics, "foo/a", None).await;
    eventually_cached(&cache, &metrics, "foo/b", Some(b"v2")).await;
}

#[rstest]
#[tokio::test]
async fn read_your_writes_after_put_through_cache(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/x").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/x", Some(b"v1")).await;

    // A get issued immediately after a put through the cache must observe the put, whether the
    // watch has caught up (cache-served) or not (gated passthrough).
    let put = cache.put("foo/x").value("v2").await.unwrap();
    let response = cache.get("foo/x").await.unwrap();
    assert_eq!(response.record().unwrap().value(), &b"v2"[..]);
    assert!(cache.last_known_revision().unwrap() >= put.header().revision());

    eventually_cached(&cache, &metrics, "foo/x", Some(b"v2")).await;
}

#[rstest]
#[tokio::test]
async fn write_outside_cache_gates_cached_reads(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    // A write through the cache to an uncached key produces no watch event for `foo/`, so the
    // range cannot prove freshness: the next read must hit the server...
    cache.put("bar/other").value("x").await.unwrap();
    let before = metrics.get().requested();
    let response = cache.get("foo/a").await.unwrap();
    assert_eq!(metrics.get().requested(), before + 1);
    assert_eq!(response.record().unwrap().value(), &b"v1"[..]);

    // ...and the progress request fired by that gated read re-warms the range.
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;
}

#[rstest]
#[tokio::test]
async fn external_write_does_not_gate(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    // An external write to an uncached key is never observed through this client, so it does not
    // gate: the very next read still serves locally.
    external.put("bar/unrelated").value("x").await.unwrap();
    let before = metrics.get().requested();
    let response = cache.get("foo/a").await.unwrap();
    assert_eq!(metrics.get().requested(), before);
    assert_eq!(response.record().unwrap().value(), &b"v1"[..]);
}

#[rstest]
#[tokio::test]
async fn pinned_revision_get_passes_through(etcd_server: EtcdServer) {
    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();

    let first = cache.put("foo/p").value("v1").await.unwrap();
    let pinned = first.header().revision();
    cache.put("foo/p").value("v2").await.unwrap();
    eventually_cached(&cache, &metrics, "foo/p", Some(b"v2")).await;

    // The cache holds only latest values; historical reads always go to the server.
    let before = metrics.get().requested();
    let response = cache.get("foo/p").at_revision(pinned).await.unwrap();
    assert_eq!(metrics.get().requested(), before + 1);
    assert_eq!(response.record().unwrap().value(), &b"v1"[..]);
}

#[rstest]
#[tokio::test]
async fn cached_list_count_and_keys_only(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    let things = [("foo/a", "a"), ("foo/b", "b"), ("foo/c", "c"), ("foo/d", "d")];
    for (key, value) in things {
        external.put(key).value(value).await.unwrap();
    }
    external.put("goo").value("outside").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/d", Some(b"d")).await;

    let before = metrics.get().requested();

    let view = cache.list_prefix("foo/").await.unwrap();
    assert_eq!(view.revision(), cache.coherent_revision(Prefix("foo/")).unwrap());
    let records = view.into_stream().map(Result::unwrap).collect::<Vec<_>>().await;
    let keys: Vec<&[u8]> = records.iter().map(|record| record.key().as_ref()).collect();
    assert_eq!(keys, [&b"foo/a"[..], b"foo/b", b"foo/c", b"foo/d"]);
    assert_eq!(records[0].value(), &b"a"[..]);

    let keys_only = cache
        .list_prefix("foo/")
        .keys_only()
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(keys_only.len(), things.len());

    let count = cache.list_prefix("foo/").count_only().await.unwrap().count();
    assert_eq!(count, things.len());

    // `limit` is a per-page fetch size; a cache-served list still yields everything.
    let limited = cache
        .list_prefix("foo/")
        .limit(1)
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(limited.len(), things.len());

    let sub = cache
        .list("foo/a".."foo/c")
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    let sub_keys: Vec<&[u8]> = sub.iter().map(|record| record.key().as_ref()).collect();
    assert_eq!(sub_keys, [&b"foo/a"[..], b"foo/b"]);

    assert_eq!(metrics.get().requested(), before);
}

#[rstest]
#[tokio::test]
async fn partial_overlap_list_passes_through(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();
    external.put("zoo").value("v2").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    // A query straddling the cached range's boundary is not fully covered and must pass through.
    let before = metrics.get().requested();
    let spanning = cache
        .list("foo/a".."zzz")
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(metrics.get().requested(), before + 1);
    assert_eq!(spanning.len(), 2);

    let before = metrics.get().requested();
    let all = cache
        .list(..)
        .await
        .unwrap()
        .into_stream()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(metrics.get().requested(), before + 1);
    assert_eq!(all.len(), 2);
}

#[rstest]
#[tokio::test]
async fn uncached_key_passes_through(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("bar/k").value("v").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually(async || cache.coherent_revision(Prefix("foo/")).map(|_| ())).await;

    let before = metrics.get().requested();
    for _ in 0..2 {
        let response = cache.get("bar/k").await.unwrap();
        assert_eq!(response.record().unwrap().value(), &b"v"[..]);
    }
    cache.put("bar/k2").value("x").await.unwrap();
    assert_eq!(metrics.get().requested(), before + 3);
}

#[rstest]
#[tokio::test]
async fn transaction_passes_through_and_bumps_last_known(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/t").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/t", Some(b"v1")).await;

    let txn = cache
        .transaction()
        .and_then_do(Put::new("foo/t").value("v2"))
        .commit()
        .await
        .unwrap();
    assert!(txn.succeeded());

    // Same read-your-writes contract as a plain put.
    let response = cache.get("foo/t").await.unwrap();
    assert_eq!(response.record().unwrap().value(), &b"v2"[..]);
    assert!(cache.last_known_revision().unwrap() >= txn.revision());

    eventually_cached(&cache, &metrics, "foo/t", Some(b"v2")).await;
}

#[rstest]
#[tokio::test]
async fn multi_range_independence(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("a/1").value("x1").await.unwrap();
    external.put("b/1").value("y1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client)
        .cache(Prefix("a/"))
        .cache(Prefix("b/"))
        .build();
    eventually_cached(&cache, &metrics, "a/1", Some(b"x1")).await;
    eventually_cached(&cache, &metrics, "b/1", Some(b"y1")).await;

    external.put("a/2").value("x2").await.unwrap();
    external.put("b/2").value("y2").await.unwrap();
    external.put("a/1").value("x1b").await.unwrap();
    external.put("b/1").value("y1b").await.unwrap();

    // etcd guarantees no event ordering across separate watches on one stream, so convergence is
    // asserted per key, never across ranges.
    eventually_cached(&cache, &metrics, "a/1", Some(b"x1b")).await;
    eventually_cached(&cache, &metrics, "a/2", Some(b"x2")).await;
    eventually_cached(&cache, &metrics, "b/1", Some(b"y1b")).await;
    eventually_cached(&cache, &metrics, "b/2", Some(b"y2")).await;

    assert!(cache.coherent_revision(Prefix("a/")).is_some());
    assert!(cache.coherent_revision(Prefix("b/")).is_some());
}

#[rstest]
#[tokio::test]
async fn overlapping_ranges_serve_consistently(etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/b").value("vb").await.unwrap();
    external.put("foo/z").value("vz").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client)
        .cache(Prefix("foo/"))
        .cache("foo/a".."foo/m")
        .build();

    // foo/b lives in both configured ranges, foo/z only in the prefix.
    eventually_cached(&cache, &metrics, "foo/b", Some(b"vb")).await;
    eventually_cached(&cache, &metrics, "foo/z", Some(b"vz")).await;

    external.put("foo/b").value("vb2").await.unwrap();
    eventually_cached(&cache, &metrics, "foo/b", Some(b"vb2")).await;

    external.delete("foo/b").await.unwrap();
    eventually_cached(&cache, &metrics, "foo/b", None).await;
}

#[rstest]
#[tokio::test]
async fn server_restart_reconnects_and_converges(mut etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/a").value("v1").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v1")).await;

    etcd_server.stop().unwrap();

    // While the server is down nothing observes a newer revision, so the range keeps serving its
    // honest snapshot locally.
    let before = metrics.get().requested();
    let response = cache.get("foo/a").await.unwrap();
    assert_eq!(metrics.get().requested(), before);
    assert_eq!(response.record().unwrap().value(), &b"v1"[..]);

    etcd_server.start().unwrap();

    // The first external write may race the server's startup; retry until it lands.
    eventually(async || external.put("foo/a").value("v2").await.ok().map(|_| ())).await;
    eventually_cached(&cache, &metrics, "foo/a", Some(b"v2")).await;
}

#[rstest]
#[tokio::test]
async fn compaction_recovery_converges(mut etcd_server: EtcdServer) {
    let external = Client::new(&etcd_server.connect_string()).unwrap();
    external.put("foo/k").value("v0").await.unwrap();

    let (client, metrics) = counting_client(&etcd_server);
    let cache = CacheClient::builder(client).cache(Prefix("foo/")).build();
    eventually_cached(&cache, &metrics, "foo/k", Some(b"v0")).await;

    // Break the watch, then advance and compact history while it reconnects. Whether the
    // reconnect wins the race (normal resume) or loses it (Compacted error -> reseed), the cache
    // must converge; this exercises the reseed path probabilistically without ever being flaky.
    etcd_server.stop().unwrap();
    etcd_server.start().unwrap();

    eventually(async || external.put("foo/k").value("v1").await.ok().map(|_| ())).await;
    for i in 2..=20 {
        external.put("foo/k").value(format!("v{i}")).await.unwrap();
    }
    let head = external.get("foo/k").await.unwrap().header().revision();
    cache.compact(head).physical().await.unwrap();

    eventually_cached(&cache, &metrics, "foo/k", Some(b"v20")).await;
}
