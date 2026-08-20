//! Retry behavior of the unary call path.
//!
//! These tests need no server. They aim a client at port 0, which no socket can ever be bound to --
//! `bind` treats 0 as "assign me a port", so it is never assigned as one. Connecting there fails
//! inside the connector every time, which is the one failure the client can prove never reached a
//! server, and therefore the one it is willing to replay even for a mutating RPC.
//!
//! Port 0 is deliberate rather than an arbitrary unused port. A port that merely looks free is a
//! race: something can claim it between the check and the connect, and then a retried mutation
//! would be delivered to whatever answered. Port 0 cannot be claimed, so there is nothing to race.
//!
//! The request counter is incremented once per *attempt*, at the point the attempt starts, because
//! the metrics span is opened inside the retry loop. That is what makes it usable as a retry oracle
//! here.

use std::error::Error as _;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use etcdrs::client::RequestCounter;
use etcdrs::{Client, OperationError, RetryPolicy};

/// Long enough that a retry is never in doubt. Nothing waits this out: the retrying calls are
/// cancelled the moment they have proven the point, and the tests that do run to completion use
/// [`RetryPolicy::Never`].
const GENEROUS: Duration = Duration::from_secs(30);

/// A client aimed at an address nothing can be listening on.
fn unreachable_client(policy: impl Into<RetryPolicy>) -> (Client, Arc<RequestCounter>) {
    let metrics = Arc::new(RequestCounter::default());
    let client = Client::builder()
        .add_connection("http://127.0.0.1:0")
        .unwrap()
        .metrics(metrics.clone())
        .retry_policy(policy)
        .build()
        .unwrap();
    (client, metrics)
}

/// Bound a call so that a connector which stalls instead of refusing fails the test rather than
/// hanging the suite. [`RetryPolicy::Never`] sends no gRPC deadline of its own, so nothing else
/// here would stop it.
async fn within<T>(call: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), call)
        .await
        .expect("a call to an unconnectable address should fail immediately, not hang")
}

/// Assert `err` is a failure the connector produced before writing any of the request.
///
/// This is the property the retry rule keys on, so the tests check it rather than inferring it.
/// `tonic::ConnectError` appears only for TCP/UDS connect and TLS handshake failures, and a status
/// decoded from response headers has no source at all, so a server cannot produce one.
fn assert_never_sent(err: &impl OperationError) {
    let status = err.grpc_status().expect("a connect failure carries a gRPC status");
    let mut source = status.source();
    while let Some(inner) = source {
        if inner.is::<tonic::ConnectError>() {
            return;
        }
        source = inner.source();
    }
    panic!("expected a pre-send connect failure, got one that reached a server: {status:?}");
}

/// Confirm the endpoint really does fail before anything is sent, so the retry assertions below
/// are testing the rule and not some unrelated failure.
async fn assert_endpoint_fails_before_sending() {
    let (client, _) = unreachable_client(RetryPolicy::Never);
    let err = within(async { client.put("retry-probe").value("v").await })
        .await
        .expect_err("put against an unconnectable address should fail");
    assert_never_sent(&err);
}

/// Wait until a second attempt has started, and return the count seen.
///
/// This waits for the evidence rather than giving the client a fixed window and hoping two attempts
/// fit inside it. The retry budget is an absolute deadline that starts before the first attempt, so
/// a window narrow enough to keep the test quick is also narrow enough for one slow connect to
/// consume -- which would fail the test while the client behaved correctly. Waiting instead means a
/// stalled first attempt only makes this slower.
async fn wait_for_second_attempt(metrics: &RequestCounter) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let attempts = metrics.get().requested();
        if attempts > 1 {
            return attempts;
        }
        assert!(
            Instant::now() < deadline,
            "the call was never retried; {attempts} attempt(s) recorded",
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test]
async fn mutations_retry_when_the_connection_was_never_made() {
    assert_endpoint_fails_before_sending().await;

    let (client, metrics) = unreachable_client(GENEROUS);
    let call = tokio::spawn(async move { client.put("retry-put").value("v").await });

    let attempts = wait_for_second_attempt(&metrics).await;

    // The point is proven; stop the call rather than letting a loop with no backoff spin out the
    // rest of the budget.
    call.abort();
    assert!(
        attempts > 1,
        "a mutation whose request never left the client should still be retried",
    );
}

#[tokio::test]
async fn reads_retry_when_the_connection_was_never_made() {
    assert_endpoint_fails_before_sending().await;

    let (client, metrics) = unreachable_client(GENEROUS);
    let call = tokio::spawn(async move { client.get("retry-get").await });

    let attempts = wait_for_second_attempt(&metrics).await;

    call.abort();
    assert!(attempts > 1, "expected a read to be retried");
}

/// Control for the two tests above: it pins that the counter measures attempts rather than calls,
/// so `> 1` there really does mean "retried". Unlike them it runs to completion, because a client
/// that never retries has a bounded amount of work to do.
#[tokio::test]
async fn retry_never_makes_a_single_attempt() {
    let (client, metrics) = unreachable_client(RetryPolicy::Never);

    let err = within(async { client.put("retry-never").value("v").await })
        .await
        .expect_err("put against an unconnectable address should fail");
    assert_never_sent(&err);

    assert_eq!(metrics.get().requested(), 1);
}
