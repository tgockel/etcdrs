use etcdrs::{
    Prefix, TransactionErrorKind,
    client::{Delete, Get, List, Put, TransactionCheck, TransactionOp, TransactionOpResponse},
};
use etcdrs_test::{EtcdCluster, EtcdServer, etcd_cluster, etcd_server};
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn big_transaction(etcd_cluster: EtcdCluster) {
    let client = etcdrs::Client::new(&etcd_cluster.connect_string()).unwrap();
    let result = client
        .transaction()
        .when([TransactionCheck::not_present("foo")])
        .and_then([
            Put::new("foo").value("bar").into(),
            Put::new("bar").value("baz").get_previous().into(),
            Get::new("baz").into(),
            Delete::new("baz").into(),
            Delete::new("fizz").get_previous().into(),
            List::new(..).count_only().into(),
        ])
        .commit()
        .await
        .unwrap();
    assert!(result.succeeded());
    assert_eq!(result.responses().len(), 6);

    let mut result_iter = result.take_responses().into_iter();
    assert!(matches!(result_iter.next(), Some(TransactionOpResponse::Put)));
    assert!(matches!(
        result_iter.next(),
        Some(TransactionOpResponse::PutPrevious(None))
    ));
    assert!(matches!(result_iter.next(), Some(TransactionOpResponse::Get(None))));
    assert!(matches!(result_iter.next(), Some(TransactionOpResponse::Delete(false))));
    assert!(matches!(
        result_iter.next(),
        Some(TransactionOpResponse::DeletePrevious(None))
    ));
    assert!(matches!(result_iter.next(), Some(TransactionOpResponse::Count(2))));

    let result = client
        .transaction()
        .when([
            // We created this key last transaction, so it will exist
            TransactionCheck::not_present("foo"),
        ])
        .and_then_do(Put::new("foo"))
        .or_else_do(Get::new("foo"))
        .commit()
        .await
        .unwrap();
    assert!(!result.succeeded());
    assert_eq!(result.responses().len(), 1);
    let TransactionOpResponse::Get(Some(record)) = &result.responses()[0] else {
        unreachable!()
    };
    assert_eq!(record.value(), &b"bar"[..]);
}

#[rstest]
#[tokio::test]
async fn duplicate_key(etcd_cluster: EtcdCluster) {
    let client = etcdrs::Client::new(&etcd_cluster.connect_string()).unwrap();
    let error = client
        .transaction()
        .and_then([
            // Putting the same key twice in a transaction is not allowed
            Put::new("foo").value("bar").into(),
            Put::new("foo").value("baz").into(),
        ])
        .commit()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), TransactionErrorKind::InvalidArgument);
}

/// A transaction reads and deletes from the empty key as a lone request does: from the first key.
/// A default `List` and every range from `""` used to put the empty key in the transaction, and
/// etcd refused the whole transaction for it.
#[rstest]
#[tokio::test]
async fn ranges_from_the_empty_key_in_a_transaction(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    for key in ["\0", "a", "b"] {
        client.put(key).value(key).await.unwrap();
    }

    let result = client
        .transaction()
        .and_then([
            List::default().count_only().into(),
            List::new(Prefix("")).count_only().into(),
            List::new(""..).count_only().into(),
            List::new("".."b").count_only().into(),
            Delete::with_prefix("").into(),
        ])
        .commit()
        .await
        .unwrap();
    assert!(
        matches!(
            result.responses(),
            [
                TransactionOpResponse::Count(3),
                TransactionOpResponse::Count(3),
                TransactionOpResponse::Count(3),
                TransactionOpResponse::Count(2),
                TransactionOpResponse::DeleteRange(3),
            ]
        ),
        "{:?}",
        result.responses()
    );
}

/// etcd has no empty key, and refuses a whole transaction that reads or deletes one.
#[rstest]
#[tokio::test]
async fn the_empty_key_is_refused_in_a_transaction(etcd_server: EtcdServer) {
    let client = etcdrs::Client::new(&etcd_server.connect_string()).unwrap();
    let ops: [TransactionOp; 3] = [
        Get::new("").into(),
        List::new("").count_only().into(),
        Delete::new("").into(),
    ];
    for op in ops {
        let err = client.transaction().and_then_do(op).commit().await.unwrap_err();
        assert_eq!(err.kind(), TransactionErrorKind::InvalidArgument, "{err:?}");
        assert_eq!(
            err.grpc_status().map(|s| s.message()),
            Some("etcdserver: key is not provided"),
            "{err:?}"
        );
    }
}
