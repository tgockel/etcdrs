use etcdrs::{
    client::{Delete, Get, List, Put, TransactionCheck, TransactionOpResponse},
    TransactionErrorKind,
};
use rstest::rstest;

use crate::{tests::etcd_cluster, EtcdCluster};

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
    assert_eq!(record.value(), b"bar");
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
