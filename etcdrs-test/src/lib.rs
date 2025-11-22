mod server;
pub use server::{EtcdCluster, EtcdServer};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use etcdrs::Version;
    use futures::StreamExt;
    use rstest::{fixture, rstest};

    use crate::*;

    #[fixture]
    fn etcd_server() -> EtcdServer {
        server::EtcdServerConfig::new_single_temporary().start().unwrap()
    }

    #[fixture]
    fn etcd_cluster() -> EtcdCluster {
        server::EtcdClusterConfig::with_generated_peers(3).start().unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn get_put_get(etcd_cluster: EtcdCluster) {
        let client = etcdrs::Client::new(&etcd_cluster.connect_string()).unwrap();
        assert!(client.get("foo").await.unwrap().is_none());

        client.put("foo").value("value").await.unwrap();
        let fetched = client.get("foo").await.unwrap().unwrap();
        assert_eq!(fetched.value(), b"value");
        assert_eq!(fetched.metadata().version, Version::new(1));
    }

    #[rstest]
    #[tokio::test]
    async fn list_basics(etcd_server: EtcdServer) {
        let metrics = Arc::new(etcdrs::client::RequestCounter::default());
        let client = etcdrs::Client::builder()
            .add_connection(etcd_server.connect_string())
            .unwrap()
            .metrics(metrics.clone())
            .build()
            .unwrap();

        let things = [("foo/a", b"a"), ("foo/b", b"b"), ("foo/c", b"c"), ("foo/d", b"d")];
        for (key, value) in things.iter() {
            client.put(*key).value(*value).await.unwrap();
        }
        assert_eq!(4, metrics.get().succeeded());

        let count = client.list_prefix("foo/").count_only().await.unwrap();
        assert_eq!(count, things.len());
        assert_eq!(5, metrics.get().succeeded());
        let keys = client
            .list("foo/a"..="foo/d")
            .keys_only()
            .limit(2)
            .into_stream()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(count, keys.len());
        assert_eq!(7, metrics.get().succeeded()); // NOTE: `.limit(2)` above takes 2 requests to list 4 records
    }

    #[rstest]
    #[tokio::test]
    async fn create_delete_get(etcd_server: EtcdServer) {
        let metrics = Arc::new(etcdrs::client::RequestCounter::default());
        let client = etcdrs::Client::builder()
            .add_connection(etcd_server.connect_string())
            .unwrap()
            .metrics(metrics.clone())
            .build()
            .unwrap();

        assert!(client.get("foo").await.unwrap().is_none());
        client.put("foo").value("value").await.unwrap();
        let fetched = client.get("foo").await.unwrap().unwrap();
        assert_eq!(fetched.value(), b"value");
        assert_eq!(fetched.metadata().version, Version::new(1));

        let deleted = client.delete("foo").await.unwrap();
        assert!(deleted);
        let deleted = client.delete("foo").await.unwrap();
        assert!(!deleted);
    }
}
