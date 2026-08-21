use super::{BuildError, Client, ClientBuilder};

/// # Construction
impl Client {
    /// Create a [`ClientBuilder`], for when a connection string alone is not enough.
    ///
    /// Use this to supply credentials, a [`RetryPolicy`][super::RetryPolicy], or a
    /// [`MetricsCollector`][super::MetricsCollector]; [`new`][Self::new] covers everything else.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Create a client from a connection string.
    ///
    /// The connection string is a comma-separated list of endpoint URIs, following the etcd
    /// convention used by `etcdctl --endpoints`:
    ///
    /// ```text
    /// http://host1:2379,http://host2:2379,http://host3:2379
    /// ```
    ///
    /// A single URI is also accepted.
    pub fn new(connection_string: &str) -> Result<Self, BuildError> {
        Self::builder().connection_string(connection_string)?.build()
    }
}
