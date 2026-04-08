use crate::client::{WatchBuilder, Watcher};

/// Driver for [`watch`][crate::Client::watch] operations.
pub trait WatchDriver {
    /// Start watching using the accumulated watch specs.
    fn start_watch(self, builder: WatchBuilder<()>) -> Watcher;
}
