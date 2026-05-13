use futures_core::Stream;

use crate::client::{WatchBuilder, WatchError, WatchEvent};

/// Driver for [`watch`][crate::Client::watch] operations.
pub trait WatchDriver {
    /// The watcher stream returned by [`start_watch`][Self::start_watch].
    type Watcher: Stream<Item = Result<WatchEvent, WatchError>> + Send;

    /// Start watching using the accumulated watch specs.
    fn start_watch(self, builder: WatchBuilder<()>) -> Self::Watcher;
}
