use std::{
    backtrace::Backtrace,
    borrow::Cow,
    collections::VecDeque,
    fmt,
    num::NonZeroI64,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicI64, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;

use crate::{
    AsRange, LeaseId, Prefix, ResponseHeader, Revision, Version,
    client::{Client, ClientInner, record_from_pb},
    pb::{
        etcdserverpb::{
            self, watch_create_request::FilterType as PbFilterType, watch_request::RequestUnion as PbRequestUnion,
        },
        mvccpb::{self, event::EventType as PbEventType},
    },
    record::{AsKey, KeyWithMetadata, Metadata, Record},
};

/// # Watching
impl Client {
    /// Create a [`WatchBuilder`] for establishing a watch stream.
    ///
    /// Chain [`.key()`][WatchBuilder::key], [`.prefix()`][WatchBuilder::prefix], or
    /// [`.range()`][WatchBuilder::range] to specify targets, then call
    /// [`.start()`][Watch::start] to open the gRPC stream.
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use etcdrs::client::WatchEvent;
    /// # async {
    /// # let client: etcdrs::Client = todo!();
    /// let mut watcher = client.watch()
    ///     .key("leader").get_previous()
    ///     .prefix("services/")
    ///     .start();
    /// while let Some(event) = watcher.next().await {
    ///     match event.unwrap() {
    ///         WatchEvent::Put { record, .. } => println!("put: {record:?}"),
    ///         WatchEvent::Delete { key, .. } => println!("delete: {key:?}"),
    ///         WatchEvent::Progress { revision, .. } => println!("progress: {revision:?}"),
    ///     }
    /// }
    /// # };
    /// ```
    pub fn watch(&self) -> WatchBuilder<Self> {
        WatchBuilder {
            client: self.clone(),
            specs: Vec::new(),
        }
    }

    /// Create a [`Watcher`] from a collection of [`Watch`] specs.
    ///
    /// This is a shortcut for building a [`WatchBuilder`], adding each watch, and calling
    /// [`start`][WatchBuilder::start]:
    ///
    /// ```no_run
    /// # use etcdrs::{client::{Watch, Client}, Prefix};
    /// # let client: Client = todo!();
    /// let watcher = client.watcher([
    ///     Watch::new("foo").get_previous(),
    ///     Watch::new(Prefix("bar/")),
    /// ]);
    /// ```
    pub fn watcher(&self, watches: impl IntoIterator<Item = Watch>) -> Watcher {
        let mut builder = self.watch();
        for watch in watches {
            builder = builder.add(watch);
        }
        builder.start()
    }
}

/// Identifier for an individual watch within a [`Watcher`].
///
/// Watch IDs are assigned by the client when a watch is created — either as part of the initial
/// [`Watch`] builder chain or via [`Watcher::add`]. Every [`WatchEvent`] carries the ID of the
/// watch that produced it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WatchId(NonZeroI64);

impl WatchId {
    pub const fn new(source: i64) -> Option<Self> {
        if let Some(non_zero) = NonZeroI64::new(source) {
            Some(WatchId(non_zero))
        } else {
            None
        }
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

/// An event delivered by a [`WatchStream`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchEvent {
    /// A key was created or updated.
    Put {
        header: ResponseHeader,
        watch_id: WatchId,
        record: Record,
        /// The record as it existed before the event's mutation. This will always be `None` if
        /// [`get_previous`][Watch::get_previous] was not called on the watch.
        prev_record: Option<Record>,
        /// `true` if the key was newly created (as opposed to updated).
        created: bool,
    },
    /// A key was deleted.
    Delete {
        header: ResponseHeader,
        watch_id: WatchId,
        key: KeyWithMetadata,
        /// The record as it existed before it was deleted. This will always be `None` if
        /// [`get_previous`][Watch::get_previous] was not called on the watch.
        prev_record: Option<Record>,
    },
    /// A progress notification from the server.
    ///
    /// Contains the current store revision. The `watch_id` is `None` for responses to a global
    /// [`Watcher::request_progress`] call, or `Some` for per-watch progress notifications enabled
    /// via [`Watch::progress_notify`].
    Progress {
        header: ResponseHeader,
        watch_id: Option<WatchId>,
        revision: Revision,
    },
}

/// A [`Client::watch`] operation builder.
///
/// A `WatchBuilder` collects watch targets — call [`.key()`][Self::key],
/// [`.prefix()`][Self::prefix], or [`.range()`][Self::range] to specify what to watch. Each call
/// transitions to a [`Watch`] builder where per-target options can be set, and additional targets
/// can be chained.
#[must_use = "WatchBuilder does nothing unless you add targets and call `start`"]
pub struct WatchBuilder<C = ()> {
    client: C,
    specs: Vec<etcdserverpb::WatchCreateRequest>,
}

impl WatchBuilder<()> {
    /// Create a new `WatchBuilder` with no client attached.
    ///
    /// Use [`with_client`][Self::with_client] to attach a client before calling `start`, or
    /// use [`Client::watch`] instead.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            client: (),
            specs: Vec::new(),
        }
    }
}

impl<C> WatchBuilder<C> {
    /// Attach a `client` to this builder.
    pub fn with_client<C2>(self, client: C2) -> WatchBuilder<C2> {
        WatchBuilder {
            client,
            specs: self.specs,
        }
    }

    pub(crate) fn into_parts(self) -> (C, WatchBuilder<()>) {
        (
            self.client,
            WatchBuilder {
                client: (),
                specs: self.specs,
            },
        )
    }

    /// Add a pre-built [`Watch`] to this builder.
    #[allow(clippy::should_implement_trait)]
    pub fn add<W>(mut self, watch: Watch<W>) -> Self {
        self.specs.push(watch.current);
        self
    }

    fn _new_watch(self, lower: Bytes, upper: Bytes) -> Watch<Self> {
        Watch {
            watcher: self,
            current: etcdserverpb::WatchCreateRequest {
                key: lower,
                range_end: upper,
                fragment: true,
                ..Default::default()
            },
        }
    }

    /// Watch a single key.
    pub fn key(self, key: impl AsKey) -> Watch<Self> {
        let key = Bytes::copy_from_slice(key.as_key());
        self._new_watch(key, Bytes::new())
    }

    /// Watch all keys with a given prefix.
    pub fn prefix(self, prefix: impl AsKey) -> Watch<Self> {
        self.range(Prefix(prefix))
    }

    /// Watch a range of keys.
    pub fn range(self, range: impl AsRange) -> Watch<Self> {
        let (lower, upper) = range.as_boundaries();
        self._new_watch(lower, upper)
    }
}

impl<C: crate::driver::WatchDriver> WatchBuilder<C> {
    /// Create a watcher and begin watching for events.
    pub fn start(self) -> C::Watcher {
        let (client, builder) = self.into_parts();
        client.start_watch(builder)
    }
}

/// A per-target watch specification for [`Client::watch`] or [`Client::watcher`].
///
/// The type parameter `W` determines what operations are available:
///
/// - **`Watch<()>`** — a standalone single-target spec, created via [`Watch::new`]. Can be
///   configured with per-target options and passed to [`Watcher::add`].
/// - **`Watch<WatchBuilder<C>>`** — part of a builder chain. Additional targets can be chained via
///   [`.key()`][Watch::key], and [`start()`][Watch::start] is available when
///   `C = Client`.
#[must_use = "Watch does nothing unless you call `start` or pass it to `Watcher::add`"]
pub struct Watch<W = ()> {
    watcher: W,
    current: etcdserverpb::WatchCreateRequest,
}

/// Standalone constructor — creates a `Watch<()>` for use with [`Watcher::add`].
impl Watch {
    /// Create a standalone watch spec.
    ///
    /// ```
    /// # use etcdrs::client::Watch;
    /// let watch = Watch::new("my-key");
    /// let watch = Watch::new("a".."z");
    /// let watch = Watch::new(etcdrs::Prefix("config/"));
    /// ```
    pub fn new(query: impl AsRange) -> Self {
        let (lower, upper) = query.as_boundaries();
        Watch {
            watcher: (),
            current: etcdserverpb::WatchCreateRequest {
                key: lower,
                range_end: upper,
                fragment: true,
                ..Default::default()
            },
        }
    }
}

/// Per-target options available for all `W`.
impl<W> Watch<W> {
    /// Start watching from a specific revision.
    ///
    /// Events that occurred at or after this revision will be delivered. If unset, the watch starts
    /// from the current store revision.
    pub fn start_revision(mut self, revision: Revision) -> Self {
        self.current.start_revision = revision.get();
        self
    }

    /// Include the previous key-value in each event.
    ///
    /// When enabled, [`WatchEvent::Put::prev_record`] and [`WatchEvent::Delete::prev_record`] will
    /// contain the record as it existed before the event's mutation.
    pub fn get_previous(mut self) -> Self {
        self.current.prev_kv = true;
        self
    }

    /// Request periodic progress notifications from the server for this watch.
    ///
    /// When enabled, the server sends progress notifications when no events have occurred recently.
    /// These are consumed internally by the stream (they contain no events), but they keep the
    /// connection alive and allow the server to advance its watch revision.
    pub fn progress_notify(mut self) -> Self {
        self.current.progress_notify = true;
        self
    }

    /// Enable or disable put events for this watch.
    ///
    /// When `enabled` is `false`, put events are filtered out (only deletes are delivered).
    /// Calling `.puts(true)` restores the default behavior.
    pub fn puts(mut self, enabled: bool) -> Self {
        let filter = PbFilterType::Noput as i32;
        if enabled {
            self.current.filters.retain(|&f| f != filter);
        } else if !self.current.filters.contains(&filter) {
            self.current.filters.push(filter);
        }
        self
    }

    /// Enable or disable delete events for this watch.
    ///
    /// When `enabled` is `false`, delete events are filtered out (only puts are delivered).
    /// Calling `.deletes(true)` restores the default behavior.
    pub fn deletes(mut self, enabled: bool) -> Self {
        let filter = PbFilterType::Nodelete as i32;
        if enabled {
            self.current.filters.retain(|&f| f != filter);
        } else if !self.current.filters.contains(&filter) {
            self.current.filters.push(filter);
        }
        self
    }
}

/// Chaining methods — only available when `W` is a `WatchBuilder<C>`.
impl<C> Watch<WatchBuilder<C>> {
    fn _finalize_and_new(self, lower: Bytes, upper: Bytes) -> Self {
        let Watch { mut watcher, current } = self;
        watcher.specs.push(current);
        Watch {
            watcher,
            current: etcdserverpb::WatchCreateRequest {
                key: lower,
                range_end: upper,
                fragment: true,
                ..Default::default()
            },
        }
    }

    /// Finalize the current target and add another watch for a single key.
    pub fn key(self, key: impl AsKey) -> Self {
        let key = Bytes::copy_from_slice(key.as_key());
        self._finalize_and_new(key, Bytes::new())
    }

    /// Finalize the current target and add another watch for a prefix.
    pub fn prefix(self, prefix: impl AsKey) -> Self {
        self.range(Prefix(prefix))
    }

    /// Finalize the current target and add another watch for a range.
    pub fn range(self, range: impl AsRange) -> Self {
        let (lower, upper) = range.as_boundaries();
        self._finalize_and_new(lower, upper)
    }
}

/// Attach a client to a clientless watch chain.
impl Watch<WatchBuilder<()>> {
    pub fn with_client(self, client: Client) -> Watch<WatchBuilder<Client>> {
        Watch {
            watcher: self.watcher.with_client(client),
            current: self.current,
        }
    }
}

impl WatchBuilder<Client> {
    /// Create a [`Watcher`] and begin watching for events.
    ///
    /// The gRPC stream is established lazily on the first poll of the returned [`Watcher`]. All
    /// targets accumulated through the builder are sent as `WatchCreateRequest` messages at that
    /// time, followed by any requests already made through the [`Watcher`]. Successful creation
    /// confirmations are consumed internally by the stream; a refusal is yielded as an error (see
    /// [`Watcher::add`]).
    ///
    /// The builder may have zero targets — the resulting [`Watcher`] simply waits for watches to
    /// be added via [`Watcher::add`].
    ///
    /// Because a watch is not active until the stream is first polled, events that occur between
    /// calling `start` and the first poll may be missed. Use
    /// [`start_revision`][Watch::start_revision] to watch from a known point in history if you need
    /// to guarantee delivery.
    ///
    /// The stream carries the client's auth token. See [`Watcher::add`] for what happens when etcd
    /// stops accepting it.
    pub(crate) fn start_client_watch(self) -> Watcher {
        let client = self.client.inner;
        let requests = Arc::new(std::sync::Mutex::new(Requests {
            attempt: None,
            replay: Some(Vec::new()),
        }));
        let sender = WatchSender {
            requests: Arc::downgrade(&requests),
            next_id: AtomicI64::new(1),
        };
        // Watches given to the builder are numbered 1..=N, in order.
        for spec in self.specs {
            sender.add(Watch {
                watcher: (),
                current: spec,
            });
        }

        let inner = async_stream::stream! {
            // Whether this attempt replaced one whose token etcd refused. A second refusal is
            // final.
            let mut refreshed = false;
            'open: loop {
                // Establish the gRPC stream, retrying on Unavailable (the server may not be
                // reachable yet when the channel was created with `connect_lazy`).
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                let (mut response_stream, unanswered_creates, generation) = loop {
                    let receiver = requests.lock().unwrap().new_attempt();
                    let unanswered_creates = Arc::new(std::sync::Mutex::new(VecDeque::new()));
                    let mut request = tonic::Request::new(ReceiverStream {
                        inner: receiver,
                        unanswered_creates: unanswered_creates.clone(),
                    });
                    let generation = client.attach_stream_token(&mut request, deadline).await;
                    let mut watch_client = etcdserverpb::watch_client::WatchClient::new(client.channel.clone());
                    match watch_client.watch(request).await {
                        Ok(resp) => break (resp.into_inner(), unanswered_creates, generation),
                        Err(status)
                            if status.code() == tonic::Code::Unavailable
                                && std::time::Instant::now() <= deadline =>
                        {
                            continue;
                        }
                        Err(status) => {
                            // A consumer may stop polling at this error, so drop `requests` now
                            // rather than at the return.
                            drop(requests);
                            yield Err(WatchError::from_status(status));
                            return;
                        }
                    }
                };

                loop {
                    match response_stream.message().await {
                        Ok(Some(mut resp)) => {
                            if resp.created {
                                // Creates are answered in the order they were sent, and a refusal
                                // carries watch ID -1 instead of the ID it refuses.
                                resp.watch_id = unanswered_creates.lock().unwrap().pop_front().unwrap_or(resp.watch_id);
                                if !refreshed
                                    && ClientInner::is_stale_token_refusal(&resp.cancel_reason)
                                    && requests.lock().unwrap().replay.is_some()
                                    && client
                                        .refresh_auth_token_unserialized(
                                            generation,
                                            Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
                                        )
                                        .await
                                        .is_ok()
                                {
                                    refreshed = true;
                                    continue 'open;
                                }
                                requests.lock().unwrap().replay = None;
                                if !resp.canceled {
                                    continue;
                                }
                            }
                            for item in convert_response(resp) {
                                yield item;
                            }
                        }
                        Ok(None) => return,
                        Err(status) => {
                            drop(requests);
                            yield Err(WatchError::from_status(status));
                            return;
                        }
                    }
                }
            }
        };

        Watcher {
            sender,
            stream: WatchStream { inner: Box::new(inner) },
        }
    }
}

/// `start` on a `Watch` chain forwards to the underlying [`WatchBuilder`].
impl<C: crate::driver::WatchDriver> Watch<WatchBuilder<C>> {
    /// Finalize the current target and create a [`Watcher`].
    ///
    /// See [`WatchBuilder::start`] for details.
    pub fn start(self) -> C::Watcher {
        let Watch { mut watcher, current } = self;
        watcher.specs.push(current);
        watcher.start()
    }
}

/// An active watcher over one or more etcd keys.
///
/// Created by [`Watch::start`]. A `Watcher` combines a [`WatchStream`] (for receiving
/// events) and a [`WatchSender`] (for adding/canceling watches and requesting progress).
///
/// `Watcher` dereferences to [`WatchStream`], so it can be used directly as a [`Stream`]:
///
/// ```no_run
/// use futures::StreamExt;
/// use etcdrs::client::WatchEvent;
/// # async {
/// # let client: etcdrs::Client = todo!();
/// let mut watcher = client.watch()
///     .key("foo")
///     .start();
/// while let Some(event) = watcher.next().await {
///     match event.unwrap() {
///         WatchEvent::Put { record, .. } => println!("put: {record:?}"),
///         WatchEvent::Delete { key, .. } => println!("delete: {key:?}"),
///         WatchEvent::Progress { revision, .. } => println!("progress: {revision:?}"),
///     }
/// }
/// # };
/// ```
///
/// Use [`into_parts`][Self::into_parts] to split the watcher when you need to send control messages
/// concurrently with consuming the stream (e.g. from separate tasks).
///
/// Control messages sent before the stream is first polled are sent once it is established. A watch
/// starts when etcd receives its request, so set [`start_revision`][Watch::start_revision] on a
/// watch that must not miss earlier events.
///
/// ## Cleanup
///
/// Dropping a `Watcher` closes the underlying gRPC stream, which causes the server to cancel
/// all watches associated with it.
pub struct Watcher {
    sender: WatchSender,
    stream: WatchStream,
}

impl Watcher {
    /// Split the `Watcher` into its sender and stream halves.
    ///
    /// This is useful when you need to send control messages (add, cancel, progress) from a
    /// different context than the one consuming events.
    pub fn into_parts(self) -> (WatchSender, WatchStream) {
        (self.sender, self.stream)
    }

    /// Add a watch to this watcher. Returns the assigned [`WatchId`].
    ///
    /// The watch becomes active once the server processes the creation request. Events may not
    /// appear immediately -- the server confirms creation with a response that the stream silently
    /// consumes, then begins delivering events for the new watch.
    ///
    /// If the server refuses to create the watch, the stream yields an error with
    /// [`WatchErrorKind::Canceled`] instead, carrying this [`WatchId`] and the server's
    /// [`cancel_reason`][WatchError::cancel_reason]. etcd refuses a watch whose range is empty or
    /// inverted, whose [`start_revision`][Watch::start_revision] is negative, or whose range the
    /// client may not read. Other watches on this watcher are unaffected.
    ///
    /// etcd also refuses a watch once it stops accepting the auth token the watcher's stream
    /// opened with, for example after the token went unused for five minutes (etcd's default) or
    /// its user's password changed. Until etcd has answered one of the watcher's watches, the
    /// watcher replaces its stream with one that carries a refreshed token, and yields nothing.
    /// After that, replacing the stream would drop the watches etcd created, so the refusal is
    /// yielded like any other, with a reason such as
    /// `rpc error: code = Unauthenticated desc = etcdserver: invalid auth token`, and the watches
    /// already created keep delivering events. To add the watch, start a new [`Watcher`]: it
    /// replaces a stale token as described.
    ///
    /// Construct the [`Watch`] with [`Watch::new`]:
    ///
    /// ```no_run
    /// # use etcdrs::client::{Watch, Watcher};
    /// # let watcher: Watcher = todo!();
    /// let id = watcher.add(Watch::new("key").get_previous());
    /// ```
    pub fn add(&self, watch: Watch) -> WatchId {
        self.sender.add(watch)
    }

    /// Request cancellation of the watch with the given `watch_id`.
    ///
    /// This sends a cancellation request to the server. Because the server processes requests
    /// asynchronously, events for this watch may still arrive after `cancel` returns -- these are
    /// events that were already in-flight before the server processed the cancellation. The stream
    /// will eventually yield an error with [`WatchErrorKind::Canceled`] for this watch, confirming the
    /// cancellation. Other watches on this watcher are unaffected.
    ///
    /// The server ignores a cancellation of a watch it refused to create (see [`add`][Self::add]),
    /// so for such a watch the refusal is the only [`WatchErrorKind::Canceled`] error.
    pub fn cancel(&self, watch_id: WatchId) {
        self.sender.cancel(watch_id);
    }

    /// Request a progress notification from the server.
    ///
    /// The server will respond with a [`WatchEvent::Progress`] containing the current store
    /// revision. This is useful for heartbeat/liveness checks and for learning the current
    /// revision when no events have occurred recently.
    pub fn request_progress(&self) {
        self.sender.request_progress();
    }
}

impl Deref for Watcher {
    type Target = WatchStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl DerefMut for Watcher {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl Stream for Watcher {
    type Item = Result<WatchEvent, WatchError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl std::async_iter::AsyncIterator for Watcher {
    type Item = Result<WatchEvent, WatchError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Stream::poll_next(Pin::new(&mut self.stream), cx)
    }
}

/// The control half of a [`Watcher`], used to add/cancel watches and request progress.
///
/// Obtained via [`Watcher::into_parts`]. All methods send requests through the shared gRPC stream.
/// Because the server processes these asynchronously, there is a window between calling a control
/// method and the server acting on it.
pub struct WatchSender {
    /// `Weak`, so that requests sent after the stream is dropped are not kept.
    requests: Weak<std::sync::Mutex<Requests>>,
    next_id: AtomicI64,
}

impl WatchSender {
    fn send(&self, request: etcdserverpb::WatchRequest) {
        if let Some(requests) = self.requests.upgrade() {
            requests.lock().unwrap().send(request);
        }
    }

    /// Add a watch. Returns the assigned [`WatchId`].
    ///
    /// See [`Watcher::add`] for details.
    pub fn add(&self, watch: Watch) -> WatchId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut request = watch.current;
        request.watch_id = id;
        self.send(etcdserverpb::WatchRequest {
            request_union: Some(PbRequestUnion::CreateRequest(request)),
        });
        WatchId::new(id).expect("watch ID counter overflowed to 0")
    }

    /// Request cancellation of a watch.
    ///
    /// See [`Watcher::cancel`] for details.
    pub fn cancel(&self, watch_id: WatchId) {
        self.send(etcdserverpb::WatchRequest {
            request_union: Some(PbRequestUnion::CancelRequest(etcdserverpb::WatchCancelRequest {
                watch_id: watch_id.get(),
            })),
        });
    }

    /// Request a progress notification.
    ///
    /// See [`Watcher::request_progress`] for details.
    pub fn request_progress(&self) {
        self.send(etcdserverpb::WatchRequest {
            request_union: Some(PbRequestUnion::ProgressRequest(etcdserverpb::WatchProgressRequest {})),
        });
    }
}

/// The event stream half of a [`Watcher`].
///
/// Obtained via [`Watcher::into_parts`]. Implements [`Stream`] — use
/// [`StreamExt::next`][futures_core::Stream] or `for await` to consume events.
pub struct WatchStream {
    inner: Box<dyn Stream<Item = Result<WatchEvent, WatchError>> + Send>,
}

impl WatchStream {
    fn poll_next_impl(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Option<WatchEvent>, WatchError>> {
        let inner = unsafe { self.map_unchecked_mut(|s| s.inner.as_mut()) };
        inner.poll_next(cx).map(|item| match item {
            None => Ok(None),
            Some(Ok(event)) => Ok(Some(event)),
            Some(Err(err)) => Err(err),
        })
    }
}

impl Stream for WatchStream {
    type Item = Result<WatchEvent, WatchError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

#[cfg(feature = "nightly-async-iterator")]
impl std::async_iter::AsyncIterator for WatchStream {
    type Item = Result<WatchEvent, WatchError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx).map(Result::transpose)
    }
}

/// Where a [`WatchSender`] sends requests, shared with the stream.
///
/// Each attempt to establish the stream gets its own channel, because the transport of a failed
/// attempt can keep taking requests from its channel until the server resets the stream.
struct Requests {
    /// The current attempt's channel, or `None` before the first attempt.
    attempt: Option<tokio::sync::mpsc::UnboundedSender<etcdserverpb::WatchRequest>>,
    /// Every request sent so far, oldest first, while the stream may still be replaced: until etcd
    /// accepts a create on it, or refuses one with a refusal the stream yields. A replaced attempt
    /// loses whatever its transport took, so each attempt's channel starts with all of them.
    replay: Option<Vec<etcdserverpb::WatchRequest>>,
}

impl Requests {
    fn send(&mut self, request: etcdserverpb::WatchRequest) {
        if let Some(replay) = &mut self.replay {
            replay.push(request.clone());
        }
        if let Some(attempt) = &self.attempt {
            let _ = attempt.send(request);
        }
    }

    /// Start a channel for a new attempt with every request sent so far, and send later ones to it.
    fn new_attempt(&mut self) -> tokio::sync::mpsc::UnboundedReceiver<etcdserverpb::WatchRequest> {
        let (attempt, receiver) = tokio::sync::mpsc::unbounded_channel();
        for request in self.replay.iter().flatten() {
            let _ = attempt.send(request.clone());
        }
        self.attempt = Some(attempt);
        receiver
    }
}

/// Wraps a [`tokio::sync::mpsc::UnboundedReceiver`] as a [`Stream`], recording the watch ID of each
/// create request as the transport takes it.
struct ReceiverStream {
    inner: tokio::sync::mpsc::UnboundedReceiver<etcdserverpb::WatchRequest>,
    /// The watch IDs of the create requests taken so far that etcd has not answered, oldest first.
    unanswered_creates: Arc<std::sync::Mutex<VecDeque<i64>>>,
}

impl Stream for ReceiverStream {
    type Item = etcdserverpb::WatchRequest;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let polled = self.inner.poll_recv(cx);
        if let Poll::Ready(Some(request)) = &polled
            && let Some(PbRequestUnion::CreateRequest(create)) = &request.request_union
        {
            self.unanswered_creates.lock().unwrap().push_back(create.watch_id);
        }
        polled
    }
}

/// What went wrong with a watch operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchErrorKind {
    /// The watch was compacted — the requested revision is older than the server's compacted
    /// revision.
    Compacted,
    /// The watch was canceled by [`Watcher::cancel`], or the server refused to create it.
    /// [`WatchError::cancel_reason`] carries the server's reason for a refusal.
    Canceled,
    /// The server returned an invalid or unexpected response.
    InvalidResponse,
    /// A gRPC transport or unexpected error.
    Unknown,
}

/// An error from a watch operation.
pub struct WatchError(Box<WatchErrorRepr>);

struct WatchErrorRepr {
    kind: WatchErrorKind,
    watch_id: Option<WatchId>,
    compact_revision: Option<i64>,
    message: Cow<'static, str>,
    status: Option<tonic::Status>,
    backtrace: Backtrace,
}

impl WatchError {
    /// The operation-specific error kind.
    pub fn kind(&self) -> WatchErrorKind {
        self.0.kind
    }

    /// The watch ID associated with this error, if applicable.
    pub fn watch_id(&self) -> Option<WatchId> {
        self.0.watch_id
    }

    /// The compact revision, if this is a [`WatchErrorKind::Compacted`] error.
    pub fn compact_revision(&self) -> Option<i64> {
        self.0.compact_revision
    }

    /// The cancellation reason, if this is a [`WatchErrorKind::Canceled`] error.
    pub fn cancel_reason(&self) -> Option<&str> {
        if self.0.kind == WatchErrorKind::Canceled {
            Some(&self.0.message)
        } else {
            None
        }
    }

    /// The original gRPC status, if this error originated from a gRPC call.
    pub fn grpc_status(&self) -> Option<&tonic::Status> {
        self.0.status.as_ref()
    }

    /// Take ownership of the original gRPC status, if present.
    pub fn take_grpc_status(self) -> Option<tonic::Status> {
        self.0.status
    }

    /// The backtrace captured when this error was created.
    pub fn backtrace(&self) -> &Backtrace {
        &self.0.backtrace
    }

    pub(crate) fn from_status(status: tonic::Status) -> Self {
        Self(Box::new(WatchErrorRepr {
            kind: WatchErrorKind::Unknown,
            watch_id: None,
            compact_revision: None,
            message: Cow::Borrowed(""),
            status: Some(status),
            backtrace: Backtrace::capture(),
        }))
    }

    fn compacted(watch_id: i64, compact_revision: i64) -> Self {
        Self(Box::new(WatchErrorRepr {
            kind: WatchErrorKind::Compacted,
            watch_id: WatchId::new(watch_id),
            compact_revision: Some(compact_revision),
            message: Cow::Owned(format!("watch {watch_id} compacted at revision {compact_revision}")),
            status: None,
            backtrace: Backtrace::capture(),
        }))
    }

    fn canceled(watch_id: i64, reason: String) -> Self {
        Self(Box::new(WatchErrorRepr {
            kind: WatchErrorKind::Canceled,
            watch_id: WatchId::new(watch_id),
            compact_revision: None,
            message: if reason.is_empty() {
                Cow::Borrowed("canceled")
            } else {
                Cow::Owned(reason)
            },
            status: None,
            backtrace: Backtrace::capture(),
        }))
    }

    fn invalid_response(message: &'static str) -> Self {
        Self(Box::new(WatchErrorRepr {
            kind: WatchErrorKind::InvalidResponse,
            watch_id: None,
            compact_revision: None,
            message: Cow::Borrowed(message),
            status: None,
            backtrace: Backtrace::capture(),
        }))
    }
}

impl WatchError {
    fn display_message(&self) -> &str {
        if !self.0.message.is_empty() {
            &self.0.message
        } else if let Some(status) = &self.0.status {
            status.message()
        } else {
            ""
        }
    }
}

impl fmt::Debug for WatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchError")
            .field("kind", &self.0.kind)
            .field("message", &self.display_message())
            .field("watch_id", &self.0.watch_id)
            .finish()
    }
}

impl fmt::Display for WatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.0.kind, self.display_message())
    }
}

impl std::error::Error for WatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.status.as_ref().map(|s| s as _)
    }
}

impl crate::error::OperationError for WatchError {
    fn grpc_status(&self) -> Option<&tonic::Status> {
        self.0.status.as_ref()
    }

    fn backtrace(&self) -> &Backtrace {
        &self.0.backtrace
    }
}

// -------------------------------------------------------------------------------------------------
// Conversion helpers
// -------------------------------------------------------------------------------------------------

/// Build [`Metadata`] from a protobuf [`KeyValue`], tolerating `create_revision = 0` which etcd
/// sends for DELETE tombstones. In that case, `create_revision` is set equal to `mod_revision`.
fn watch_metadata_from_pb(kv: &mvccpb::KeyValue) -> Metadata {
    let create_revision = Revision::new(kv.create_revision)
        .unwrap_or_else(|| Revision::new(kv.mod_revision).expect("mod_revision should be non-zero"));
    Metadata {
        create_revision,
        modified_revision: Revision::new(kv.mod_revision).expect("mod_revision should be non-zero"),
        version: Version::new(kv.version as u64),
        lease: LeaseId::new(kv.lease),
    }
}

/// Like [`record_from_pb`], but tolerates `create_revision = 0` which etcd sends for DELETE
/// tombstones.
fn watch_record_from_pb(kv: mvccpb::KeyValue) -> Record {
    let metadata = watch_metadata_from_pb(&kv);
    Record::new(kv.key, kv.value, metadata)
}

/// Convert a [`WatchResponse`] into an iterator of stream items.
fn convert_response(resp: etcdserverpb::WatchResponse) -> Vec<Result<WatchEvent, WatchError>> {
    if resp.canceled {
        let err = if resp.compact_revision != 0 {
            WatchError::compacted(resp.watch_id, resp.compact_revision)
        } else {
            WatchError::canceled(resp.watch_id, resp.cancel_reason)
        };
        return vec![Err(err)];
    }

    let header = resp.header.map(ResponseHeader::from_pb);

    // Progress notification: no events, not created, not canceled.
    if resp.events.is_empty() {
        if let Some(header) = header {
            return vec![Ok(WatchEvent::Progress {
                revision: header.revision(),
                header,
                watch_id: WatchId::new(resp.watch_id),
            })];
        }
        return vec![];
    }

    let header = header.expect("WatchResponse with events should have a valid header");
    let watch_id = WatchId::new(resp.watch_id).expect("server returned watch_id 0");
    resp.events
        .into_iter()
        .map(|event| {
            let kv = event
                .kv
                .ok_or_else(|| WatchError::invalid_response("watch event missing kv field"))?;

            let prev_record = event.prev_kv.map(record_from_pb);

            if event.r#type == PbEventType::Delete as i32 {
                let metadata = watch_metadata_from_pb(&kv);
                Ok(WatchEvent::Delete {
                    header,
                    watch_id,
                    key: KeyWithMetadata::new(kv.key, metadata),
                    prev_record,
                })
            } else {
                let created = kv.create_revision == kv.mod_revision;
                let record = watch_record_from_pb(kv);
                Ok(WatchEvent::Put {
                    header,
                    watch_id,
                    record,
                    prev_record,
                    created,
                })
            }
        })
        .collect()
}
