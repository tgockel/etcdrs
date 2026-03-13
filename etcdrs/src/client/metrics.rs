use std::{
    fmt,
    sync::{atomic, Arc},
    time::Duration,
};

/// Attached to [`Client`][`crate::Client`]s to log metrics about requests.
pub trait MetricsCollector: Send + Sync {
    /// Called just before a request is about to be dispatched.
    fn request_start(&self);

    /// Called just after a request has completed, either in success or failure.
    fn request_end(&self, duration: Duration, success: bool);
}

/// A bit of a hacky RAII mechanism to track metrics. If this ever gets exposed, it will need some clean up.
pub(crate) struct MetricsSpan<'a, C: MetricsCollector> {
    inner: Option<(&'a C, std::time::Instant)>,
}

impl<'a, C: MetricsCollector> MetricsSpan<'a, C> {
    pub fn new(collector: &'a Option<C>) -> Self {
        if let Some(collector) = collector {
            collector.request_start();
            Self {
                inner: Some((collector, std::time::Instant::now())),
            }
        } else {
            Self { inner: None }
        }
    }

    #[inline]
    pub fn complete(mut self, success: bool) {
        self.complete_impl(success)
    }

    fn complete_impl(&mut self, success: bool) {
        if let Some((collector, start)) = self.inner.take() {
            let duration = std::time::Instant::now() - start;
            collector.request_end(duration, success);
        }
    }
}

impl<'a, C: MetricsCollector> Drop for MetricsSpan<'a, C> {
    fn drop(&mut self) {
        self.complete_impl(false);
    }
}

impl<T: MetricsCollector> MetricsCollector for Box<T> {
    fn request_start(&self) {
        self.as_ref().request_start()
    }

    fn request_end(&self, duration: Duration, success: bool) {
        self.as_ref().request_end(duration, success)
    }
}

impl MetricsCollector for Box<dyn MetricsCollector> {
    fn request_start(&self) {
        self.as_ref().request_start()
    }

    fn request_end(&self, duration: Duration, success: bool) {
        self.as_ref().request_end(duration, success)
    }
}

impl<T: MetricsCollector> MetricsCollector for Arc<T> {
    fn request_start(&self) {
        self.as_ref().request_start()
    }

    fn request_end(&self, duration: Duration, success: bool) {
        self.as_ref().request_end(duration, success)
    }
}

/// A [`MetricsCollector`] which counts requests, their successes, and their failures.
#[derive(Default)]
pub struct RequestCounter {
    requested: atomic::AtomicUsize,
    respond_success: atomic::AtomicUsize,
    respond_failure: atomic::AtomicUsize,
}

impl MetricsCollector for RequestCounter {
    fn request_start(&self) {
        self.requested.fetch_add(1, atomic::Ordering::Relaxed);
    }

    fn request_end(&self, _duration: Duration, success: bool) {
        let respond = if success {
            &self.respond_success
        } else {
            &self.respond_failure
        };
        respond.fetch_add(1, atomic::Ordering::Relaxed);
    }
}

impl RequestCounter {
    /// Get the current snapshot of collected metrics.
    pub fn get(&self) -> RequestCount {
        let mut out = RequestCount {
            requested: self.requested.load(atomic::Ordering::Relaxed),
            success: self.respond_success.load(atomic::Ordering::Relaxed),
            failure: self.respond_failure.load(atomic::Ordering::Relaxed),
        };
        if out.requested < out.success + out.failure {
            // If there were fewer requested than success and failures, our atomic loads happened between multiple
            // requests starting and then completing. So just bring requested up to speed.
            out.requested = out.success + out.failure;
        }
        out
    }
}

#[derive(Clone, Copy, Default)]
pub struct RequestCount {
    requested: usize,
    success: usize,
    failure: usize,
}

impl RequestCount {
    #[inline]
    pub fn requested(&self) -> usize {
        self.requested
    }

    #[inline]
    pub fn succeeded(&self) -> usize {
        self.success
    }

    #[inline]
    pub fn failed(&self) -> usize {
        self.failure
    }

    #[inline]
    pub fn completed(&self) -> usize {
        self.succeeded() + self.failed()
    }

    #[inline]
    pub fn outstanding(&self) -> usize {
        self.requested() - self.completed()
    }
}

impl fmt::Display for RequestCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{success}/{completed} ({rate:.2}% success, {failure} failure{failure_s}) outstanding: {outstanding}",
            success = self.succeeded(),
            completed = self.completed(),
            rate = (self.succeeded() as f64) / (self.completed() as f64) * 100.0,
            failure = self.failed(),
            failure_s = if self.failed() == 1 { "" } else { "s" },
            outstanding = self.outstanding()
        )
    }
}

impl fmt::Debug for RequestCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RequestCount")
            .field("requested", &self.requested())
            .field("completed", &self.completed())
            .field("succeeded", &self.succeeded())
            .field("failed", &self.failed())
            .field("outstanding", &self.outstanding())
            .finish()
    }
}
