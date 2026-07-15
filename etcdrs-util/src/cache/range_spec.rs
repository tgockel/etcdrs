use std::borrow::Cow;
use std::ops::Bound;

use bytes::Bytes;
use etcdrs::client::{List, Watch};
use etcdrs::{AsRange, Record, Revision, TargetRange};

/// A cached key range, decoded from the wire encoding produced by [`AsRange`].
///
/// This is an owned analogue of [`TargetRange`]: it captures the `(key, range_end)` boundary bytes
/// of a range expression so the cache configuration can store them and answer containment
/// questions about keys and other ranges.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RangeSpec {
    /// Every key in the store.
    All,
    /// A single key.
    Single(Bytes),
    /// The half-open span `[lower, upper)`.
    Span(Bytes, Bytes),
    /// Every key `>= lower`.
    From(Bytes),
}

impl RangeSpec {
    /// Decode a range expression into a spec.
    pub(crate) fn from_range(range: impl AsRange) -> Self {
        let (lower, upper) = range.as_boundaries();
        match TargetRange::from_wire(&lower, &upper) {
            TargetRange::All => RangeSpec::All,
            TargetRange::Single(_) => RangeSpec::Single(lower),
            TargetRange::ToEnd(_) => RangeSpec::From(lower),
            TargetRange::Span(..) => RangeSpec::Span(lower, upper),
        }
    }

    /// Decode a borrowed [`TargetRange`] into an owned spec.
    pub(crate) fn from_target(target: TargetRange<'_>) -> Self {
        match target {
            TargetRange::All => RangeSpec::All,
            TargetRange::Single(key) => RangeSpec::Single(Bytes::copy_from_slice(key)),
            TargetRange::Span(lower, upper) => {
                RangeSpec::Span(Bytes::copy_from_slice(lower), Bytes::copy_from_slice(upper))
            }
            TargetRange::ToEnd(lower) => RangeSpec::From(Bytes::copy_from_slice(lower)),
        }
    }

    /// Whether `key` falls within this range.
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        match self {
            RangeSpec::All => true,
            RangeSpec::Single(single) => key == single,
            RangeSpec::Span(lower, upper) => key >= lower.as_ref() && key < upper.as_ref(),
            RangeSpec::From(lower) => key >= lower.as_ref(),
        }
    }

    /// Whether `other` is entirely contained within this range.
    pub(crate) fn contains_span(&self, other: &RangeSpec) -> bool {
        self.lower() <= other.lower()
            && match (self.upper(), other.upper()) {
                (None, _) => true,
                (Some(_), None) => false,
                (Some(outer), Some(inner)) => inner <= outer,
            }
    }

    /// The inclusive lower bound.
    ///
    /// [`All`][RangeSpec::All] uses `[0x00]`, the smallest key etcd accepts (empty keys are
    /// rejected by the server).
    fn lower(&self) -> &[u8] {
        match self {
            RangeSpec::All => &[0],
            RangeSpec::Single(key) | RangeSpec::Span(key, _) | RangeSpec::From(key) => key.as_ref(),
        }
    }

    /// The exclusive upper bound, or `None` if unbounded.
    ///
    /// A single key `k` covers exactly the span `[k, k + "\0")`.
    fn upper(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            RangeSpec::All | RangeSpec::From(_) => None,
            RangeSpec::Single(key) => Some(Cow::Owned(successor(key))),
            RangeSpec::Span(_, upper) => Some(Cow::Borrowed(upper.as_ref())),
        }
    }

    /// The bounds of this range for scanning an ordered map keyed by `Bytes`.
    ///
    /// A degenerate span (`lower >= upper`) is normalized to an empty range so it is safe to pass
    /// to [`BTreeMap::range`][std::collections::BTreeMap::range], which panics on inverted bounds.
    pub(crate) fn as_bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        match self {
            RangeSpec::All => (Bound::Unbounded, Bound::Unbounded),
            RangeSpec::Single(key) => (Bound::Included(key.as_ref()), Bound::Included(key.as_ref())),
            RangeSpec::Span(lower, upper) if lower >= upper => {
                (Bound::Included(lower.as_ref()), Bound::Excluded(lower.as_ref()))
            }
            RangeSpec::Span(lower, upper) => (Bound::Included(lower.as_ref()), Bound::Excluded(upper.as_ref())),
            RangeSpec::From(lower) => (Bound::Included(lower.as_ref()), Bound::Unbounded),
        }
    }

    /// A detached [`List`] operation covering exactly this range.
    pub(crate) fn seed_list(&self) -> List<(), Record> {
        match self {
            RangeSpec::All => List::new(..),
            RangeSpec::Single(key) => List::new(key.clone()),
            RangeSpec::Span(lower, upper) => List::new(lower.clone()..upper.clone()),
            RangeSpec::From(lower) => List::new(lower.clone()..),
        }
    }

    /// A watch spec covering exactly this range, delivering events at or after `start` and
    /// requesting server progress notifications.
    pub(crate) fn watch(&self, start: Revision) -> Watch {
        let watch = match self {
            RangeSpec::All => Watch::new(..),
            RangeSpec::Single(key) => Watch::new(key.clone()),
            RangeSpec::Span(lower, upper) => Watch::new(lower.clone()..upper.clone()),
            RangeSpec::From(lower) => Watch::new(lower.clone()..),
        };
        watch.start_revision(start).progress_notify()
    }
}

/// The smallest key greater than `key` in byte-string order (`key` + `0x00`).
fn successor(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1);
    out.extend_from_slice(key);
    out.push(0);
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use etcdrs::{AsRange, Prefix, TargetRange};

    use super::RangeSpec;

    fn spec(range: impl AsRange) -> RangeSpec {
        RangeSpec::from_range(range)
    }

    fn bytes(source: &str) -> Bytes {
        Bytes::copy_from_slice(source.as_bytes())
    }

    #[test]
    fn from_range_decodes_shapes() {
        assert_eq!(spec(..), RangeSpec::All);
        assert_eq!(spec("foo"), RangeSpec::Single(bytes("foo")));
        assert_eq!(spec("a".."b"), RangeSpec::Span(bytes("a"), bytes("b")));
        assert_eq!(spec("a"..), RangeSpec::From(bytes("a")));
        assert_eq!(spec(Prefix("foo/")), RangeSpec::Span(bytes("foo/"), bytes("foo0")));
    }

    #[test]
    fn contains_key_all() {
        assert!(spec(..).contains_key(b"\0"));
        assert!(spec(..).contains_key(b"anything"));
    }

    #[test]
    fn contains_key_single() {
        let single = spec("foo");
        assert!(single.contains_key(b"foo"));
        assert!(!single.contains_key(b"fo"));
        assert!(!single.contains_key(b"foo\0"));
        assert!(!single.contains_key(b"foob"));
    }

    #[test]
    fn contains_key_span_is_half_open() {
        let span = spec("a".."m");
        assert!(span.contains_key(b"a"));
        assert!(span.contains_key(b"a\0"));
        assert!(span.contains_key(b"lzzz"));
        assert!(!span.contains_key(b"m"));
        assert!(!span.contains_key(b"A"));
    }

    #[test]
    fn contains_key_prefix_edges() {
        let prefix = spec(Prefix("foo/"));
        assert!(prefix.contains_key(b"foo/"));
        assert!(prefix.contains_key(b"foo/a"));
        assert!(prefix.contains_key(b"foo/\xff\xff"));
        assert!(!prefix.contains_key(b"foo0"));
        assert!(!prefix.contains_key(b"foo"));
    }

    #[test]
    fn contains_key_from() {
        let from = spec("m"..);
        assert!(from.contains_key(b"m"));
        assert!(from.contains_key(b"zzz"));
        assert!(!from.contains_key(b"lzzz"));
    }

    #[test]
    fn contains_span_all_contains_everything() {
        let all = spec(..);
        assert!(all.contains_span(&spec(..)));
        assert!(all.contains_span(&spec("foo")));
        assert!(all.contains_span(&spec("a".."b")));
        assert!(all.contains_span(&spec("a"..)));
    }

    #[test]
    fn contains_span_bounded_never_contains_unbounded() {
        assert!(!spec("a".."z").contains_span(&spec(..)));
        assert!(!spec("a".."z").contains_span(&spec("a"..)));
        assert!(!spec("foo").contains_span(&spec(..)));
    }

    #[test]
    fn contains_span_from() {
        let from = spec("a"..);
        assert!(from.contains_span(&spec("b"..)));
        assert!(from.contains_span(&spec("b".."x")));
        assert!(from.contains_span(&spec("a")));
        assert!(!from.contains_span(&spec(..)));
        assert!(!spec("b"..).contains_span(&spec("a"..)));
    }

    #[test]
    fn contains_span_prefix() {
        let prefix = spec(Prefix("foo/"));
        assert!(prefix.contains_span(&spec(Prefix("foo/"))));
        assert!(prefix.contains_span(&spec(Prefix("foo/bar/"))));
        assert!(prefix.contains_span(&spec("foo/a")));
        assert!(prefix.contains_span(&spec("foo/a".."foo/m")));
        assert!(!prefix.contains_span(&spec("foo/a".."foo1")));
        assert!(!prefix.contains_span(&spec("fon".."foo/m")));
        assert!(!prefix.contains_span(&spec("foo")));
        assert!(!prefix.contains_span(&spec("foo0")));
    }

    #[test]
    fn contains_span_single() {
        let single = spec("foo");
        assert!(single.contains_span(&spec("foo")));
        assert!(!single.contains_span(&spec("foo\0")));
        assert!(!single.contains_span(&spec("fo".."fop")));
    }

    #[test]
    fn contains_span_upper_boundary_exact() {
        let span = spec("a".."m");
        assert!(span.contains_span(&spec("a".."m")));
        assert!(!span.contains_span(&spec("a".."n")));
        // successor("m") = "m\0" lies just past the exclusive upper bound.
        assert!(!span.contains_span(&spec("m")));
        assert!(span.contains_span(&spec("lzz")));
    }

    #[test]
    fn as_bounds_scans_ordered_map() {
        let mut map = BTreeMap::new();
        for key in ["a", "foo/", "foo/a", "foo/b", "foo0", "m", "z"] {
            map.insert(bytes(key), ());
        }
        let scan = |spec: &RangeSpec| -> Vec<&str> {
            map.range::<[u8], _>(spec.as_bounds())
                .map(|(k, ())| std::str::from_utf8(k).unwrap())
                .collect()
        };

        assert_eq!(scan(&spec(..)), vec!["a", "foo/", "foo/a", "foo/b", "foo0", "m", "z"]);
        assert_eq!(scan(&spec("foo/a")), vec!["foo/a"]);
        assert_eq!(scan(&spec(Prefix("foo/"))), vec!["foo/", "foo/a", "foo/b"]);
        assert_eq!(scan(&spec("foo/a".."m")), vec!["foo/a", "foo/b", "foo0"]);
        assert_eq!(scan(&spec("m"..)), vec!["m", "z"]);
        // Degenerate spans normalize to an empty scan instead of panicking.
        assert_eq!(scan(&spec("m".."a")), Vec::<&str>::new());
    }

    #[test]
    fn seed_list_round_trips_target_range() {
        assert_eq!(spec(..).seed_list().target_range(), TargetRange::All);
        assert_eq!(spec("foo").seed_list().target_range(), TargetRange::Single(b"foo"));
        assert_eq!(
            spec(Prefix("foo/")).seed_list().target_range(),
            TargetRange::Span(b"foo/", b"foo0"),
        );
        assert_eq!(spec("a"..).seed_list().target_range(), TargetRange::ToEnd(b"a"));
    }
}
