use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use crate::record::AsKey;

mod private {
    pub trait Sealed {}
}

/// Convert the source into a range query.
///
/// This trait converts range expressions like `"a"..`, `"a".."b"`, `..`, and so on into the database encoded boundaries
/// for a [`list`][crate::Client::list] query. It is implemented for all range expressions that can be converted via
/// [`AsKey`]. There is also a [`Prefix`] newtype that specifies a prefix query.
pub trait AsRange: private::Sealed {
    /// Convert the range into the database encoded boundaries.
    ///
    /// These are used in the `key` and `range_end` fields of a `RangeRequest` Protobuf message. For reference, this is
    /// the documentation from the `.proto` files:
    ///
    /// **key**
    /// > key is the first key for the range. If range_end is not given, the request only looks up key.
    ///
    /// **range_end**
    /// > range_end is the upper bound on the requested range [key, range_end).
    /// > If range_end is '\0', the range is all keys >= key.
    /// > If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"),
    /// > then the range request gets all keys prefixed with key.
    /// > If both key and range_end are '\0', then the range request returns all keys.
    #[doc(hidden)]
    fn as_boundaries(&self) -> (Bytes, Bytes);
}

fn specify_boundaries(start_bound: Bound<&[u8]>, end_bound: Bound<&[u8]>) -> (Bytes, Bytes) {
    let lower = match start_bound {
        Bound::Included(val) => Bytes::copy_from_slice(val.as_key()),
        Bound::Excluded(val) => successor(val.as_key()).into(),
        Bound::Unbounded => Bytes::from_static(&[0]),
    };
    let upper = match end_bound {
        Bound::Included(val) => add_one(val.as_key()).into(),
        Bound::Excluded(val) => Bytes::copy_from_slice(val.as_key()),
        Bound::Unbounded => Bytes::from_static(&[0]),
    };
    (lower, upper)
}

fn range_to_boundaries<R: RangeBounds<impl AsKey>>(range: &R) -> (Bytes, Bytes) {
    specify_boundaries(
        range.start_bound().map(AsKey::as_key),
        range.end_bound().map(AsKey::as_key),
    )
}

macro_rules! impl_as_range_for_range_type {
    ($template:ident) => {
        impl<T: AsKey> private::Sealed for std::ops::$template<T> {}
        impl<T: AsKey> AsRange for std::ops::$template<T> {
            fn as_boundaries(&self) -> (Bytes, Bytes) {
                range_to_boundaries(self)
            }
        }
    };
    ($($template:ident),* $(,)?) => {
        $(impl_as_range_for_range_type!($template);)*
    }
}

impl_as_range_for_range_type! {
    Range,
    RangeInclusive,
    RangeFrom,
    RangeTo,
    RangeToInclusive,
}

impl private::Sealed for std::ops::RangeFull {}
impl AsRange for std::ops::RangeFull {
    fn as_boundaries(&self) -> (Bytes, Bytes) {
        (Bytes::from_static(&[0]), Bytes::from_static(&[0]))
    }
}

/// Specifies a prefix query in [`AsRange`].
#[derive(Debug, Clone, Copy)]
pub struct Prefix<T: ?Sized>(pub T);

impl<T: AsKey + ?Sized> private::Sealed for Prefix<T> {}
impl<T: AsKey + ?Sized> AsRange for Prefix<T> {
    fn as_boundaries(&self) -> (Bytes, Bytes) {
        (Bytes::copy_from_slice(self.0.as_key()), add_one(self.0.as_key()).into())
    }
}

impl<T: AsKey + ?Sized> private::Sealed for T {}
impl<T: AsKey + ?Sized> AsRange for T {
    fn as_boundaries(&self) -> (Bytes, Bytes) {
        (Bytes::copy_from_slice(self.as_key()), Bytes::new())
    }
}

/// A decoded view of an etcd range request's `(key, range_end)` bytes.
///
/// This is the inverse of [`AsRange::as_boundaries`]: given the wire bytes stored in a
/// [`RangeRequest`][crate::pb::etcdserverpb::RangeRequest] (or any operation that carries the same
/// encoding, such as a [`Delete`][crate::client::Delete]), it tells you what the operation
/// addresses in human terms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetRange<'a> {
    /// Every key in the store (wire: `key = [0]`, `range_end = [0]`).
    All,
    /// A single key lookup (wire: `key = k`, `range_end = ""`).
    Single(&'a [u8]),
    /// The range `[lower, upper)` with an explicit upper bound (wire: both non-empty and
    /// `range_end != [0]`).
    Span(&'a [u8], &'a [u8]),
    /// Every key `>= lower` (wire: `key = k`, `range_end = [0]`, `k != [0]`).
    ToEnd(&'a [u8]),
}

impl<'a> TargetRange<'a> {
    /// Decode the wire representation of `(key, range_end)` used by etcd range-bearing
    /// operations.
    pub fn from_wire(key: &'a [u8], range_end: &'a [u8]) -> Self {
        match (key, range_end) {
            ([0], [0]) => TargetRange::All,
            (k, []) => TargetRange::Single(k),
            (k, [0]) => TargetRange::ToEnd(k),
            (k, e) => TargetRange::Span(k, e),
        }
    }
}

/// Add one bit to the last element of `input`, carrying left on overflow.
///
/// This is used in range queries to specify "include this `input`".
fn add_one(input: &[u8]) -> Vec<u8> {
    let mut out = input.to_owned();
    while let Some(last) = out.last_mut() {
        if *last < u8::MAX {
            *last += 1;
            break;
        } else {
            out.pop();
        }
    }
    // special case -- input was a string of 0xff, so put in a 0x00 which means to get until the end of the database
    if out.is_empty() {
        out.push(0);
    }
    out
}

/// Get the "next" key that follows `input`.
pub(crate) fn successor(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + 1);
    input.clone_into(&mut out);
    out.push(0);
    out
}

#[cfg(test)]
mod tests {
    use super::{TargetRange, add_one};

    #[test]
    fn target_range_from_wire() {
        assert_eq!(TargetRange::from_wire(&[0], &[0]), TargetRange::All);
        assert_eq!(TargetRange::from_wire(b"foo", b""), TargetRange::Single(b"foo"));
        assert_eq!(TargetRange::from_wire(b"foo", &[0]), TargetRange::ToEnd(b"foo"));
        assert_eq!(
            TargetRange::from_wire(b"foo/", b"foo0"),
            TargetRange::Span(b"foo/", b"foo0"),
        );
    }

    #[test]
    fn test_add_one() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"aa", b"ab"),
            (b"a\xff", b"b"),
            (b"\xff", b"\0"),
            (b"\xff\xff\xff", b"\0"),
        ];
        for (input, expected) in cases {
            let output = add_one(input);
            assert_eq!(*expected, output);
        }
    }
}
