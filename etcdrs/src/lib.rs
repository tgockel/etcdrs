#![cfg_attr(feature = "nightly-async-iterator", feature(async_iterator))]

use std::num::{NonZeroI64, NonZeroU64};

pub mod client;
pub mod error;
pub(crate) mod pb;
pub(crate) mod range;
pub mod record;

pub use client::{Client, WatchId};
pub use error::{Error, ErrorKind};
pub use range::{AsRange, Prefix};
pub use record::{KeyWithMetadata, Metadata, Record};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct ClusterId(NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct MemberId(NonZeroU64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct LeaseId(NonZeroI64);

impl LeaseId {
    pub fn new(source: i64) -> Option<Self> {
        NonZeroI64::new(source).map(Self)
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Term(NonZeroU64);

impl Term {
    pub fn new(source: u64) -> Option<Self> {
        NonZeroU64::new(source).map(Self)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(transparent)]
pub struct Revision(NonZeroI64);

impl Revision {
    pub fn new(source: i64) -> Option<Self> {
        NonZeroI64::new(source).map(Self)
    }

    pub fn get(&self) -> i64 {
        self.0.get()
    }
}

/// A monotonically increasing version number.
///
/// This is used to track the [`version`][record::Metadata::version] of a [`Record`]. Each time the record is modified,
/// the version is incremented. A newly-created record has the version `1`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(transparent)]
pub struct Version(u64);

impl Version {
    pub const fn new(source: u64) -> Self {
        Self(source)
    }
}
