use std::num::{NonZeroI64, NonZeroU64};

pub mod client;
pub mod error;
pub mod fake;
mod pb;
pub mod record;

pub use client::Client;
pub use error::{Error, ErrorKind};

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

    pub(crate) fn next(&mut self) {
        self.0 = NonZeroI64::new(self.0.get() + 1).unwrap();
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(transparent)]
pub struct Version(u64);

impl Version {
    pub fn new(source: u64) -> Self {
        Self(source)
    }

    pub(crate) fn next(&mut self) {
        self.0 += 1;
    }
}
