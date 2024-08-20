use std::ops::{Deref, DerefMut};

use crate::{LeaseId, Revision, Version};

/// Couples the components of a database entry's three parts: the key, the value, and the associated metadata.
///
/// Monadic operations are provided for all elements. By default, the operand is the value (for example, [`Self::map`]
/// transforms the value). Functions which transform the key and metadata elements have `_key` and `_metadata` in their
/// names (for example, [`Self::map_key`] and [`Self::map_metadata`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Record<K = Vec<u8>, V = Vec<u8>, M = Metadata> {
    metadata: M,
    key: K,
    value: V,
}

impl<K, V, M> Record<K, V, M> {
    pub fn new(key: K, value: V, metadata: M) -> Self {
        Self { metadata, key, value }
    }

    /// Decompose this record into its constituent parts.
    pub fn into_parts(self) -> (M, K, V) {
        (self.metadata, self.key, self.value)
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn key_mut(&mut self) -> &mut K {
        &mut self.key
    }

    pub fn take_key(self) -> K {
        self.key
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut V {
        &mut self.value
    }

    pub fn take_value(self) -> V {
        self.value
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut M {
        &mut self.metadata
    }

    pub fn take_metadata(self) -> M {
        self.metadata
    }

    pub fn without_value(self) -> KeyWithMetadata<K, M> {
        KeyWithMetadata::new(self.key, self.metadata)
    }

    /// Call `f` with the `value` and return a new `Record` with the result of the function call.
    pub fn map<RV>(self, f: impl FnOnce(V) -> RV) -> Record<K, RV, M> {
        Record {
            metadata: self.metadata,
            key: self.key,
            value: f(self.value),
        }
    }

    /// Call `f` with the `value`. If it returns `Ok(v)`, then this returns a new `Record` with that constructed `v`. If
    /// it returns `Err(e)`, then that is returned instead.
    ///
    /// ```
    /// # use etcdrs::record::Record;
    /// let r = Record::new(b"key", b"value", 1);
    /// let Ok(r) = r.map_checked(|v| std::str::from_utf8(v)) else {
    ///     panic!("not UTF-8!");
    /// };
    /// ```
    pub fn map_checked<RV, E>(self, f: impl FnOnce(V) -> Result<RV, E>) -> Result<Record<K, RV, M>, E> {
        Ok(Record {
            metadata: self.metadata,
            key: self.key,
            value: f(self.value)?,
        })
    }

    pub fn map_key<RK>(self, f: impl FnOnce(K) -> RK) -> Record<RK, V, M> {
        Record {
            metadata: self.metadata,
            key: f(self.key),
            value: self.value,
        }
    }

    pub fn map_key_checked<RK, E>(self, f: impl FnOnce(K) -> Result<RK, E>) -> Result<Record<RK, V, M>, E> {
        Ok(Record {
            metadata: self.metadata,
            key: f(self.key)?,
            value: self.value,
        })
    }

    pub fn map_metadata<RM>(self, f: impl FnOnce(M) -> RM) -> Record<K, V, RM> {
        Record {
            metadata: f(self.metadata),
            key: self.key,
            value: self.value,
        }
    }

    pub fn map_metadata_checked<RM, E>(self, f: impl FnOnce(M) -> Result<RM, E>) -> Result<Record<K, V, RM>, E> {
        Ok(Record {
            metadata: f(self.metadata)?,
            key: self.key,
            value: self.value,
        })
    }

    pub fn as_ref(&self) -> Record<&K, &V, &M> {
        Record {
            metadata: &self.metadata,
            key: &self.key,
            value: &self.value,
        }
    }

    pub fn as_mut(&mut self) -> Record<&mut K, &mut V, &mut M> {
        Record {
            metadata: &mut self.metadata,
            key: &mut self.key,
            value: &mut self.value,
        }
    }

    pub fn as_deref(&self) -> Record<&K, &V::Target, &M>
    where
        V: Deref,
    {
        self.as_ref().map(Deref::deref)
    }

    pub fn as_deref_mut(&mut self) -> Record<&mut K, &mut V::Target, &mut M>
    where
        V: DerefMut,
    {
        self.as_mut().map(DerefMut::deref_mut)
    }

    /// Call `f` with this instance, then return the original record. This is useful when doing method chaining.
    ///
    /// ```
    /// # use etcdrs::record::Record;
    /// Record::new("key-1", 5, ()) // <- imagine you got this from a function
    ///     .inspect(|r| println!("got {:?} = {}", r.key(), r.value()))
    ///     .map(|x| x * x)
    ///     // ... and so on ...
    ///     ;
    /// ```
    #[inline]
    pub fn inspect(self, f: impl FnOnce(&Self)) -> Self {
        f(&self);
        self
    }
}

impl<K, V, M, E> Record<K, Result<V, E>, M> {
    /// If you have ended up with a `value` which is a `Result` type, lift the value into the `Record` or return the
    /// error.
    ///
    /// ```
    /// # use etcdrs::record::Record;
    /// let r = Record::new(b"key", b"value", 1);
    /// let Ok(r) = r.map(|v| std::str::from_utf8(v)).transpose() else {
    ///     panic!("not UTF-8!");
    /// };
    /// ```
    pub fn transpose(self) -> Result<Record<K, V, M>, E> {
        Ok(Record {
            metadata: self.metadata,
            key: self.key,
            value: self.value?,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct KeyWithMetadata<K = Vec<u8>, M = Metadata> {
    metadata: M,
    key: K,
}

impl<K, M> KeyWithMetadata<K, M> {
    pub fn new(key: K, metadata: M) -> Self {
        Self { metadata, key }
    }

    /// Decompose this into its constituent parts.
    pub fn into_parts(self) -> (M, K) {
        (self.metadata, self.key)
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn key_mut(&mut self) -> &mut K {
        &mut self.key
    }

    pub fn take_key(self) -> K {
        self.key
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut M {
        &mut self.metadata
    }

    pub fn take_metadata(self) -> M {
        self.metadata
    }

    /// Add the `value` to the metadata contents to make a [`Record`].
    pub fn with_value<V>(self, value: V) -> Record<K, V, M> {
        Record::new(self.key, value, self.metadata)
    }

    pub fn map_key<RK>(self, f: impl FnOnce(K) -> RK) -> KeyWithMetadata<RK, M> {
        KeyWithMetadata {
            metadata: self.metadata,
            key: f(self.key),
        }
    }

    pub fn map_key_checked<RK, E>(self, f: impl FnOnce(K) -> Result<RK, E>) -> Result<KeyWithMetadata<RK, M>, E> {
        Ok(KeyWithMetadata {
            metadata: self.metadata,
            key: f(self.key)?,
        })
    }

    pub fn map_metadata<RM>(self, f: impl FnOnce(M) -> RM) -> KeyWithMetadata<K, RM> {
        KeyWithMetadata {
            metadata: f(self.metadata),
            key: self.key,
        }
    }

    pub fn map_metadata_checked<RM, E>(self, f: impl FnOnce(M) -> Result<RM, E>) -> Result<KeyWithMetadata<K, RM>, E> {
        Ok(KeyWithMetadata {
            metadata: f(self.metadata)?,
            key: self.key,
        })
    }

    pub fn as_ref(&self) -> KeyWithMetadata<&K, &M> {
        KeyWithMetadata {
            metadata: &self.metadata,
            key: &self.key,
        }
    }

    pub fn as_mut(&mut self) -> KeyWithMetadata<&mut K, &mut M> {
        KeyWithMetadata {
            metadata: &mut self.metadata,
            key: &mut self.key,
        }
    }

    /// Call `f` with this instance, then return the original record. This is useful when doing method chaining.
    ///
    /// ```
    /// # use etcdrs::record::KeyWithMetadata;
    /// KeyWithMetadata::new("key-1", 5) // <- imagine you got this from a function
    ///     .inspect(|r| println!("got {:?} metdata={}", r.key(), r.metadata()))
    ///     // ... and so on ...
    ///     ;
    /// ```
    #[inline]
    pub fn inspect(self, f: impl FnOnce(&Self)) -> Self {
        f(&self);
        self
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Metadata {
    /// The revision of the last time this key was created.
    pub create_revision: Revision,

    /// The revision of the last time this key was modified.
    pub modified_revision: Revision,

    /// The version of this record. Each modification to the record increments the version. It is reset to `0` only when
    /// it has been deleted and created again.
    pub version: Version,

    /// If the record has a lease, it will be automatically deleted when that lease expires. A value of `None` means the
    /// record is not ephemeral (there is no lease attached to the record).
    pub lease: Option<LeaseId>,
}

/// Convert the source into a database-encoded key.
pub trait AsKey {
    fn as_key(&self) -> &[u8];
}

/// Convert the source into a database-encoded value.
pub trait AsValue {
    fn as_value(&self) -> &[u8];
}

macro_rules! impl_basic_as {
    ($trait_name:ident, $fn_name:ident) => {
        impl $trait_name for &[u8] {
            fn $fn_name(&self) -> &[u8] {
                self
            }
        }

        impl<const N: usize> $trait_name for &[u8; N] {
            fn $fn_name(&self) -> &[u8] {
                &self[..]
            }
        }

        impl $trait_name for &mut [u8] {
            fn $fn_name(&self) -> &[u8] {
                self
            }
        }

        impl<const N: usize> $trait_name for &mut [u8; N] {
            fn $fn_name(&self) -> &[u8] {
                &self[..]
            }
        }

        impl $trait_name for Vec<u8> {
            fn $fn_name(&self) -> &[u8] {
                &self
            }
        }

        impl $trait_name for &str {
            fn $fn_name(&self) -> &[u8] {
                self.as_bytes()
            }
        }

        impl $trait_name for &mut str {
            fn $fn_name(&self) -> &[u8] {
                self.as_bytes()
            }
        }

        impl $trait_name for String {
            fn $fn_name(&self) -> &[u8] {
                self.as_bytes()
            }
        }

        impl<T: $trait_name + ?Sized> $trait_name for Box<T> {
            fn $fn_name(&self) -> &[u8] {
                T::$fn_name(&**self)
            }
        }

        impl<T: $trait_name + ?Sized> $trait_name for std::rc::Rc<T> {
            fn $fn_name(&self) -> &[u8] {
                T::$fn_name(&**self)
            }
        }

        impl<T: $trait_name + ?Sized> $trait_name for std::sync::Arc<T> {
            fn $fn_name(&self) -> &[u8] {
                T::$fn_name(&**self)
            }
        }
    };
}

impl_basic_as!(AsKey, as_key);
impl_basic_as!(AsValue, as_value);
