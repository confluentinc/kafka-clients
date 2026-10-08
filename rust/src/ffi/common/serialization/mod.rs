// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `kafka_common_serialization_*`: `org.apache.kafka.common.serialization`
//! (CLAUDE.md §4).
//!
//! The `Serializer` and `Deserializer` interfaces are generic in Java; their
//! `T` crosses the boundary as a `void *` (§4, "Generic types"), which Rust
//! holds as a [`GenericValue`] and never reads. What the pointer means is the
//! business of the implementation on each side:
//!
//!   - a C implementation registered with `Serializer_new` /
//!     `Deserializer_new` decides it, and a `void *` a C deserializer
//!     produces is owned by the C side, never freed by Rust;
//!   - each built-in class documents its representation: `StringSerializer`
//!     reads a NUL-terminated `const char *`, `ByteArraySerializer` a
//!     `const kafka_Bytes_t *`; `StringDeserializer` returns an owned `char *`
//!     freed with `kafka_string_destroy`, `ByteArrayDeserializer` and
//!     `BytesDeserializer` an owned `kafka_Bytes_t *` freed with
//!     `kafka_Bytes_destroy`.
//!
//! Every built-in class is a handle ([`SerializerHandle`] /
//! [`DeserializerHandle`]) that hands out a borrowed view of its interface
//! through `__as_Serializer` / `__as_Deserializer` (§4, "Traits"), valid as
//! long as the class handle and never passed to the interface's `_destroy`.
//! Passing the view where a client consumes a `*mut` interface handle shares
//! the implementation and leaves the class handle valid.
//!
//! `Deserializer` has `&mut self` methods (`configure`, `close`), so its
//! handle guards the implementation with a mutex and `__as_Deserializer`
//! returns `*mut`; `Serializer` has none and its view is `*const`.

use std::sync::{Arc, Mutex, OnceLock};

use crate::common::serialization::{Deserializer, Serializer};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::serialization::deserializer::{SharedDeserializer, kafka_common_serialization_Deserializer_t};
use crate::ffi::common::serialization::serializer::{
    DynSerializer, SerializerInner, kafka_common_serialization_Serializer_t,
};
use crate::ffi::util::GenericValue;

pub(crate) mod byte_array_deserializer;
pub(crate) mod byte_array_serializer;
pub(crate) mod bytes_deserializer;
pub(crate) mod deserializer;
pub(crate) mod serializer;
pub(crate) mod string_deserializer;
pub(crate) mod string_serializer;

/// What a serializer class handle points at: the implementation over the
/// `void *` representation, shared with every client it was handed to, plus
/// the `Serializer` view it hands out.
pub(crate) struct SerializerHandle<T> {
    imp: Arc<T>,
    as_serializer: OnceLock<SerializerInner>,
}

impl<T: Serializer<GenericValue> + Send + Sync + 'static> SerializerHandle<T> {
    /// Hands `imp` to C as an owned handle.
    pub(crate) fn boxed(imp: T) -> *mut Self {
        Box::into_raw(Box::new(Self { imp: Arc::new(imp), as_serializer: OnceLock::new() }))
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this class.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        unsafe { &*ptr }
    }

    /// The implementation as a `Serializer`: a view valid as long as the
    /// handle.
    pub(crate) fn as_serializer(&self) -> *const kafka_common_serialization_Serializer_t {
        self.as_serializer
            .get_or_init(|| SerializerInner::view(Arc::clone(&self.imp) as Arc<DynSerializer>))
            .as_ptr()
    }

    /// Frees an owned handle; null is a no-op.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

/// What a deserializer class handle points at: the implementation over the
/// `void *` representation behind the mutex its `&mut self` methods need,
/// shared with every client it was handed to, plus the `Deserializer` view
/// it hands out.
pub(crate) struct DeserializerHandle<T> {
    imp: Arc<Mutex<T>>,
    as_deserializer: OnceLock<Interface<SharedDeserializer>>,
}

impl<T: Deserializer<GenericValue>> DeserializerHandle<T> {
    /// Hands `imp` to C as an owned handle.
    pub(crate) fn boxed(imp: T) -> *mut Self {
        Box::into_raw(Box::new(Self {
            imp: Arc::new(Mutex::new(imp)),
            as_deserializer: OnceLock::new(),
        }))
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this class.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        unsafe { &*ptr }
    }

    /// The implementation as a `Deserializer`: a view valid as long as the
    /// handle, `*mut` because the trait mutates. The view itself is never
    /// written through: the invokers lock the mutex behind it.
    pub(crate) fn as_deserializer(&self) -> *mut kafka_common_serialization_Deserializer_t {
        let view = self
            .as_deserializer
            .get_or_init(|| Interface::view(Arc::clone(&self.imp) as Arc<SharedDeserializer>));
        view as *const Interface<SharedDeserializer> as *mut kafka_common_serialization_Deserializer_t
    }

    /// Frees an owned handle; null is a no-op.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::serialization::deserializer::{deserializer_ref, take_deserializer};
    use crate::ffi::common::serialization::serializer::{serializer_ref, take_serializer};
    use crate::ffi::common::serialization::string_deserializer::GenericStringDeserializer;
    use crate::ffi::common::serialization::string_serializer::GenericStringSerializer;

    #[test]
    fn serializer_views_are_cached_and_share_the_implementation() {
        let handle = SerializerHandle::boxed(GenericStringSerializer::default());
        unsafe {
            let h = SerializerHandle::from_ptr(handle);
            assert_eq!(h.as_serializer(), h.as_serializer());
            assert_eq!(Arc::strong_count(&h.imp), 2, "handle + view");
            // Taking the view shares the implementation: the handle keeps
            // working and the taken `Arc` is the same allocation.
            let shared = take_serializer(h.as_serializer() as *mut _);
            assert_eq!(Arc::strong_count(&h.imp), 3);
            assert!(ptr::addr_eq(Arc::as_ptr(&shared), Arc::as_ptr(&h.imp)));
            assert_eq!(serializer_ref(h.as_serializer()).as_ptr(), h.as_serializer());
            drop(shared);
            SerializerHandle::destroy(handle);
            SerializerHandle::<GenericStringSerializer>::destroy(ptr::null_mut());
        }
    }

    #[test]
    fn deserializer_views_are_cached_and_share_the_implementation() {
        let handle = DeserializerHandle::boxed(GenericStringDeserializer::default());
        unsafe {
            let h = DeserializerHandle::from_ptr(handle);
            assert_eq!(h.as_deserializer(), h.as_deserializer());
            assert_eq!(Arc::strong_count(&h.imp), 2, "handle + view");
            let shared = take_deserializer(h.as_deserializer());
            assert_eq!(Arc::strong_count(&h.imp), 3);
            assert!(ptr::addr_eq(Arc::as_ptr(&shared), Arc::as_ptr(&h.imp)));
            assert!(ptr::addr_eq(deserializer_ref(h.as_deserializer()), Arc::as_ptr(&h.imp)));
            drop(shared);
            DeserializerHandle::destroy(handle);
            DeserializerHandle::<GenericStringDeserializer>::destroy(ptr::null_mut());
        }
    }
}
