// Copyright 2026 Confluent Inc.
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

//! The dyn-compatible view of [`Deserializer`].
//!
//! Has no Java counterpart (`definition-of-done.md` §7). A Java interface is
//! always usable as a runtime-polymorphic type, so a consumer holds its
//! `Deserializer<K>` whatever its class. The Rust [`Deserializer`] trait
//! returns `impl Future` from its methods, which keeps a statically dispatched
//! call free of a boxed future but makes the trait itself not dyn-compatible.
//! `DynDeserializer` restores the Java capability (CLAUDE.md §3, "traits must
//! be dyn-compatible") the way `DynProducer` does for `Producer`, except that
//! its futures are [`InlineFuture`]s rather than boxes: a deserializer is
//! called once per key and once per value of every fetched record, so a box per
//! call would break the receive path's allocation budget
//! (`consumer-threading.md` §27).
//!
//! Unlike `DynProducer` it is crate-private: the crate only takes
//! deserializers in, through the `impl Deserializer<K>` parameters of
//! `KafkaConsumer::new`, which box them once, and never hands one out, so no
//! caller needs to name the companion, and keeping it private leaves the
//! dispatch free to change (`check-dyn-compatible`). The C binding, whose
//! deserializers are trait objects already, passes its boxes to the
//! crate-private `KafkaConsumer::with_boxed_deserializers` instead of boxing
//! them again.
//!
//! A method added to [`Deserializer`] goes in three places here: the trait,
//! the blanket impl and the impl on `dyn DynDeserializer`. `cargo xtask
//! lint-custom` (`check-dyn-forwarding`) fails until all three forward it.

use std::collections::HashMap;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Deserializer;
use crate::common::serialization::InlineFuture;

mod private {
    // Seals `DynDeserializer` to `Deserializer` implementors. Nothing outside
    // this module can name it, which `unnameable_types` would report if the
    // companion were public; it is crate-private, so the lint stays quiet.
    pub trait Sealed<T> {}
}

impl<T, D: super::Deserializer<T>> private::Sealed<T> for D {}

/// A dyn-compatible [`Deserializer`], for holding a deserializer of any type
/// behind one type, as the consumer does with `Box<dyn DynDeserializer<K>>`.
///
/// Every [`Deserializer`] implements `DynDeserializer` automatically, and the
/// trait is sealed, so it cannot be implemented directly: implement
/// [`Deserializer`] instead. Each method forwards to the [`Deserializer`]
/// method of the same name and returns its future as an [`InlineFuture`],
/// which allocates nothing while the future fits its buffer.
/// `dyn DynDeserializer<T>` implements [`Deserializer`] in turn, so a boxed
/// deserializer can be passed as `&*boxed` to generic code taking
/// `D: Deserializer<T> + ?Sized`.
///
/// Box a deserializer as `dyn` once: a deserializer that wraps a
/// `dyn DynDeserializer` has a future that holds the inner [`InlineFuture`],
/// which no longer fits a second one, so boxing the wrapper as `dyn` again
/// allocates on every call.
///
/// With both traits in scope a method call on a `dyn DynDeserializer` is
/// ambiguous, since both define it; call it as
/// `DynDeserializer::deserialize(&*deserializer, topic, data)`, or import only
/// one of the two traits.
pub(crate) trait DynDeserializer<T>: private::Sealed<T> + Send + Sync + 'static {
    /// See [`Deserializer::deserialize`].
    ///
    /// # Errors
    ///
    /// As [`Deserializer::deserialize`].
    fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a;

    /// See [`Deserializer::deserialize_with_headers`].
    ///
    /// # Errors
    ///
    /// As [`Deserializer::deserialize_with_headers`].
    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a [u8],
    ) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a;

    /// See [`Deserializer::deserialize_with_headers_bytes`].
    ///
    /// # Errors
    ///
    /// As [`Deserializer::deserialize_with_headers_bytes`].
    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a Bytes,
    ) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a;

    /// See [`Deserializer::configure`].
    ///
    /// # Errors
    ///
    /// As [`Deserializer::configure`].
    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> InlineFuture<'a, Result<(), Error>>
    where
        T: 'a;

    /// See [`Deserializer::close`].
    ///
    /// # Errors
    ///
    /// As [`Deserializer::close`].
    fn close<'a>(&'a mut self) -> InlineFuture<'a, Result<(), Error>>
    where
        T: 'a;
}

impl<T, D: Deserializer<T>> DynDeserializer<T> for D {
    fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a,
    {
        InlineFuture::new(Deserializer::deserialize(self, topic, data))
    }

    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a [u8],
    ) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a,
    {
        InlineFuture::new(Deserializer::deserialize_with_headers(self, topic, headers, data))
    }

    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a Bytes,
    ) -> InlineFuture<'a, Result<T, Error>>
    where
        T: 'a,
    {
        InlineFuture::new(Deserializer::deserialize_with_headers_bytes(self, topic, headers, data))
    }

    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> InlineFuture<'a, Result<(), Error>>
    where
        T: 'a,
    {
        InlineFuture::new(Deserializer::configure(self, configs, is_key))
    }

    fn close<'a>(&'a mut self) -> InlineFuture<'a, Result<(), Error>>
    where
        T: 'a,
    {
        InlineFuture::new(Deserializer::close(self))
    }
}

// On the trait object rather than on `Box<dyn DynDeserializer>`: a boxed
// deserializer that implemented `Deserializer` would also pick up the blanket
// `DynDeserializer` impl, and a call on the box would then nest one
// `InlineFuture` in another, which no longer fits inline.
impl<T: 'static> Deserializer<T> for dyn DynDeserializer<T> {
    fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        DynDeserializer::deserialize(self, topic, data)
    }

    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a [u8],
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        DynDeserializer::deserialize_with_headers(self, topic, headers, data)
    }

    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a mut RecordHeaders,
        data: &'a Bytes,
    ) -> impl Future<Output = Result<T, Error>> + Send + 'a {
        DynDeserializer::deserialize_with_headers_bytes(self, topic, headers, data)
    }

    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        DynDeserializer::configure(self, configs, is_key)
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        DynDeserializer::close(self)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::common::serialization::BytesDeserializer;
    use crate::common::serialization::StringDeserializer;

    async fn through_generic<D: Deserializer<String> + ?Sized>(deserializer: &D, data: &[u8]) -> String {
        Deserializer::deserialize(deserializer, "topic", data)
            .await
            .expect("deserialize")
    }

    #[tokio::test]
    async fn test_dyn_deserializer_is_a_deserializer() {
        let boxed: Box<dyn DynDeserializer<String>> = Box::new(StringDeserializer::new());
        assert_eq!("value", through_generic(&*boxed, b"value").await);
    }

    /// A direct call through the companion, named in full to avoid the
    /// ambiguity with `Deserializer::deserialize`.
    #[tokio::test]
    async fn test_call_through_the_companion() {
        let deserializer: Box<dyn DynDeserializer<String>> = Box::new(StringDeserializer::new());
        let value = DynDeserializer::deserialize(&*deserializer, "topic", b"value").await.unwrap();
        assert_eq!("value", value);
    }

    #[tokio::test]
    async fn test_bytes_overload_keeps_the_override_through_dyn() {
        let boxed: Box<dyn DynDeserializer<Bytes>> = Box::new(BytesDeserializer::new());
        let source = Bytes::from(vec![1u8, 2, 3, 4]);
        let data = source.slice(1..3);
        let mut headers = RecordHeaders::new();
        let result = DynDeserializer::deserialize_with_headers_bytes(&*boxed, "topic", &mut headers, &data)
            .await
            .expect("deserialize");
        assert_eq!(
            data.as_ptr(),
            result.as_ptr(),
            "BytesDeserializer's override returns the slice itself"
        );
    }

    /// `consumer-threading.md` §27: a call through `dyn` on the receive path
    /// allocates nothing beyond the deserialized value; `BytesDeserializer`'s
    /// value is the shared buffer itself, so nothing at all.
    #[test]
    fn test_dyn_call_allocates_nothing() {
        let boxed: Box<dyn DynDeserializer<Bytes>> = Box::new(BytesDeserializer::new());
        let source = Bytes::from(vec![1u8, 2, 3, 4]);
        // Slicing promotes the buffer to a shared one, so the clone below is a
        // reference count increment.
        let data = source.slice(1..3);
        let mut headers = RecordHeaders::new();
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let (count, result) = {
            let _guard = crate::AllocTrackingGuard::new();
            crate::AllocTrackingGuard::reset();
            let mut future = std::pin::pin!(DynDeserializer::deserialize_with_headers_bytes(
                &*boxed,
                "topic",
                &mut headers,
                &data
            ));
            let result = future.as_mut().poll(&mut cx);
            (crate::AllocTrackingGuard::count(), result)
        };
        match result {
            std::task::Poll::Ready(Ok(bytes)) => assert_eq!(data.as_ptr(), bytes.as_ptr()),
            other => panic!("expected the ready slice, got {other:?}"),
        }
        assert_eq!(0, count, "a dyn deserializer call must not allocate");
    }

    #[tokio::test]
    async fn test_configure_and_close_reach_the_implementation() {
        struct Counting(Arc<AtomicUsize>, Arc<AtomicUsize>);
        impl Deserializer<String> for Counting {
            async fn deserialize(&self, _topic: &str, _data: &[u8]) -> Result<String, Error> {
                Ok(String::new())
            }
            async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), Error> {
                assert!(is_key);
                self.0.fetch_add(configs.len(), Ordering::SeqCst);
                Ok(())
            }
            async fn close(&mut self) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Err(Error::serialization("close failed"))
            }
        }
        let (configured, closed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut boxed: Box<dyn DynDeserializer<String>> = Box::new(Counting(configured.clone(), closed.clone()));
        let configs = HashMap::from([("a".to_string(), "b".to_string())]);
        DynDeserializer::configure(&mut *boxed, &configs, true)
            .await
            .expect("configure");
        let error = DynDeserializer::close(&mut *boxed).await.expect_err("close fails");
        assert!(error.is_serialization_error());
        assert_eq!("close failed", error.message());
        assert_eq!(1, configured.load(Ordering::SeqCst));
        assert_eq!(1, closed.load(Ordering::SeqCst));
    }
}
