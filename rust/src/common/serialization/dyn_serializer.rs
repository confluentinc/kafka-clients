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

//! The dyn-compatible view of [`Serializer`].
//!
//! Has no Java counterpart (`definition-of-done.md` §7). A Java interface is
//! always usable as a runtime-polymorphic type, so a producer holds its
//! `Serializer<K>` whatever its class. The Rust [`Serializer`] trait returns
//! `impl Future` from its methods, which keeps a statically dispatched call
//! free of a boxed future but makes the trait itself not dyn-compatible.
//! `DynSerializer` restores the Java capability (CLAUDE.md §3, "traits must be
//! dyn-compatible") the way `DynProducer` does for `Producer`, except that its
//! futures are [`InlineFuture`]s rather than boxes: a serializer is called once
//! per key and once per value of every record, so a box per call would be an
//! allocation per record on the send path (CLAUDE.md §13).
//!
//! Unlike `DynProducer` it is crate-private: the crate only takes serializers
//! in, through the `impl Serializer<K> + 'static` parameters of
//! `KafkaProducer::new` and `MockProducerOptionsBuilder::set_key_serializer` /
//! `set_value_serializer`, which box them once, and never hands one out, so no
//! caller needs to name the companion, and keeping it private leaves the
//! dispatch free to change (`check-dyn-compatible`). The C binding, whose
//! serializers are trait objects already, passes its boxes to the
//! crate-private `KafkaProducer::with_boxed_serializers` instead of boxing
//! them again.
//!
//! A method added to [`Serializer`] goes in three places here: the trait, the
//! blanket impl and the impl on `dyn DynSerializer`. `cargo xtask lint-custom`
//! (`check-dyn-forwarding`) fails until all three forward it.

use std::borrow::Cow;
use std::collections::HashMap;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::InlineFuture;
use crate::common::serialization::Serializer;

mod private {
    // Seals `DynSerializer` to `Serializer` implementors. Nothing outside this
    // module can name it, which `unnameable_types` would report if the
    // companion were public; it is crate-private, so the lint stays quiet.
    pub trait Sealed<T: ?Sized> {}
}

impl<T: ?Sized, S: super::Serializer<T>> private::Sealed<T> for S {}

/// A dyn-compatible [`Serializer`], for holding a serializer of any type
/// behind one type, as the producer does with `Box<dyn DynSerializer<K>>`.
///
/// Every [`Serializer`] implements `DynSerializer` automatically, and the trait
/// is sealed, so it cannot be implemented directly: implement [`Serializer`]
/// instead. Each method forwards to the [`Serializer`] method of the same name
/// and returns its future as an [`InlineFuture`], which allocates nothing while
/// the future fits its buffer. `dyn DynSerializer<T>` implements [`Serializer`]
/// in turn, so a boxed serializer can be passed as `&*boxed` to generic code
/// taking `S: Serializer<T> + ?Sized`.
///
/// Box a serializer as `dyn` once: a serializer that wraps a
/// `dyn DynSerializer` has a future that holds the inner [`InlineFuture`],
/// which no longer fits a second one, so boxing the wrapper as `dyn` again
/// allocates on every call.
///
/// With both traits in scope a method call on a `dyn DynSerializer` is
/// ambiguous, since both define it; call it as
/// `DynSerializer::serialize(&*serializer, topic, data)`, or import only one of
/// the two traits.
pub(crate) trait DynSerializer<T: ?Sized>: private::Sealed<T> + Send + Sync {
    /// See [`Serializer::serialize`].
    ///
    /// # Errors
    ///
    /// As [`Serializer::serialize`].
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a T>,
    ) -> InlineFuture<'a, Result<Option<Cow<'a, [u8]>>, Error>>;

    /// See [`Serializer::serialize_with_headers`].
    ///
    /// # Errors
    ///
    /// As [`Serializer::serialize_with_headers`].
    fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a T>,
    ) -> InlineFuture<'h, Result<Option<Cow<'a, [u8]>>, Error>>
    where
        'a: 'h;

    /// See [`Serializer::configure`].
    ///
    /// # Errors
    ///
    /// As [`Serializer::configure`].
    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> InlineFuture<'a, Result<(), Error>>;

    /// See [`Serializer::close`].
    ///
    /// # Errors
    ///
    /// As [`Serializer::close`].
    fn close(&self) -> InlineFuture<'_, Result<(), Error>>;
}

impl<T: ?Sized, S: Serializer<T>> DynSerializer<T> for S {
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a T>,
    ) -> InlineFuture<'a, Result<Option<Cow<'a, [u8]>>, Error>> {
        InlineFuture::new(Serializer::serialize(self, topic, data))
    }

    fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a T>,
    ) -> InlineFuture<'h, Result<Option<Cow<'a, [u8]>>, Error>>
    where
        'a: 'h,
    {
        InlineFuture::new(Serializer::serialize_with_headers(self, topic, headers, data))
    }

    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> InlineFuture<'a, Result<(), Error>> {
        InlineFuture::new(Serializer::configure(self, configs, is_key))
    }

    fn close(&self) -> InlineFuture<'_, Result<(), Error>> {
        InlineFuture::new(Serializer::close(self))
    }
}

// On the trait object rather than on `Box<dyn DynSerializer>`: a boxed
// serializer that implemented `Serializer` would also pick up the blanket
// `DynSerializer` impl, and a call on the box would then nest one
// `InlineFuture` in another, which no longer fits inline.
impl<T: ?Sized> Serializer<T> for dyn DynSerializer<T> + '_ {
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a T>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + 'a {
        DynSerializer::serialize(self, topic, data)
    }

    fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a T>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send
    where
        'a: 'h,
    {
        DynSerializer::serialize_with_headers(self, topic, headers, data)
    }

    fn configure<'a>(
        &'a mut self,
        configs: &'a HashMap<String, String>,
        is_key: bool,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'a {
        DynSerializer::configure(self, configs, is_key)
    }

    fn close(&self) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        DynSerializer::close(self)
    }
}

#[cfg(test)]
mod tests {
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::*;
    use crate::common::serialization::ByteArraySerializer;
    use crate::common::serialization::StringSerializer;

    async fn through_generic<S: Serializer<str> + ?Sized>(serializer: &S, value: &str) -> Vec<u8> {
        Serializer::serialize(serializer, "topic", Some(value))
            .await
            .expect("serialize")
            .expect("bytes")
            .into_owned()
    }

    #[tokio::test]
    async fn test_dyn_serializer_is_a_serializer() {
        let boxed: Box<dyn DynSerializer<str>> = Box::new(StringSerializer::new());
        assert_eq!(b"value".to_vec(), through_generic(&*boxed, "value").await);
    }

    /// A direct call through the companion, named in full to avoid the
    /// ambiguity with `Serializer::serialize`.
    #[tokio::test]
    async fn test_call_through_the_companion() {
        let serializer: Box<dyn DynSerializer<str>> = Box::new(StringSerializer::new());
        let bytes = DynSerializer::serialize(&*serializer, "topic", Some("value")).await.unwrap();
        assert_eq!(Some(&b"value"[..]), bytes.as_deref());
    }

    #[test]
    fn test_dyn_call_on_a_built_in_is_ready_and_borrows() {
        let boxed: Box<dyn DynSerializer<[u8]>> = Box::new(ByteArraySerializer::new());
        let data = [1u8, 2, 3];
        let mut headers = RecordHeaders::new();
        let mut future = std::pin::pin!(DynSerializer::serialize_with_headers(
            &*boxed,
            "topic",
            &mut headers,
            Some(&data[..])
        ));
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(Some(Cow::Borrowed(bytes)))) => assert_eq!(data.as_ptr(), bytes.as_ptr()),
            other => panic!("expected ready borrowed bytes, got {other:?}"),
        }
    }

    /// CLAUDE.md §13: a call through `dyn` on the send path allocates nothing,
    /// neither for the future (it fits the `InlineFuture` buffer) nor for the
    /// bytes (the built-ins borrow the input).
    #[test]
    fn test_dyn_call_allocates_nothing() {
        let string: Box<dyn DynSerializer<str>> = Box::new(StringSerializer::new());
        let array: Box<dyn DynSerializer<[u8]>> = Box::new(ByteArraySerializer::new());
        let mut headers = RecordHeaders::new();
        let data = [1u8, 2, 3];
        let mut cx = Context::from_waker(Waker::noop());
        let count = {
            let _guard = crate::AllocTrackingGuard::new();
            crate::AllocTrackingGuard::reset();
            let mut future = std::pin::pin!(DynSerializer::serialize_with_headers(
                &*string,
                "topic",
                &mut headers,
                Some("value")
            ));
            assert!(matches!(
                future.as_mut().poll(&mut cx),
                Poll::Ready(Ok(Some(Cow::Borrowed(b"value"))))
            ));
            let mut future = std::pin::pin!(DynSerializer::serialize(&*array, "topic", Some(&data[..])));
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Ok(Some(Cow::Borrowed(_))))));
            crate::AllocTrackingGuard::count()
        };
        assert_eq!(0, count, "a dyn serializer call must not allocate");
    }

    #[tokio::test]
    async fn test_configure_and_close_forward_through_dyn() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Counting(Arc<AtomicUsize>, Arc<AtomicUsize>);
        impl Serializer<str> for Counting {
            async fn serialize<'a>(
                &'a self,
                _topic: &'a str,
                _data: Option<&'a str>,
            ) -> Result<Option<Cow<'a, [u8]>>, Error> {
                Ok(None)
            }
            async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) -> Result<(), Error> {
                assert!(is_key);
                self.0.fetch_add(configs.len(), Ordering::SeqCst);
                Err(Error::serialization("configure failed"))
            }
            async fn close(&self) -> Result<(), Error> {
                self.1.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let (configured, closed) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let mut boxed: Box<dyn DynSerializer<str>> = Box::new(Counting(configured.clone(), closed.clone()));
        let configs = HashMap::from([("a".to_string(), "b".to_string())]);
        let error = DynSerializer::configure(&mut *boxed, &configs, true)
            .await
            .expect_err("configure fails");
        assert!(error.is_serialization_error());
        assert_eq!("configure failed", error.message());
        DynSerializer::close(&*boxed).await.expect("close");
        // And through the `Serializer` impl on the trait object.
        Serializer::close(&*boxed).await.expect("close");
        assert_eq!(1, configured.load(Ordering::SeqCst));
        assert_eq!(2, closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_configure_and_close_default_to_no_ops() {
        let mut boxed: Box<dyn DynSerializer<str>> = Box::new(StringSerializer::new());
        DynSerializer::configure(&mut *boxed, &HashMap::new(), false)
            .await
            .expect("configure");
        DynSerializer::close(&*boxed).await.expect("close");
        assert_eq!(b"v".to_vec(), through_generic(&*boxed, "v").await);
    }

    #[test]
    fn test_errors_pass_through_unchanged() {
        struct Failing;
        impl Serializer<str> for Failing {
            async fn serialize<'a>(
                &'a self,
                _topic: &'a str,
                _data: Option<&'a str>,
            ) -> Result<Option<Cow<'a, [u8]>>, Error> {
                Err(Error::serialization("cannot serialize"))
            }
        }
        let boxed: Box<dyn DynSerializer<str>> = Box::new(Failing);
        let mut future = std::pin::pin!(DynSerializer::serialize(&*boxed, "topic", Some("v")));
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Err(error)) => {
                assert!(error.is_serialization_error());
                assert_eq!("cannot serialize", error.message());
            },
            other => panic!("expected a ready error, got {other:?}"),
        }
    }
}
