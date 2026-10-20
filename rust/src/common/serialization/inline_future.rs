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

//! A type-erased future stored inline, without a heap allocation.
//!
//! Has no Java counterpart (`definition-of-done.md` §7): Java calls a
//! `Serializer` / `Deserializer` through its interface at no extra cost, while a
//! Rust trait object cannot return an unboxed future. The serde companions
//! [`DynSerializer`] and [`DynDeserializer`] return this type instead of a
//! `Pin<Box<dyn Future>>`, so a call through `dyn` allocates nothing per record
//! (CLAUDE.md §13).
//!
//! [`DynSerializer`]: crate::common::serialization::DynSerializer
//! [`DynDeserializer`]: crate::common::serialization::DynDeserializer

use std::fmt;
use std::marker::PhantomData;
use std::marker::PhantomPinned;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// Bytes available to a future stored inline.
///
/// Private on purpose: it never appears in a signature, so it can change in a
/// minor release (CLAUDE.md §3, no fixed-size array on a public interface).
const CAPACITY: usize = 256;

/// The inline buffer: [`CAPACITY`] bytes aligned to 16, enough for any future
/// whose fields are at most 16-byte aligned (`u128`, SIMD-free types).
#[repr(C, align(16))]
struct Storage(MaybeUninit<[u8; CAPACITY]>);

/// The future a too-large or over-aligned future is boxed into.
type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A future of output `T` that lives in a fixed buffer inside this value
/// rather than on the heap.
///
/// It is what the dyn-compatible serde companions
/// [`DynSerializer`](crate::common::serialization::DynSerializer) and
/// [`DynDeserializer`](crate::common::serialization::DynDeserializer) return,
/// so that a producer or consumer holding its serdes as trait objects pays no
/// allocation per key and per value. A future whose state fits the private
/// buffer (a few hundred bytes, 16-byte aligned) is stored in place; a larger
/// or more strictly aligned one is boxed once per call, so it still works, at
/// the price of that allocation.
///
/// Keep a serde's future small to stay on the inline path: a serde that
/// awaits a large operation only now and then (a schema-registry lookup on a
/// cache miss) should `Box::pin` that operation alone and return the cached
/// answer without it. Wrapping a `dyn DynSerializer` inside another serde that
/// is then boxed as `dyn` again nests one `InlineFuture` in another, which no
/// longer fits: box a serde as `dyn` once.
///
/// The value is `!Unpin`: once polled, the stored future does not move, as
/// [`Future::poll`]'s `Pin` contract requires. `.await` pins it for you.
/// Dropping it before completion drops the stored future in place, which
/// cancels it like dropping any future.
///
/// Crate-private, like the companions that return it: nothing public names
/// it, so its buffer size, its layout and the boxing fallback can change in
/// any release.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct InlineFuture<'a, T> {
    /// The stored future: an `F`, or a [`BoxedFuture`] wrapping one.
    storage: Storage,
    /// Polls the future in `storage`, monomorphised for its type.
    poll: unsafe fn(*mut u8, &mut Context<'_>) -> Poll<T>,
    /// Drops the future in `storage` in place, monomorphised for its type.
    drop_in_place: unsafe fn(*mut u8),
    /// Owns a `Send` future borrowing `'a` (for `Send`, the drop check and
    /// variance) and, being `!Sync`, keeps the value `!Sync` like the futures
    /// it may hold.
    _future: PhantomData<BoxedFuture<'a, T>>,
    /// The stored future may be self-referential once polled.
    _pinned: PhantomPinned,
}

impl<'a, T> InlineFuture<'a, T> {
    /// Stores `future`, inline when it fits the buffer, boxed otherwise.
    pub(crate) fn new<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'a,
        T: 'a,
    {
        if fits::<F>() {
            // SAFETY: `fits::<F>()` is the precondition of `store`.
            unsafe { Self::store(future) }
        } else {
            const { assert!(fits::<BoxedFuture<'static, ()>>(), "a boxed future must fit inline") };
            let boxed: BoxedFuture<'a, T> = Box::pin(future);
            // SAFETY: a `BoxedFuture` is two pointers, which the `const`
            // assertion above checks fits, whatever its output type.
            unsafe { Self::store(boxed) }
        }
    }

    /// Writes `future` into the buffer.
    ///
    /// # Safety
    ///
    /// `fits::<F>()` must hold.
    unsafe fn store<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'a,
    {
        let mut storage = Storage(MaybeUninit::uninit());
        // SAFETY: the caller guarantees the buffer is large and aligned enough
        // for an `F`, and it is uninitialised, so writing overwrites nothing.
        unsafe { storage.0.as_mut_ptr().cast::<F>().write(future) };
        Self {
            storage,
            poll: poll_stored::<F>,
            drop_in_place: drop_stored::<F>,
            _future: PhantomData,
            _pinned: PhantomPinned,
        }
    }

    /// Whether the future is stored inline rather than boxed.
    #[cfg(test)]
    pub(crate) fn is_inline_for<F>(_future: &F) -> bool {
        fits::<F>()
    }
}

/// Whether an `F` fits [`Storage`].
const fn fits<F>() -> bool {
    size_of::<F>() <= CAPACITY && align_of::<F>() <= align_of::<Storage>()
}

/// Polls the `F` at `ptr`.
///
/// # Safety
///
/// `ptr` points to a live `F` that is pinned: it is not moved until dropped.
unsafe fn poll_stored<F: Future>(ptr: *mut u8, cx: &mut Context<'_>) -> Poll<F::Output> {
    // SAFETY: the caller guarantees `ptr` is a live, pinned `F`, and the
    // `&mut` is unique because it comes from `InlineFuture::poll`'s
    // `Pin<&mut Self>`.
    let future = unsafe { Pin::new_unchecked(&mut *ptr.cast::<F>()) };
    future.poll(cx)
}

/// Drops the `F` at `ptr` in place.
///
/// # Safety
///
/// `ptr` points to a live `F` that is not used again.
unsafe fn drop_stored<F>(ptr: *mut u8) {
    // SAFETY: the caller guarantees `ptr` is a live `F` that nothing uses
    // after this call.
    unsafe { ptr.cast::<F>().drop_in_place() }
}

impl<T> Future for InlineFuture<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // SAFETY: nothing is moved out of `this`; the stored future is only
        // polled in place, so the pin carries over to it.
        let this = unsafe { self.get_unchecked_mut() };
        let ptr = this.storage.0.as_mut_ptr().cast::<u8>();
        // SAFETY: `store` wrote an `F` at `ptr` and set `poll` to
        // `poll_stored::<F>`; the future stays there until `drop`, and this
        // value is pinned, so the `F` is too.
        unsafe { (this.poll)(ptr, cx) }
    }
}

impl<T> Drop for InlineFuture<'_, T> {
    fn drop(&mut self) {
        let ptr = self.storage.0.as_mut_ptr().cast::<u8>();
        // SAFETY: `store` wrote an `F` at `ptr` and set `drop_in_place` to
        // `drop_stored::<F>`; it was never dropped before, and `self` is not
        // used after `drop`. Dropping in place keeps the pin guarantee.
        unsafe { (self.drop_in_place)(ptr) }
    }
}

impl<T> fmt::Debug for InlineFuture<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineFuture").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Waker;

    use super::*;

    /// Counts its drops, so a test can tell the stored future was dropped once.
    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Pends once, then resolves to its value; holds a `DropCounter` throughout.
    struct PendOnce<const N: usize> {
        polled: bool,
        _padding: [u8; N],
        _counter: DropCounter,
        value: u32,
    }

    impl<const N: usize> PendOnce<N> {
        fn new(value: u32, drops: &Arc<AtomicUsize>) -> Self {
            Self { polled: false, _padding: [0; N], _counter: DropCounter(drops.clone()), value }
        }
    }

    impl<const N: usize> Future for PendOnce<N> {
        type Output = u32;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
            if self.polled {
                Poll::Ready(self.value)
            } else {
                self.polled = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    fn poll_once<'a, T>(future: &mut Pin<impl std::ops::DerefMut<Target = InlineFuture<'a, T>>>) -> Poll<T> {
        future.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    }

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn test_small_future_is_inline_and_resolves() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = PendOnce::<8>::new(7, &drops);
        assert!(InlineFuture::<u32>::is_inline_for(&future));
        // Boxed rather than `pin!`ned, so that `drop` below drops the future
        // itself rather than a pinned reference to it.
        let mut future = Box::pin(InlineFuture::new(future));
        assert_eq!(Poll::Pending, poll_once(&mut future));
        assert_eq!(Poll::Ready(7), poll_once(&mut future));
        assert_eq!(0, drops.load(Ordering::SeqCst), "completed, not yet dropped");
        drop(future);
        assert_eq!(1, drops.load(Ordering::SeqCst), "dropped once after completion");
    }

    #[test]
    fn test_large_future_falls_back_to_a_box_and_resolves() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = PendOnce::<{ CAPACITY * 2 }>::new(9, &drops);
        assert!(!InlineFuture::<u32>::is_inline_for(&future));
        // Boxed rather than `pin!`ned, so that `drop` below drops the future
        // itself rather than a pinned reference to it.
        let mut future = Box::pin(InlineFuture::new(future));
        assert_eq!(Poll::Pending, poll_once(&mut future));
        assert_eq!(Poll::Ready(9), poll_once(&mut future));
        drop(future);
        assert_eq!(1, drops.load(Ordering::SeqCst));
    }

    #[test]
    fn test_over_aligned_future_falls_back_to_a_box() {
        #[repr(align(64))]
        struct Aligned(u8);
        let future = async {
            let aligned = Aligned(3);
            std::future::ready(()).await;
            aligned.0
        };
        assert!(!InlineFuture::<u8>::is_inline_for(&future));
        let mut future = std::pin::pin!(InlineFuture::new(future));
        assert_eq!(Poll::Ready(3), poll_once(&mut future));
    }

    #[test]
    fn test_drop_before_first_poll_drops_the_future_once() {
        for inline in [true, false] {
            let drops = Arc::new(AtomicUsize::new(0));
            let future = if inline {
                InlineFuture::new(PendOnce::<8>::new(1, &drops))
            } else {
                InlineFuture::new(PendOnce::<{ CAPACITY * 2 }>::new(1, &drops))
            };
            drop(future);
            assert_eq!(1, drops.load(Ordering::SeqCst), "inline: {inline}");
        }
    }

    #[test]
    fn test_drop_after_a_pending_poll_drops_the_future_once() {
        for inline in [true, false] {
            let drops = Arc::new(AtomicUsize::new(0));
            let future = if inline {
                InlineFuture::new(PendOnce::<8>::new(1, &drops))
            } else {
                InlineFuture::new(PendOnce::<{ CAPACITY * 2 }>::new(1, &drops))
            };
            let mut future = Box::pin(future);
            assert_eq!(Poll::Pending, future.as_mut().poll(&mut Context::from_waker(Waker::noop())));
            drop(future);
            assert_eq!(1, drops.load(Ordering::SeqCst), "inline: {inline}");
        }
    }

    #[test]
    fn test_drop_after_completion_drops_the_future_once() {
        for inline in [true, false] {
            let drops = Arc::new(AtomicUsize::new(0));
            let future = if inline {
                InlineFuture::new(PendOnce::<8>::new(5, &drops))
            } else {
                InlineFuture::new(PendOnce::<{ CAPACITY * 2 }>::new(5, &drops))
            };
            let mut future = Box::pin(future);
            let mut cx = Context::from_waker(Waker::noop());
            assert_eq!(Poll::Pending, future.as_mut().poll(&mut cx));
            assert_eq!(Poll::Ready(5), future.as_mut().poll(&mut cx));
            assert_eq!(0, drops.load(Ordering::SeqCst), "inline: {inline}, completed, not yet dropped");
            drop(future);
            assert_eq!(1, drops.load(Ordering::SeqCst), "inline: {inline}");
        }
    }

    #[test]
    fn test_an_async_block_borrowing_its_environment_runs_inline() {
        let data = [1u8, 2, 3];
        let future = InlineFuture::new(async { data.iter().map(|b| u32::from(*b)).sum::<u32>() });
        assert_send(&future);
        let mut future = std::pin::pin!(future);
        assert_eq!(Poll::Ready(6), poll_once(&mut future));
    }

    #[tokio::test]
    async fn test_inline_future_is_send_across_spawn() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = InlineFuture::new(PendOnce::<8>::new(11, &drops));
        assert_eq!(11, tokio::spawn(future).await.expect("join"));
        assert_eq!(1, drops.load(Ordering::SeqCst));
    }
}
