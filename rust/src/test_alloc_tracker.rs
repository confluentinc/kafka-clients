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

//! Test-only allocation tracker.
//!
//! Provides a thin wrapper over `std::alloc::System` that increments a
//! thread-local counter when "tracking is enabled" for the current
//! thread. The wrapper is plugged in as `#[global_allocator]` so every
//! Rust allocation passes through it; the counter only ticks when the
//! `TRACK` guard is active, so the cost on the cold path is one
//! thread-local read per allocation.
//!
//! Used by the §27 per-record allocation-budget regression test
//! (`consumer-threading.md` §27, "Tests required") to assert that the
//! receive path stays zero-copy, by the decode-hardening tests to assert
//! that a length read off the wire does not size an allocation
//! ([`AllocTrackingGuard::max_allocation`]), and by the compressed-batch tests
//! to assert what a batch's records keep allocated once they are handed out
//! ([`AllocTrackingGuard::net_bytes`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Per-thread allocation count — incremented by [`TrackingAllocator`]
    /// when [`TRACK_ENABLED`] is `true`.
    static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    /// Per-thread "is tracking on?" flag — flipped to `true` by
    /// [`AllocTrackingGuard::new`] and back to `false` on drop.
    static TRACK_ENABLED: Cell<bool> = const { Cell::new(false) };
    /// Per-thread size in bytes of the largest single allocation (or
    /// reallocation target) requested while [`TRACK_ENABLED`] is `true`.
    static MAX_ALLOC_SIZE: Cell<usize> = const { Cell::new(0) };
    /// Per-thread bytes allocated minus bytes freed while [`TRACK_ENABLED`] is
    /// `true`. Signed: a block allocated before tracking began may be freed
    /// while it is on.
    static NET_BYTES: Cell<isize> = const { Cell::new(0) };
}

/// Counts one allocation of `size` bytes, if tracking is enabled on this thread.
fn record(size: usize) {
    if TRACK_ENABLED.with(|t| t.get()) {
        ALLOC_COUNT.with(|c| c.set(c.get().saturating_add(1)));
        MAX_ALLOC_SIZE.with(|m| m.set(m.get().max(size)));
        NET_BYTES.with(|n| n.set(n.get().saturating_add_unsigned(size)));
    }
}

/// Counts one deallocation of `size` bytes, if tracking is enabled on this thread.
fn release(size: usize) {
    if TRACK_ENABLED.with(|t| t.get()) {
        NET_BYTES.with(|n| n.set(n.get().saturating_sub_unsigned(size)));
    }
}

/// A `GlobalAlloc` wrapper that delegates to `System` and increments a
/// thread-local counter on each allocation when tracking is enabled.
pub(crate) struct TrackingAllocator;

// SAFETY: every method forwards the caller's arguments unchanged to `System`, so the
// `GlobalAlloc` contract holds exactly as it does for `System` (`alloc` returns a block
// of the requested layout or null; `dealloc` / `realloc` receive a pointer this
// allocator returned together with its layout). The only extra work is `record` and
// `release`, which update `const`-initialised `Cell` thread-locals (the count, the
// largest size and the net bytes) and perform no allocation of their own.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: layout is provided by the caller per GlobalAlloc contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        release(layout.size());
        // SAFETY: ptr / layout provided by the caller per GlobalAlloc contract.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        // SAFETY: layout is provided by the caller per GlobalAlloc contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        release(layout.size());
        record(new_size);
        // SAFETY: ptr / layout / new_size provided by the caller per GlobalAlloc contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

/// RAII guard that enables allocation tracking for the current thread on
/// construction and disables it on drop.
///
/// Usage:
/// ```ignore
/// let count = {
///     let _guard = AllocTrackingGuard::new();
///     // ... code under measurement ...
///     AllocTrackingGuard::count()
/// };
/// ```
pub(crate) struct AllocTrackingGuard(());

impl AllocTrackingGuard {
    /// Enables tracking, resetting the counter, the largest allocation and the
    /// net bytes to 0.
    pub(crate) fn new() -> Self {
        Self::reset();
        TRACK_ENABLED.with(|t| t.set(true));
        Self(())
    }

    /// Returns the current allocation count for this thread.
    pub(crate) fn count() -> usize {
        ALLOC_COUNT.with(|c| c.get())
    }

    /// Returns the size in bytes of the largest single allocation (or
    /// reallocation target) this thread requested while tracking was enabled.
    pub(crate) fn max_allocation() -> usize {
        MAX_ALLOC_SIZE.with(|m| m.get())
    }

    /// Returns the bytes this thread allocated minus the bytes it freed while
    /// tracking was enabled: what the code under measurement still holds.
    /// Negative when more was freed than allocated, which blocks allocated
    /// before tracking began and freed under it can make it.
    pub(crate) fn net_bytes() -> isize {
        NET_BYTES.with(|n| n.get())
    }

    /// Resets the per-thread counter, largest allocation and net bytes to zero
    /// without changing whether tracking is enabled.
    pub(crate) fn reset() {
        ALLOC_COUNT.with(|c| c.set(0));
        MAX_ALLOC_SIZE.with(|m| m.set(0));
        NET_BYTES.with(|n| n.set(0));
    }
}

impl Drop for AllocTrackingGuard {
    fn drop(&mut self) {
        TRACK_ENABLED.with(|t| t.set(false));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracking_disabled_by_default() {
        // No guard => counter does not tick.
        AllocTrackingGuard::reset();
        let _v: Vec<u8> = Vec::with_capacity(1024);
        assert_eq!(
            0,
            AllocTrackingGuard::count(),
            "counter should not tick when tracking is disabled"
        );
    }

    #[test]
    fn test_tracking_counts_vec_allocation() {
        let _guard = AllocTrackingGuard::new();
        let start = AllocTrackingGuard::count();
        let _v: Vec<u8> = Vec::with_capacity(1024);
        let end = AllocTrackingGuard::count();
        assert!(end > start, "expected at least one allocation");
    }

    #[test]
    fn test_max_allocation_tracks_the_largest_request() {
        let _guard = AllocTrackingGuard::new();
        let small: Vec<u8> = Vec::with_capacity(64);
        let large: Vec<u8> = Vec::with_capacity(4096);
        assert_eq!(4096, AllocTrackingGuard::max_allocation());
        drop((small, large));
        AllocTrackingGuard::reset();
        assert_eq!(0, AllocTrackingGuard::max_allocation());
        let mut grown: Vec<u8> = Vec::with_capacity(16);
        grown.reserve_exact(8192);
        assert_eq!(8192, AllocTrackingGuard::max_allocation(), "a realloc counts its new size");
    }

    #[test]
    fn test_net_bytes_follow_allocations_and_frees() {
        let before: Vec<u8> = Vec::with_capacity(1024);
        let _guard = AllocTrackingGuard::new();
        assert_eq!(0, AllocTrackingGuard::net_bytes());
        let held: Vec<u8> = Vec::with_capacity(4096);
        let freed: Vec<u8> = Vec::with_capacity(512);
        drop(freed);
        assert_eq!(4096, AllocTrackingGuard::net_bytes(), "a freed block no longer counts");
        let mut grown: Vec<u8> = Vec::with_capacity(16);
        grown.reserve_exact(8192);
        assert_eq!(
            4096 + 8192,
            AllocTrackingGuard::net_bytes(),
            "a realloc counts its new size only"
        );
        drop(before);
        assert_eq!(
            4096 + 8192 - 1024,
            AllocTrackingGuard::net_bytes(),
            "freeing a block allocated before tracking began counts against it"
        );
        drop((held, grown));
        assert_eq!(-1024, AllocTrackingGuard::net_bytes());
        AllocTrackingGuard::reset();
        assert_eq!(0, AllocTrackingGuard::net_bytes());
    }

    #[test]
    fn test_tracking_resets_between_runs() {
        {
            let _g = AllocTrackingGuard::new();
            let _v: Vec<u8> = Vec::with_capacity(64);
            assert!(AllocTrackingGuard::count() > 0);
        }
        // Outside the guard, tracking is off — confirm the next guard
        // starts at 0.
        let _g = AllocTrackingGuard::new();
        assert_eq!(0, AllocTrackingGuard::count());
    }
}
