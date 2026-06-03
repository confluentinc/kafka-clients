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
//! receive path stays zero-copy.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Per-thread allocation count — incremented by [`TrackingAllocator`]
    /// when [`TRACK_ENABLED`] is `true`.
    static ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    /// Per-thread "is tracking on?" flag — flipped to `true` by
    /// [`AllocTrackingGuard::new`] and back to `false` on drop.
    static TRACK_ENABLED: Cell<bool> = const { Cell::new(false) };
}

/// A `GlobalAlloc` wrapper that delegates to `System` and increments a
/// thread-local counter on each allocation when tracking is enabled.
pub(crate) struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let track = TRACK_ENABLED.with(|t| t.get());
        if track {
            ALLOC_COUNT.with(|c| c.set(c.get().saturating_add(1)));
        }
        // SAFETY: layout is provided by the caller per GlobalAlloc contract.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr / layout provided by the caller per GlobalAlloc contract.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let track = TRACK_ENABLED.with(|t| t.get());
        if track {
            ALLOC_COUNT.with(|c| c.set(c.get().saturating_add(1)));
        }
        // SAFETY: layout is provided by the caller per GlobalAlloc contract.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let track = TRACK_ENABLED.with(|t| t.get());
        if track {
            ALLOC_COUNT.with(|c| c.set(c.get().saturating_add(1)));
        }
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
    /// Enables tracking, resetting the counter to 0.
    pub(crate) fn new() -> Self {
        ALLOC_COUNT.with(|c| c.set(0));
        TRACK_ENABLED.with(|t| t.set(true));
        Self(())
    }

    /// Returns the current allocation count for this thread.
    pub(crate) fn count() -> usize {
        ALLOC_COUNT.with(|c| c.get())
    }

    /// Resets the per-thread counter to zero without disabling tracking.
    pub(crate) fn reset() {
        ALLOC_COUNT.with(|c| c.set(0));
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
