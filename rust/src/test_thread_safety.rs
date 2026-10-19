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

//! Test-only probes telling whether a type is `Send` / `Sync`, so a test can
//! assert that a type is *not* `Sync` with a plain `assert!` instead of a
//! compile-fail test.
//!
//! Has no Java counterpart (`definition-of-done.md` §7): Java has no auto
//! traits. It pins the thread-safety of the consumer types
//! (`consumer-threading.md` §2).
//!
//! The probe is the autoderef trick: an inherent method that exists only when
//! the bound holds shadows a method of the same name on the type the probe
//! dereferences to, which answers `false`. Method resolution picks the probe's
//! own method when its bound is met, and falls back through `Deref` otherwise.

use std::marker::PhantomData;
use std::ops::Deref;

/// Carries the probed type.
pub(crate) struct Probe<T: ?Sized>(PhantomData<T>);

impl<T: ?Sized> Probe<T> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

/// The fallback answering `false`, reached through `Deref` when the probe's
/// own method does not apply.
pub(crate) struct Fallback;

impl Fallback {
    pub(crate) fn is_send(&self) -> bool {
        false
    }

    pub(crate) fn is_sync(&self) -> bool {
        false
    }
}

impl<T: ?Sized> Deref for Probe<T> {
    type Target = Fallback;

    fn deref(&self) -> &Fallback {
        &Fallback
    }
}

impl<T: ?Sized + Send> Probe<T> {
    /// Shadows [`Fallback::is_send`] when `T: Send`.
    pub(crate) fn is_send(&self) -> bool {
        true
    }
}

impl<T: ?Sized + Sync> Probe<T> {
    /// Shadows [`Fallback::is_sync`] when `T: Sync`.
    pub(crate) fn is_sync(&self) -> bool {
        true
    }
}

/// `true` when the type is `Send`.
macro_rules! is_send {
    ($t:ty) => {
        $crate::test_thread_safety::Probe::<$t>::new().is_send()
    };
}

/// `true` when the type is `Sync`.
macro_rules! is_sync {
    ($t:ty) => {
        $crate::test_thread_safety::Probe::<$t>::new().is_sync()
    };
}

pub(crate) use {is_send, is_sync};

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    /// The probe tells the four combinations apart.
    #[test]
    fn probe_answers_for_known_types() {
        assert!(is_send!(i32) && is_sync!(i32));
        assert!(is_send!(Cell<i32>) && !is_sync!(Cell<i32>));
        assert!(!is_send!(Rc<i32>) && !is_sync!(Rc<i32>));
        assert!(!is_send!(std::sync::MutexGuard<'static, i32>) && is_sync!(std::sync::MutexGuard<'static, i32>));
        assert!(is_send!(dyn Send) && !is_sync!(dyn Send));
    }
}
