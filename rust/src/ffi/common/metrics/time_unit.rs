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

//! `kafka_common_metrics_TimeUnit_t`: the Rust-only [`TimeUnit`] standing
//! for `java.util.concurrent.TimeUnit` where the metrics API takes one
//! (CLAUDE.md §4, "Enums").
//!
//! A unit enum crosses as borrowed per-value singletons: each value's
//! function returns a static instance, never freed and comparable with `==`,
//! `kafka_common_metrics_TimeUnit_e` is the C enum for a `switch`, and
//! `__enum` maps a singleton to it.

#![expect(non_camel_case_types)]

use std::ffi::c_char;

use crate::common::metrics::TimeUnit;

/// Opaque handle to a [`TimeUnit`] singleton.
// the Rust-only stand-in for `java.util.concurrent.TimeUnit`
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_metrics_TimeUnit_t {
    _private: [u8; 0],
}

/// The values of [`TimeUnit`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_metrics_TimeUnit_e {
    nanoseconds,
    microseconds,
    milliseconds,
    seconds,
    minutes,
    hours,
    days,
}

/// One static instance per value, indexed by [`kafka_common_metrics_TimeUnit_e`].
static VARIANTS: [TimeUnit; 7] = [
    TimeUnit::Nanoseconds,
    TimeUnit::Microseconds,
    TimeUnit::Milliseconds,
    TimeUnit::Seconds,
    TimeUnit::Minutes,
    TimeUnit::Hours,
    TimeUnit::Days,
];

/// Exhaustive, so a value added later fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(unit: TimeUnit) -> kafka_common_metrics_TimeUnit_e {
    match unit {
        TimeUnit::Nanoseconds => kafka_common_metrics_TimeUnit_e::nanoseconds,
        TimeUnit::Microseconds => kafka_common_metrics_TimeUnit_e::microseconds,
        TimeUnit::Milliseconds => kafka_common_metrics_TimeUnit_e::milliseconds,
        TimeUnit::Seconds => kafka_common_metrics_TimeUnit_e::seconds,
        TimeUnit::Minutes => kafka_common_metrics_TimeUnit_e::minutes,
        TimeUnit::Hours => kafka_common_metrics_TimeUnit_e::hours,
        TimeUnit::Days => kafka_common_metrics_TimeUnit_e::days,
    }
}

/// The borrowed singleton standing for `unit`.
pub(crate) fn singleton(unit: TimeUnit) -> *const kafka_common_metrics_TimeUnit_t {
    &VARIANTS[enum_of(unit) as usize] as *const TimeUnit as *const kafka_common_metrics_TimeUnit_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `unit` must be a singleton returned by this module.
pub(crate) unsafe fn value_of(unit: *const kafka_common_metrics_TimeUnit_t) -> TimeUnit {
    unsafe { *(unit as *const TimeUnit) }
}

/// `TimeUnit.NANOSECONDS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_nanoseconds() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Nanoseconds)
}

/// `TimeUnit.MICROSECONDS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_microseconds() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Microseconds)
}

/// `TimeUnit.MILLISECONDS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_milliseconds() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Milliseconds)
}

/// `TimeUnit.SECONDS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_seconds() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Seconds)
}

/// `TimeUnit.MINUTES`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_minutes() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Minutes)
}

/// `TimeUnit.HOURS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_hours() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Hours)
}

/// `TimeUnit.DAYS`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_TimeUnit_days() -> *const kafka_common_metrics_TimeUnit_t {
    singleton(TimeUnit::Days)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_TimeUnit__enum(
    self_: *const kafka_common_metrics_TimeUnit_t,
) -> kafka_common_metrics_TimeUnit_e {
    enum_of(unsafe { value_of(self_) })
}

/// `name()`: the Java constant name, a static string never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_TimeUnit_name(
    self_: *const kafka_common_metrics_TimeUnit_t,
) -> *const c_char {
    // Every name is a plain ASCII literal; the table keeps the trailing NUL
    // the C side needs while `name()` stays the Java string.
    match unsafe { value_of(self_) } {
        TimeUnit::Nanoseconds => c"NANOSECONDS".as_ptr(),
        TimeUnit::Microseconds => c"MICROSECONDS".as_ptr(),
        TimeUnit::Milliseconds => c"MILLISECONDS".as_ptr(),
        TimeUnit::Seconds => c"SECONDS".as_ptr(),
        TimeUnit::Minutes => c"MINUTES".as_ptr(),
        TimeUnit::Hours => c"HOURS".as_ptr(),
        TimeUnit::Days => c"DAYS".as_ptr(),
    }
}

/// `toMillis(long window)`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_TimeUnit_to_millis(
    self_: *const kafka_common_metrics_TimeUnit_t,
    window: i64,
) -> i64 {
    unsafe { value_of(self_) }.to_millis(window)
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn singletons_round_trip_and_match_their_enumerator() {
        for (index, &unit) in VARIANTS.iter().enumerate() {
            let handle = singleton(unit);
            unsafe {
                assert_eq!(value_of(handle), unit);
                assert_eq!(kafka_common_metrics_TimeUnit__enum(handle) as usize, index);
                assert_eq!(
                    CStr::from_ptr(kafka_common_metrics_TimeUnit_name(handle)).to_str().unwrap(),
                    unit.name()
                );
                assert_eq!(kafka_common_metrics_TimeUnit_to_millis(handle, 2), unit.to_millis(2));
            }
        }
        assert_eq!(kafka_common_metrics_TimeUnit_nanoseconds(), singleton(TimeUnit::Nanoseconds));
        assert_eq!(kafka_common_metrics_TimeUnit_microseconds(), singleton(TimeUnit::Microseconds));
        assert_eq!(kafka_common_metrics_TimeUnit_milliseconds(), singleton(TimeUnit::Milliseconds));
        assert_eq!(kafka_common_metrics_TimeUnit_seconds(), singleton(TimeUnit::Seconds));
        assert_eq!(kafka_common_metrics_TimeUnit_minutes(), singleton(TimeUnit::Minutes));
        assert_eq!(kafka_common_metrics_TimeUnit_hours(), singleton(TimeUnit::Hours));
        assert_eq!(kafka_common_metrics_TimeUnit_days(), singleton(TimeUnit::Days));
        unsafe {
            assert_eq!(
                kafka_common_metrics_TimeUnit_to_millis(kafka_common_metrics_TimeUnit_seconds(), 3),
                3000
            );
        }
    }
}
