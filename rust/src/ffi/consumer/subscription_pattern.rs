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

//! `kafka_consumer_SubscriptionPattern_t`:
//! `org.apache.kafka.clients.consumer.SubscriptionPattern`, the server-side
//! (RE2/J) regular expression of `subscribe(SubscriptionPattern)`.

use std::ffi::{CString, c_char};

use crate::consumer::SubscriptionPattern;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`SubscriptionPattern`].
#[repr(C)]
pub struct kafka_consumer_SubscriptionPattern_t {
    _private: [u8; 0],
}

struct SubscriptionPatternInner {
    pattern: SubscriptionPattern,
    pattern_c: CString,
}

/// The value behind a handle.
///
/// # Safety
///
/// `pattern` must be a valid subscription-pattern handle.
pub(crate) unsafe fn subscription_pattern_ref<'a>(
    pattern: *const kafka_consumer_SubscriptionPattern_t,
) -> &'a SubscriptionPattern {
    &unsafe { &*(pattern as *const SubscriptionPatternInner) }.pattern
}

/// `new SubscriptionPattern(String pattern)`: an owned handle freed with
/// [`kafka_consumer_SubscriptionPattern_destroy`]. The client performs no
/// validation; the group coordinator does.
///
/// # Safety
///
/// `pattern` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionPattern_new(
    pattern: *const c_char,
) -> *mut kafka_consumer_SubscriptionPattern_t {
    let pattern = unsafe { c_str_to_string(pattern) };
    let pattern_c = owned_c_string(&pattern);
    Box::into_raw(Box::new(SubscriptionPatternInner {
        pattern: SubscriptionPattern::new(pattern),
        pattern_c,
    })) as *mut kafka_consumer_SubscriptionPattern_t
}

/// `pattern()`, borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionPattern_pattern(
    self_: *const kafka_consumer_SubscriptionPattern_t,
) -> *const c_char {
    unsafe { &*(self_ as *const SubscriptionPatternInner) }.pattern_c.as_ptr()
}

/// Java `toString()`: an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionPattern_to_string(
    self_: *const kafka_consumer_SubscriptionPattern_t,
) -> *mut c_char {
    into_c_string(&unsafe { subscription_pattern_ref(self_) }.to_string())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionPattern_destroy(self_: *mut kafka_consumer_SubscriptionPattern_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut SubscriptionPatternInner)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn round_trip() {
        let p = unsafe { kafka_consumer_SubscriptionPattern_new(c"topic-.*".as_ptr()) };
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_consumer_SubscriptionPattern_pattern(p)).to_str().unwrap(),
                "topic-.*"
            );
            assert_eq!(subscription_pattern_ref(p).pattern(), "topic-.*");
            let s = kafka_consumer_SubscriptionPattern_to_string(p);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                SubscriptionPattern::new("topic-.*").to_string()
            );
            kafka_string_destroy(s);
            kafka_consumer_SubscriptionPattern_destroy(p);
        }
    }
}
