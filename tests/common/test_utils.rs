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

//! General-purpose async test utilities.
//!
//! Provides helpers analogous to `kafka.utils.TestUtils` in the Apache Kafka
//! Scala test utilities.

use std::future::Future;
use std::time::Duration;

/// Default total wait time for [`try_until_no_assertion_error`], matching the
/// Apache Kafka Scala value of 15 seconds.
pub const RETRY_WAIT_TIME: Duration = Duration::from_millis(15_000);

/// Default polling interval for [`try_until_no_assertion_error`], matching the
/// Apache Kafka Scala value of 100 ms.
pub const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// Retry `f` until it returns `Ok(T)` or the deadline is exceeded.
///
/// Equivalent to `kafka.utils.TestUtils.tryUntilNoAssertionError` in the
/// Apache Kafka Scala test utilities. Polls every `pause` until `wait_time`
/// elapses, then propagates the last error.
pub async fn try_until_no_assertion_error<F, Fut, T, E>(wait_time: Duration, pause: Duration, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let deadline = tokio::time::Instant::now() + wait_time;
    let mut last_err;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                last_err = e;
                if tokio::time::Instant::now() >= deadline {
                    return Err(last_err);
                }
                tokio::time::sleep(pause).await;
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn returns_ok_on_first_attempt() {
        let result = try_until_no_assertion_error(Duration::from_millis(1_000), Duration::from_millis(10), || async {
            Ok::<_, String>(42)
        })
        .await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn retries_on_transient_failures_then_ok() {
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let result = try_until_no_assertion_error(Duration::from_millis(1_000), Duration::from_millis(10), move || {
            let counter = Arc::clone(&call_count_clone);
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                if n < 2 { Err("not yet") } else { Ok("done") }
            }
        })
        .await;

        assert_eq!(result, Ok("done"));
        assert!(call_count.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn returns_last_err_when_deadline_exceeded() {
        let result = try_until_no_assertion_error(Duration::from_millis(50), Duration::from_millis(10), || async {
            Err::<(), _>("always fails")
        })
        .await;
        assert_eq!(result, Err("always fails"));
    }
}
