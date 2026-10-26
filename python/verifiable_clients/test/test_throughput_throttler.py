# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ThroughputThrottler. Java has no test of its own; these are the Rust tool's
(rust/tools/verifiable-clients/src/throughput_throttler.rs), plus the timed
sleep and the deficit carried below the minimum sleep."""

from __future__ import annotations

import threading
import time

from throughput_throttler import ThroughputThrottler

NS_PER_SEC = 1_000_000_000
LONG_MAX = 2**63 - 1


def _throttle_in_thread(throttler: ThroughputThrottler) -> threading.Thread:
    thread = threading.Thread(target=throttler.throttle, daemon=True)
    thread.start()
    return thread


def test_should_never_throttle_with_negative_target() -> None:
    throttler = ThroughputThrottler(-1, 0)
    assert not throttler.should_throttle(0, 0)
    assert not throttler.should_throttle(1_000_000, 10_000)


def test_should_not_throttle_when_no_time_has_elapsed() -> None:
    throttler = ThroughputThrottler(100, 1000)
    assert not throttler.should_throttle(10, 1000)


def test_should_throttle_when_rate_exceeds_target() -> None:
    # One second elapsed: the rate is the amount.
    throttler = ThroughputThrottler(100, 0)
    assert throttler.should_throttle(200, 1000)
    assert not throttler.should_throttle(50, 1000)
    # Exactly at the target is not over it.
    assert not throttler.should_throttle(100, 1000)


def test_should_throttle_with_zero_target() -> None:
    # 0 is not < 0, so the rate check applies: any positive rate throttles.
    throttler = ThroughputThrottler(0, 0)
    assert throttler.should_throttle(1, 1000)
    assert not throttler.should_throttle(0, 1000)
    assert not throttler.should_throttle(1, 0)


def test_sleep_time_is_ns_per_sec_over_target() -> None:
    assert ThroughputThrottler(1000, 0)._sleep_time_ns == NS_PER_SEC // 1000
    assert ThroughputThrottler(1, 0)._sleep_time_ns == NS_PER_SEC
    # Java's (long) cast truncates.
    assert ThroughputThrottler(3, 0)._sleep_time_ns == 333_333_333


def test_sleep_time_is_max_when_target_not_positive() -> None:
    assert ThroughputThrottler(0, 0)._sleep_time_ns == LONG_MAX
    assert ThroughputThrottler(-5, 0)._sleep_time_ns == LONG_MAX


def test_zero_target_blocks_until_wakeup() -> None:
    throttler = ThroughputThrottler(0, 0)
    thread = _throttle_in_thread(throttler)
    thread.join(0.2)
    assert thread.is_alive()
    throttler.wakeup()
    thread.join(5)
    assert not thread.is_alive()


def test_zero_target_returns_immediately_if_already_woken() -> None:
    # Java never resets the flag on this path, so it stays set.
    throttler = ThroughputThrottler(0, 0)
    throttler.wakeup()
    thread = _throttle_in_thread(throttler)
    thread.join(5)
    assert not thread.is_alive()
    thread = _throttle_in_thread(throttler)
    thread.join(5)
    assert not thread.is_alive()


def test_timed_path_sleeps_for_the_deficit() -> None:
    # 10/sec: each throttle() adds 100 ms of deficit and sleeps it off.
    throttler = ThroughputThrottler(10, 0)
    started = time.monotonic()
    throttler.throttle()
    assert time.monotonic() - started >= 0.09
    assert throttler._sleep_deficit_ns == 0


def test_deficit_below_minimum_sleep_is_carried() -> None:
    # 1000/sec: 1 ms a throttle(), under the 2 ms minimum, so the first call
    # does not sleep and the second sleeps both off.
    throttler = ThroughputThrottler(1000, 0)
    throttler.throttle()
    assert throttler._sleep_deficit_ns == 1_000_000
    throttler.throttle()
    assert throttler._sleep_deficit_ns == 0


def test_timed_path_sleeps_then_wakeup_interrupts() -> None:
    # 1/sec: one throttle() sleeps about 1 s unless woken.
    throttler = ThroughputThrottler(1, 0)
    started = time.monotonic()
    thread = _throttle_in_thread(throttler)
    time.sleep(0.05)
    throttler.wakeup()
    thread.join(5)
    assert not thread.is_alive()
    assert time.monotonic() - started < 0.5
    # The timed path resets the flag, so the next throttle() sleeps again.
    assert not throttler._wakeup


def test_timed_path_wakeup_before_throttle_is_not_lost() -> None:
    throttler = ThroughputThrottler(1, 0)
    throttler.wakeup()
    started = time.monotonic()
    throttler.throttle()
    assert time.monotonic() - started < 0.5
