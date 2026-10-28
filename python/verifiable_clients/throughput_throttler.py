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

"""Translated from ``org.apache.kafka.server.util.ThroughputThrottler``.

Part of the verifiable clients as the producer's one dependency outside the
client: Java keeps it in ``server-common``, which has no Python counterpart.
"""

from __future__ import annotations

import ctypes
import threading
import time

__all__ = ["ThroughputThrottler"]

_NS_PER_MS = 1_000_000
_NS_PER_SEC = 1000 * _NS_PER_MS
_MIN_SLEEP_NS = 2 * _NS_PER_MS
_LONG_MAX = 2**63 - 1


def _float32(value: float) -> float:
    """``value`` rounded to a Java ``float``."""
    return ctypes.c_float(value).value


class ThroughputThrottler:
    """This class helps producers throttle throughput.

    If ``target_throughput >= 0``, the resulting average throughput will be
    approximately ``min(target_throughput, maximum_possible_throughput)``. If
    ``target_throughput < 0``, no throttling will occur.

    To use, do this between successive send attempts::

        if throttler.should_throttle(amount_so_far, send_start_ms):
            throttler.throttle()

    Note that this can be used to throttle message throughput or data
    throughput.

    Java's ``synchronized`` / ``wait`` / ``notifyAll`` on the instance is a
    ``threading.Condition``. Java's ``InterruptedException`` paths have no
    counterpart: a Python thread cannot be interrupted, so ``throttle()``
    returns only when its sleep ends or ``wakeup()`` is called.
    """

    def __init__(self, target_throughput: float, start_ms: int) -> None:
        """``target_throughput`` can be messages/sec or bytes/sec;
        ``start_ms`` is when the very first message is sent."""
        self._start_ms = start_ms
        self._target_throughput = target_throughput
        self._sleep_time_ns = (int(_NS_PER_SEC / target_throughput)
                               if target_throughput > 0 else _LONG_MAX)
        self._sleep_deficit_ns = 0
        self._wakeup = False
        self._cond = threading.Condition()

    def should_throttle(self, amount_so_far: int, send_start_ms: int) -> bool:
        """``amount_so_far`` is the bytes produced so far if you want to
        throttle data throughput, or the messages produced so far if you want
        to throttle message throughput; ``send_start_ms`` is the timestamp of
        the most recently sent message. Returns ``True`` if throttling should
        happen."""
        if self._target_throughput < 0:
            # No throttling in this case
            return False

        # Java computes the elapsed seconds as a float: (sendStartMs - startMs) / 1000.f
        elapsed_sec = _float32(_float32(send_start_ms - self._start_ms) / 1000.0)
        return elapsed_sec > 0 and (amount_so_far / elapsed_sec) > self._target_throughput

    def throttle(self) -> None:
        """Occasionally blocks for small amounts of time to achieve
        ``target_throughput``.

        Note that if ``target_throughput`` is 0, this will block extremely
        aggressively.
        """
        if self._target_throughput == 0:
            with self._cond:
                while not self._wakeup:
                    self._cond.wait()
            return

        # throttle throughput by sleeping, on average,
        # (1 / self.throughput) seconds between "things sent"
        self._sleep_deficit_ns += self._sleep_time_ns

        # If enough sleep deficit has accumulated, sleep a little
        if self._sleep_deficit_ns >= _MIN_SLEEP_NS:
            sleep_start_ns = time.monotonic_ns()
            with self._cond:
                remaining = self._sleep_deficit_ns
                while not self._wakeup and remaining > 0:
                    self._cond.wait(remaining / _NS_PER_SEC)
                    elapsed = time.monotonic_ns() - sleep_start_ns
                    remaining = self._sleep_deficit_ns - elapsed
                self._wakeup = False
            self._sleep_deficit_ns = 0

    def wakeup(self) -> None:
        """Wakeup the throttler if it is sleeping."""
        with self._cond:
            self._wakeup = True
            self._cond.notify_all()
