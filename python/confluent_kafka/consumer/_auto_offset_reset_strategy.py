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

"""``AutoOffsetResetStrategy``: Java's
``org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy`` (private).

An ``internals`` class, so not public (CLAUDE.md, Python Binding Conventions,
Scope); ``MockConsumer`` parses its ``offset_reset_strategy`` with it, as Java's
mock does. Only what the mock reads is translated: ``fromString``, ``type()``
and the three constants.
"""

from __future__ import annotations

import re
from enum import Enum

from confluent_kafka.illegal_argument_error import IllegalArgumentError

__all__ = ["AutoOffsetResetStrategy"]

# Java's java.time.Duration.parse pattern (Duration.java, Lazy.PATTERN).
_DURATION = re.compile(
    r"([-+]?)P(?:([-+]?[0-9]+)D)?"
    r"(T(?:([-+]?[0-9]+)H)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?",
    re.IGNORECASE)

_LONG_MAX = (1 << 63) - 1


def _parse_duration(text: str) -> float:
    """``java.time.Duration.parse(text)`` in seconds; raises ``ValueError``
    where Java throws ``DateTimeParseException`` / ``ArithmeticException``."""
    match = _DURATION.fullmatch(text)
    if match is None or match.group(3) == "T" or not any(
            match.group(i) is not None for i in (2, 4, 5, 6)):
        raise ValueError(f"Text cannot be parsed to a Duration: {text}")
    days, hours, minutes, seconds = (int(match.group(i) or 0) for i in (2, 4, 5, 6))
    fraction = match.group(7) or ""
    nanos = int((fraction + "000000000")[:9])
    if (match.group(6) or "").startswith("-"):
        nanos = -nanos
    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    if abs(total) > _LONG_MAX:
        raise ValueError(f"Text cannot be parsed to a Duration: {text}")
    value = total + nanos / 1e9
    return -value if match.group(1) == "-" else value


class AutoOffsetResetStrategy:
    """Java's ``AutoOffsetResetStrategy``: a strategy type and, for
    ``by_duration``, its duration (seconds)."""

    class StrategyType(Enum):
        """Java's nested ``StrategyType``; ``__str__`` is Java's lower-case
        ``toString()``."""

        LATEST = "LATEST"
        EARLIEST = "EARLIEST"
        NONE = "NONE"
        BY_DURATION = "BY_DURATION"

        def __str__(self) -> str:
            return self.name.lower()

    EARLIEST: AutoOffsetResetStrategy
    LATEST: AutoOffsetResetStrategy
    NONE: AutoOffsetResetStrategy

    __slots__ = ("_type", "_duration")

    def __init__(self, strategy_type: AutoOffsetResetStrategy.StrategyType,
                 duration: float | None = None) -> None:
        self._type = strategy_type
        self._duration = duration

    @staticmethod
    def from_string(offset_strategy: str | None) -> AutoOffsetResetStrategy:
        """Java's ``fromString(String)``, with its messages."""
        by_duration = str(AutoOffsetResetStrategy.StrategyType.BY_DURATION)
        if offset_strategy is None:
            raise IllegalArgumentError(message="Auto offset reset strategy is null")
        if offset_strategy == by_duration:
            raise IllegalArgumentError(
                message="<:duration> part is missing in by_duration auto offset reset strategy.")
        options = [str(t) for t in AutoOffsetResetStrategy.StrategyType]
        if offset_strategy in options:
            return {
                "earliest": AutoOffsetResetStrategy.EARLIEST,
                "latest": AutoOffsetResetStrategy.LATEST,
                "none": AutoOffsetResetStrategy.NONE,
            }[offset_strategy]
        if offset_strategy.startswith(by_duration + ":"):
            iso_duration = offset_strategy[len(by_duration) + 1:]
            try:
                duration = _parse_duration(iso_duration)
                if duration < 0:
                    raise ValueError(
                        "Negative duration is not supported in by_duration offset reset strategy.")
            except ValueError as e:
                # Java's catch (Exception e) wraps the negative-duration check too.
                raise IllegalArgumentError(
                    message="Unable to parse duration string in by_duration offset reset strategy.",
                    cause=e) from e
            return AutoOffsetResetStrategy(AutoOffsetResetStrategy.StrategyType.BY_DURATION, duration)
        raise IllegalArgumentError(message="Unknown auto offset reset strategy: " + offset_strategy)

    def type(self) -> AutoOffsetResetStrategy.StrategyType:
        """Java's ``type()``."""
        return self._type

    def duration(self) -> float | None:
        """Java's ``duration()``, in seconds."""
        return self._duration


AutoOffsetResetStrategy.EARLIEST = AutoOffsetResetStrategy(AutoOffsetResetStrategy.StrategyType.EARLIEST)
AutoOffsetResetStrategy.LATEST = AutoOffsetResetStrategy(AutoOffsetResetStrategy.StrategyType.LATEST)
AutoOffsetResetStrategy.NONE = AutoOffsetResetStrategy(AutoOffsetResetStrategy.StrategyType.NONE)
