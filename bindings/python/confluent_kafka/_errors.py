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

"""Errors across the FFI: core -> Python and Python -> core (private).

CLAUDE.md, Python Binding Conventions, Errors:

- Core -> Python (:func:`from_ffi_error`): construct the class of the error's
  FFI id, with the payload read from the FFI accessors, chained from its cause
  (``kafka_common_Error_source``); an unknown id raises the base ``KafkaError``.
- Python -> core (:func:`to_ffi_id`): ``type(error)._ffi_id``. The base
  ``KafkaError`` carries ``UNKNOWN_SERVER_ERROR`` (-1), which
  ``UnknownServerError`` owns in the ``id -> class`` table; an error class
  without an id (``NullPointerError``, a user's own exception) reports -1 too.

The ``id -> class`` table is derived from the generated registry, every class
with its own ``_ffi_id``.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any, cast

from ._error_registry import ERRORS
from ._throwable import new

try:  # pragma: no cover - import guard
    import _confluentkafka as _lib  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - the native extension is optional here
    _lib = None

__all__ = ["class_for_ffi_id", "error_classes", "from_ffi_error", "to_ffi_id"]

#: The FFI id the core reports for a bare ``KafkaException``.
UNKNOWN_SERVER_ERROR = -1

_classes: list[type[BaseException]] | None = None
_by_id: dict[int, type[BaseException]] | None = None


def error_classes() -> list[type[BaseException]]:
    """Every generated error class, in registry order."""
    global _classes
    if _classes is None:
        _classes = [getattr(importlib.import_module(module), name)
                    for module, name, _java in ERRORS]
    return _classes


def _table() -> dict[int, type[BaseException]]:
    global _by_id
    if _by_id is None:
        from .common.kafka_error import KafkaError

        table: dict[int, type[BaseException]] = {}
        for cls in error_classes():
            if cls is KafkaError or "_ffi_id" not in cls.__dict__:
                continue
            ffi_id = cls.__dict__["_ffi_id"]
            if ffi_id in table:
                raise RuntimeError(
                    f"duplicate _ffi_id {ffi_id}: {table[ffi_id].__name__} and {cls.__name__}")
            table[ffi_id] = cls
        _by_id = table
    return _by_id


def class_for_ffi_id(ffi_id: int) -> type[BaseException]:
    """The class of an FFI id; the base ``KafkaError`` when the id is unknown."""
    from .common.kafka_error import KafkaError

    return _table().get(ffi_id, KafkaError)


def to_ffi_id(error: BaseException) -> int:
    """The FFI id the core reports for ``error`` (mock injection, a raising
    listener): ``type(error)._ffi_id``, or ``UNKNOWN_SERVER_ERROR`` (-1) for a
    class without one."""
    ffi_id = getattr(type(error), "_ffi_id", UNKNOWN_SERVER_ERROR)
    return int(ffi_id)


def reported_ffi_id(error: BaseException) -> int:
    """The id the core reports back for an error injected with
    :func:`to_ffi_id`: the core builds an injected error from a Kafka protocol
    code, so a negative (client-side) id comes back as ``UNKNOWN_SERVER_ERROR``."""
    ffi_id = to_ffi_id(error)
    return ffi_id if ffi_id >= 0 else UNKNOWN_SERVER_ERROR


# The payload of a core error, read by the C extension (``KafkaError_payload``),
# turned into the keyword arguments of the Java constructor that takes it.

def _tp(raw: tuple[str, int]) -> Any:
    from .common.topic_partition import TopicPartition

    return TopicPartition(topic=raw[0], partition=raw[1])


def _oam(raw: tuple[int, str, int | None]) -> Any:
    from .consumer.offset_and_metadata import OffsetAndMetadata

    offset, metadata, epoch = raw
    return OffsetAndMetadata(offset=offset, leader_epoch=epoch, metadata=metadata)


def _long_map(raw: dict[tuple[str, int], int] | None) -> dict[Any, int] | None:
    return None if raw is None else {_tp(k): v for k, v in raw.items()}


def _metric(payload: dict[str, Any]) -> Any:
    from .common.kafka_metric import KafkaMetric

    return KafkaMetric._snapshot(
        name=payload.get("metric_name") or "",
        group=payload.get("metric_group") or "",
        value=payload.get("value"),
    )


_CORE_KWARGS: dict[str, Callable[[str | None, dict[str, Any]], dict[str, Any]]] = {
    "TopicAuthorizationError": lambda m, p: {
        "message": m, "unauthorized_topics": set(p["unauthorized_topics"])},
    "GroupAuthorizationError": lambda m, p: {"message": m, "group_id": p.get("group_id")},
    "InvalidTopicError": lambda m, p: {"message": m, "invalid_topics": set(p["invalid_topics"])},
    "ThrottlingQuotaExceededError": lambda m, p: {
        "throttle_time_ms": p["throttle_time_ms"], "message": m},
    # Java's QuotaViolationException(KafkaMetric, double, double) has no message.
    "QuotaViolationError": lambda m, p: {
        "metric": _metric(p), "value": p["value"], "bound": p["bound"]},
    "LogTruncationError": lambda m, p: {
        "message": m,
        "fetch_offsets": _long_map(p["offset_out_of_range_partitions"]),
        "divergent_offsets": {_tp(k): _oam(v) for k, v in p["divergent_offsets"].items()},
    },
    "OffsetOutOfRangeError": lambda m, p: {
        "message": m,
        "offset_out_of_range_partitions": _long_map(p["offset_out_of_range_partitions"]),
    },
    # Java's NoOffsetForPartitionException(Collection) builds its own message.
    "NoOffsetForPartitionError": lambda m, p: {"partitions": {_tp(t) for t in p["partitions"]}},
    "RecordTooLargeError": lambda m, p: {
        "message": m,
        "record_too_large_partitions": _long_map(p.get("record_too_large_partitions")),
    },
    "DuplicateResourceError": lambda m, p: {"resource": p.get("resource"), "message": m},
    "ResourceNotFoundError": lambda m, p: {"resource": p.get("resource"), "message": m},
    "CorrelationIdMismatchError": lambda m, p: {
        "message": m,
        "request_correlation_id": p["request_correlation_id"],
        "response_correlation_id": p["response_correlation_id"],
    },
}


def construct(cls: type[BaseException], message: str | None,
              payload: dict[str, Any] | None) -> BaseException:
    """An instance of ``cls`` for an error the core reported, built through
    its Java constructor; a class whose constructors cannot take what the core
    reported (a ``RecordDeserializationError`` without its record) carries the
    message only."""
    builder = _CORE_KWARGS.get(cls.__name__)
    make = cast(Any, cls)
    try:
        error: BaseException
        if builder is not None and payload:
            error = make(**builder(message, payload))
        else:
            error = make(message=message)
    except (TypeError, RuntimeError):
        error = new(cls, message)
    return error


def from_ffi_error(handle: int, *, cause: BaseException | None = None) -> BaseException:
    """The Python error for a C ``kafka_common_Error_t`` handle (consumed).

    The class is the one of the error's FFI id (unknown -> ``KafkaError``),
    built with the payload the FFI accessors expose. The cause chain
    (``kafka_common_Error_source``) becomes ``__cause__``; an explicit
    ``cause`` wins. The error is returned, so the caller can
    ``raise from_ffi_error(handle)``.
    """
    if _lib is None:
        raise RuntimeError(
            "the _confluentkafka native extension is not loaded; from_ffi_error "
            "needs it to read the error handle")
    ffi_id: int = _lib.KafkaError_code(handle)
    message: str = _lib.KafkaError_message(handle)
    source_cause: BaseException | None = None
    if cause is None:
        error_source = getattr(_lib, "KafkaError_source", None)
        if error_source is not None:
            source_handle: int = error_source(handle)
            if source_handle:
                source_cause = from_ffi_error(source_handle)
    payload: dict[str, Any] | None = None
    error_payload = getattr(_lib, "KafkaError_payload", None)
    if error_payload is not None:
        payload = error_payload(handle)
    _lib.KafkaError_destroy(handle)
    error = construct(class_for_ffi_id(ffi_id), message, payload)
    if cause is not None:
        error.__cause__ = cause
    elif source_cause is not None:
        error.__cause__ = source_cause
    return error
