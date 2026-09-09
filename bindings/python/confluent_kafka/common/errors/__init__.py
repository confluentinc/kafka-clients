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

"""``confluent_kafka.common.errors`` — the typed Kafka error hierarchy.

Re-exports the hand-written base :class:`KafkaError` (``_base``) and every
generated ``…Error`` class (``_generated``), and provides the internal FFI
conversion (Design Decisions D1):

- :func:`from_ffi_error` — core → Python: read the error's FFI id and message
  through the C extension, pick the class from the ``id → class`` table
  (unknown id → base :class:`KafkaError`), construct it, and chain the cause
  (``raise cls(msg) from cause``).
- :func:`to_ffi_id` — Python → core (mock injection): ``type(error)._ffi_id``.

No ``code()`` and no ``is_retriable()`` / ``is_fatal()`` /
``txn_requires_abort()`` on the public surface: retriability/fatality is
expressed through the type hierarchy (``except RetriableError``), and the FFI id
stays private (``_ffi_id``).
"""

from __future__ import annotations

from . import _generated
from ._base import KafkaError
from ._generated import *  # noqa: F401,F403 -- re-export the whole hierarchy

# The C extension owns the ``kafka_common_Error_*`` accessors. It is imported
# lazily so this module (and its ``id -> class`` table) is usable in contexts
# where the native extension is not loaded (pure hierarchy tests, type checking).
try:  # pragma: no cover - import guard
    import _confluentkafka as _lib  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - the native extension is optional here
    _lib = None

__all__ = ["KafkaError", "from_ffi_error", "to_ffi_id", *_generated.__all__]


def _build_by_ffi_id() -> dict[int, type[BaseException]]:
    """The ``id -> class`` table, derived from every generated class' ``_ffi_id``.

    Covers all three generated packages that carry ``_ffi_id`` — ``common.errors``
    (here), the consumer package, the config package — and the root JDK analogs.
    Abstract catch-only bases carry no ``_ffi_id`` and are excluded.
    """
    from confluent_kafka import _generated_errors as _root_errors
    from confluent_kafka.common.config import _generated_errors as _config_errors
    from confluent_kafka.consumer import _generated_errors as _consumer_errors

    table: dict[int, type[BaseException]] = {}
    modules = (_generated, _root_errors, _config_errors, _consumer_errors)
    for module in modules:
        for name in module.__all__:
            cls = getattr(module, name)
            ffi_id = getattr(cls, "_ffi_id", None)
            # Only concrete classes carry an ``_ffi_id``; skip abstract bases (whose
            # ``_ffi_id`` is inherited from a concrete ancestor, not their own).
            if ffi_id is None or "_ffi_id" not in cls.__dict__:
                continue
            if ffi_id in table:
                raise RuntimeError(
                    f"duplicate _ffi_id {ffi_id}: {table[ffi_id].__name__} and {cls.__name__}"
                )
            table[ffi_id] = cls
    return table


# One table shared by both conversion directions, built lazily on first use so
# importing this package does not eagerly import the sibling generated modules
# (which import back into this package — a cycle at import time). Exposed as the
# module attribute ``_BY_FFI_ID`` (Design Decisions D1) via ``__getattr__``.
_by_ffi_id_cache: dict[int, type[BaseException]] | None = None


def _by_ffi_id() -> dict[int, type[BaseException]]:
    global _by_ffi_id_cache
    if _by_ffi_id_cache is None:
        _by_ffi_id_cache = _build_by_ffi_id()
    return _by_ffi_id_cache


def __getattr__(name: str) -> object:
    # ``_BY_FFI_ID`` is materialised on first access to keep import cheap and
    # cycle-free; every other missing attribute is a real error.
    if name == "_BY_FFI_ID":
        return _by_ffi_id()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _class_for_ffi_id(ffi_id: int) -> type[BaseException]:
    """The Python class for an FFI id; the base :class:`KafkaError` when unknown
    (a generation-lag fallback — never a ``KeyError``, Design Decisions D1 item 8).
    """
    return _by_ffi_id().get(ffi_id, KafkaError)


def from_ffi_error(handle: int, *, cause: BaseException | None = None) -> BaseException:
    """Build the Python error for a C ``kafka_common_Error_t`` handle.

    Reads the error's FFI id (``kafka_common_Error_code``) and message
    (``kafka_common_Error_message``) through the C extension, picks the class from
    the ``id -> class`` table (unknown id → base :class:`KafkaError`), and
    constructs it with the message. The returned exception is *returned*, not
    raised, so the caller can ``raise from_ffi_error(handle) from cause`` to
    preserve Java's ``getCause()`` chain.

    The handle is consumed (destroyed) here, mirroring the existing
    ``KafkaError._from_c`` contract.
    """
    if _lib is None:
        raise RuntimeError(
            "the _confluentkafka native extension is not loaded; from_ffi_error "
            "needs it to read the error handle"
        )
    ffi_id: int = _lib.KafkaError_code(handle)
    message: str = _lib.KafkaError_message(handle)
    _lib.KafkaError_destroy(handle)
    cls = _class_for_ffi_id(ffi_id)
    error = cls(message)
    if cause is not None:
        error.__cause__ = cause
    return error


def to_ffi_id(error: BaseException) -> int:
    """The FFI id for a Python error, for mock injection into the core.

    ``type(error)._ffi_id`` — the inverse of :func:`from_ffi_error`.

    The base :class:`KafkaError` carries no ``_ffi_id`` (it is only the no-mapping
    fallback, never injected — Critic 64 F2), so passing a bare ``KafkaError``
    raises ``TypeError`` rather than silently coercing to a code. To inject the
    catch-all wire error, use its concrete class ``UnknownServerError``. Any error
    class without an ``_ffi_id`` (a non-Kafka/non-JDK exception, or the base) is
    likewise rejected.
    """
    ffi_id: int | None = getattr(type(error), "_ffi_id", None)
    if ffi_id is None:
        raise TypeError(
            f"{type(error).__name__} has no _ffi_id; only generated Kafka/JDK error "
            "classes can be injected into the core"
        )
    return ffi_id
