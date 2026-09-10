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

"""Send-path helpers shared by the sync and async producers.

The C extension (``_confluentkafka.c``) owns the batching send path: it accepts
a native ``_confluentkafka.ProducerRecord`` (which holds a
``kafka_producer_ProducerRecord_t`` pointing at the caller's serialized ``bytes``
with no copy — C10), accumulates records, and its background poll task invokes a
Python callback ``cb(result, error)`` with the raw C handles as ints. These
helpers marshal those handles into the typed public objects
(:class:`RecordMetadata`, the typed error hierarchy) exactly once per completion.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, cast

import _confluentkafka as _lib  # type: ignore[import-not-found]

from confluent_kafka.common.errors import KafkaError, from_ffi_error

from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    # Java: org.apache.kafka.clients.producer.Callback.onCompletion(
    #           RecordMetadata metadata, Exception exception).
    DeliveryCallback = Callable[
        ["RecordMetadata | None", "KafkaError | None"], None]

_log = logging.getLogger("confluent_kafka")


def _metadata_from_ffi(handle: int) -> RecordMetadata:
    """Build the public :class:`RecordMetadata` from a C
    ``kafka_producer_RecordMetadata_t`` handle, consuming (destroying) it.

    Reads topic / partition / offset / timestamp / serialized key & value sizes
    through the C extension's ``RecordMetadata_copy_full`` native, which extracts
    every field in one call and frees the handle — no per-field FFI round trip.
    """
    fields = _lib.RecordMetadata_copy_full(handle)
    (topic, partition, offset, timestamp,
     serialized_key_size, serialized_value_size) = fields
    return RecordMetadata._from_ffi(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        serialized_key_size=serialized_key_size,
        serialized_value_size=serialized_value_size,
    )


def _completion_to_python(
        result: int, error: int
) -> tuple[RecordMetadata | None, KafkaError | None]:
    """Convert the raw C completion handles into owned Python objects.

    Called exactly once per completion, before any branching, so ownership of
    both handles is transferred into Python objects that free themselves —
    :func:`from_ffi_error` destroys the error handle, and
    :func:`_metadata_from_ffi` destroys the metadata handle. Every downstream
    branch can then use or ignore the objects with no double-free or leak.
    """
    metadata = _metadata_from_ffi(result) if result != 0 else None
    # A delivery failure always crosses as a Kafka error (a wire / producer
    # error, never a JDK analog), so the base KafkaError type is exact here.
    exception = (cast("KafkaError", from_ffi_error(error))
                 if error != 0 else None)
    return metadata, exception


def _invoke_on_delivery(
        on_delivery: DeliveryCallback | None,
        metadata: RecordMetadata | None,
        exception: KafkaError | None,
) -> None:
    """Invoke a user delivery callback, shielding the C caller from it.

    Mirrors Java's ``Callback.onCompletion(RecordMetadata, Exception)``: exactly
    one of the two arguments is meaningful (``metadata`` on success,
    ``exception`` on failure) and the callback returns nothing. Java's contract
    is that the callback fires exactly once per record, so it is invoked here on
    *every* completion path.

    An exception raised by the callback is logged and swallowed. It must not
    propagate: the caller is a background completion thread (or the event loop
    drain), and the record's future has already been resolved by then — Java's
    contract that a raising callback does not affect the producer (spec §7.1)."""
    if on_delivery is None:
        return
    try:
        on_delivery(metadata, exception)
    except Exception:  # noqa: BLE001 - user callback, must not escape
        _log.exception("Error in on_delivery callback")
