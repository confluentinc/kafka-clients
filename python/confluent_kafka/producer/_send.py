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

"""Send-completion helpers shared by the producers (private).

The C extension hands each record to the C API's queued send
(``kafka_producer_Producer_send_with_callback_cb``) with a delivery callback,
which the producer's callback pump (``kafka_producer_Producer__execute_callbacks``)
runs as ``on_completion(metadata, error)``: the metadata's fields as a tuple
(``None`` when there is none) and the BORROWED error handle as an int (0 for
none). :func:`completion_to_python` turns them into the public objects once
per completion; :func:`invoke_callback` runs the user's ``Callback`` (CLAUDE.md,
Python Binding Conventions, Threads and callbacks).
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, cast

from confluent_kafka._errors import from_ffi_error
from confluent_kafka.common.topic_partition import TopicPartition

from .record_metadata import RecordMetadata

if TYPE_CHECKING:
    from .callback import Callback

__all__ = ["MetadataTuple", "completion_to_python", "failure_metadata", "in_callback", "invoke_callback"]

_LOG = logging.getLogger("confluent_kafka.producer")

# The producer whose delivery callback runs on the current thread, if any: Java
# tests ``Thread.currentThread() == this.ioThread`` in ``flush()`` and
# ``close()``, and a callback is the only user code on the producer's threads.
_callback_scope = threading.local()


def failure_metadata(topic: str, partition: int | None) -> RecordMetadata:
    """The metadata Java hands a callback on failure: ``RecordMetadata(tp, -1,
    -1, RecordBatch.NO_TIMESTAMP, -1, -1)``, the partition -1 when none was
    chosen (``KafkaProducer.AppendCallbacks.onCompletion``)."""
    tp = TopicPartition(topic=topic, partition=-1 if partition is None else partition)
    return RecordMetadata(topic_partition=tp, base_offset=-1, batch_index=-1, timestamp=-1,
                          serialized_key_size=-1, serialized_value_size=-1)


# The delivery callback's metadata as the C extension passes it: (topic,
# partition, offset, timestamp, serialized key size, serialized value size),
# offset and timestamp -1 when absent.
MetadataTuple = tuple[str, int, int, int, int, int]


def completion_to_python(metadata_tuple: MetadataTuple | None, error: int, topic: str,
                         partition: int | None) -> tuple[RecordMetadata, Exception | None]:
    """The ``(metadata, exception)`` of a completion, from what the delivery
    callback received: the metadata tuple and the BORROWED error handle, which
    is read here and never freed (its owner, the C API, frees it once the
    callback returns).

    Both may be given: a failure the core raises before the record reaches a
    batch carries Java's -1 metadata beside the error. Its partition is the one
    Java's ``AppendCallbacks.topicPartition()`` gives: the partition chosen for
    the record, else the record's own, else -1. The core reports -1 for a
    failure before the partition is chosen even when the record names one, so
    the record's own is filled in here. A failure with no metadata (a
    broker-side or delivery failure) gets :func:`failure_metadata`, as Java's
    ``AppendCallbacks`` replaces the null metadata of
    ``ProducerBatch.completeFutureAndFireCallbacks``; the FFI does not report
    the partition a batch had, so it is the record's own, or -1.
    """
    exception = cast("Exception", from_ffi_error(error, owned=False)) if error else None
    if metadata_tuple is None:
        return failure_metadata(topic, partition), exception
    (topic_, partition_, offset, timestamp, key_size, value_size) = metadata_tuple
    if exception is not None and partition_ == -1 and partition is not None:
        partition_ = partition
    metadata = RecordMetadata._from_ffi(
        topic=topic_, partition=partition_, offset=offset, timestamp=timestamp,
        serialized_key_size=key_size, serialized_value_size=value_size)
    return metadata, exception


def invoke_callback(producer: object, callback: Callback | None,
                    metadata: RecordMetadata, exception: Exception | None) -> None:
    """Run the user's ``Callback`` for one completion.

    An exception it raises is logged, not propagated, as Java's
    ``ProducerBatch.completeFutureAndFireCallbacks`` logs it: the caller is the
    producer's completion thread or the event loop, and the next callback must
    still run. While it runs, :func:`in_callback` is true for ``producer`` on
    this thread.
    """
    if callback is None:
        return
    previous = getattr(_callback_scope, "producer", None)
    _callback_scope.producer = producer
    try:
        callback(metadata, exception)
    except Exception:  # noqa: BLE001 - a user callback must not escape
        _LOG.exception("Error executing user-provided callback on message for "
                       "topic-partition '%s'",
                       f"{metadata.topic()}-{metadata.partition()}")
    finally:
        _callback_scope.producer = previous


def in_callback(producer: object) -> bool:
    """Whether the current thread is running a delivery callback of
    ``producer`` (Java's ``Thread.currentThread() == this.ioThread``)."""
    return getattr(_callback_scope, "producer", None) is producer
