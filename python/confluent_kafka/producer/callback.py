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

"""``Callback``: Java's ``org.apache.kafka.clients.producer.Callback``.

A single-method callback interface, so a ``Callable`` alias of the interface's
name (CLAUDE.md, Python Binding Conventions, Idiom translations), called
positionally as Java's ``onCompletion(RecordMetadata metadata, Exception
exception)``.

A callback interface that the user can implement to allow code to execute when
the request is complete. This callback will generally execute in the background
I/O thread so it should be fast: on ``KafkaProducer`` it runs on the producer's
background completion thread, on ``AsyncKafkaProducer`` on the event loop, one
at a time in completion order; an exception it raises is logged, not propagated
(Threads and callbacks).

The callback is called when the record sent to the server has been
acknowledged. ``metadata`` is the metadata for the record that was sent (i.e.
the partition and offset); it is never ``None``: when ``exception`` is not
``None`` it carries the special -1 value for every field but the topic and the
partition, and -1 as the partition if none could be chosen (Java builds
``RecordMetadata(tp, -1, -1, NO_TIMESTAMP, -1, -1)``). ``exception`` is the
exception thrown during processing of this record, ``None`` if no error occurred.
"""

from __future__ import annotations

from typing import Callable

from .record_metadata import RecordMetadata

__all__ = ["Callback"]

#: Java's ``Callback.onCompletion(RecordMetadata metadata, Exception exception)``.
Callback = Callable[[RecordMetadata, Exception | None], None]
