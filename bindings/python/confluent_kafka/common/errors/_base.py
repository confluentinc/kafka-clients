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

"""The base of the Kafka exception hierarchy, ``KafkaError``.

Hand-written (not generated) so both the generated classes
(``common/errors/_generated.py``) and the runtime mapping
(``common/errors/__init__.py``) can import it without a cycle. It mirrors Java's
``org.apache.kafka.common.KafkaException``: message and cause only — **no**
``code()``, and **no** ``is_retriable()`` / ``is_fatal()`` /
``txn_requires_abort()`` on the public surface (Design Decisions D1; those Rust
predicates are expressed here through the type hierarchy — ``except
RetriableError``). The FFI id lives in a private ``_ffi_id`` class attribute on
each generated subclass, used only internally to pick which class to raise.
"""

from __future__ import annotations

from typing import ClassVar


class KafkaError(Exception):
    """Base of the Kafka error hierarchy — Java's ``KafkaException``.

    Caught broadly with ``except KafkaError``; caught narrowly by a dedicated
    subclass (``except TopicAuthorizationError``). Carries a message and, through
    Python's native chaining (``raise ... from cause``), a cause — exactly Java's
    ``getMessage()`` / ``getCause()``. It has no error code or predicate methods on
    the public surface (Design Decisions D1).
    """

    # The base itself is raised only as the no-mapping fallback (an FFI id with no
    # dedicated class); it inherits ``UNKNOWN_SERVER_ERROR`` for round-tripping.
    _ffi_id: ClassVar[int] = -1  # kafka_common_ErrorCode_UNKNOWN_SERVER_ERROR

    def __str__(self) -> str:
        # ``KafkaError(message)`` -> ``message``; matches Java's ``getMessage()``.
        return "" if not self.args else str(self.args[0])
