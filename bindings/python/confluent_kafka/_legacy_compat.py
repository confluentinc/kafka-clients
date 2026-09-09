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

"""Private compatibility shims for the **paused** admin binding (``admin.py``).

The Python interface spec (``design/current/python-client-interface-spec.md``)
replaces the flat ``KafkaError`` (``code()`` / ``is_retriable()``) and the
positional ``Node`` / ``OffsetAndMetadata`` value types with the typed error
hierarchy (``confluent_kafka.common.errors``) and the keyword-only value types
(``confluent_kafka.common.Node`` / ``confluent_kafka.consumer.OffsetAndMetadata``).

The admin client is **paused** — its public surface (including the flat
``KafkaError`` values it returns per-key and its positional value types) must not
change (PLAN §"Out of scope"; the admin interface spec is paused). Rather than
keep the retired top-level ``producer.py`` / ``consumer.py`` modules alive only
for ``admin.py`` to import, the two things admin needs from them are moved here,
private to the package:

* the flat ``KafkaError`` with ``_from_c`` / ``_from_parts`` and the
  ``code`` / ``message`` / ``is_retriable`` / ``is_fatal`` accessors, and
* the positional ``Node`` / ``OffsetAndMetadata`` value types.

Nothing in the *new* ``confluent_kafka`` public surface imports this module. It
exists only so ``admin.py`` (and the gRPC admin translation that speaks admin's
frozen contract) keeps working after ``producer.py`` / ``consumer.py`` are
deleted. When the admin binding is un-paused and ported to the spec, this module
is deleted with it.
"""

from __future__ import annotations

from typing import Any

import _confluentkafka as _lib  # type: ignore[import-not-found]


class KafkaError(Exception):
    """Flat Kafka error with a wire code and retriable/fatal flags.

    The legacy admin error type. The new public surface uses the typed hierarchy
    in :mod:`confluent_kafka.common.errors`; this flat form is kept only for the
    paused admin client's frozen per-key result contract.
    """

    _code: int
    _message: str
    _is_retriable: bool
    _is_fatal: bool
    _txn_requires_abort: bool

    def __init__(self) -> None:
        raise NotImplementedError()

    def __str__(self) -> str:
        return self._message

    @staticmethod
    def _from_c(_id: int) -> KafkaError:
        ret = KafkaError.__new__(KafkaError)
        ret._code = _lib.KafkaError_code(_id)
        ret._message = _lib.KafkaError_message(_id)
        ret._is_retriable = _lib.KafkaError_is_retriable(_id)
        ret._is_fatal = _lib.KafkaError_is_fatal(_id)
        ret._txn_requires_abort = _lib.KafkaError_txn_requires_abort(_id)
        _lib.KafkaError_destroy(_id)
        return ret

    @staticmethod
    def _from_parts(code: int, message: str, is_retriable: int,
                    is_fatal: int) -> KafkaError:
        """Build a KafkaError from already-copied fields.

        Used for *borrowed* per-key errors inside an admin result handle: those
        die with their parent handle, so the C layer copies their fields out
        before destroying it and there is nothing left to ``KafkaError_destroy``.
        """
        ret = KafkaError.__new__(KafkaError)
        ret._code = code
        ret._message = message
        ret._is_retriable = bool(is_retriable)
        ret._is_fatal = bool(is_fatal)
        ret._txn_requires_abort = False
        return ret

    @property
    def code(self) -> int:
        return self._code

    @property
    def message(self) -> str:
        return self._message

    @property
    def is_retriable(self) -> bool:
        return self._is_retriable

    @property
    def is_fatal(self) -> bool:
        return self._is_fatal

    @property
    def txn_requires_abort(self) -> bool:
        return self._txn_requires_abort


class Node:
    """A Kafka broker node (positional, admin-legacy shape)."""

    __slots__ = ("id", "host", "port", "rack")

    def __init__(self, id: int, host: str, port: int,
                 rack: str | None = None) -> None:
        self.id = id
        self.host = host
        self.port = port
        self.rack = rack

    def __repr__(self) -> str:
        return f"Node(id={self.id}, host={self.host!r}, port={self.port}, rack={self.rack!r})"


class OffsetAndMetadata:
    """A committed offset with optional metadata and leader epoch (positional,
    admin-legacy shape)."""

    __slots__ = ("offset", "metadata", "leader_epoch")

    def __init__(self, offset: int, metadata: str = "",
                 leader_epoch: int | None = None) -> None:
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch

    def __eq__(self, other: Any) -> bool:
        return (isinstance(other, OffsetAndMetadata)
                and self.offset == other.offset
                and self.metadata == other.metadata
                and self.leader_epoch == other.leader_epoch)

    def __hash__(self) -> int:
        return hash((self.offset, self.metadata, self.leader_epoch))

    def __repr__(self) -> str:
        return (f"OffsetAndMetadata(offset={self.offset}, metadata={self.metadata!r}, "
                f"leader_epoch={self.leader_epoch})")
