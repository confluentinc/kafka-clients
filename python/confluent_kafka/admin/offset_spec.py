# Copyright 2026 Confluent Inc.
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

"""``OffsetSpec``: Java's ``org.apache.kafka.clients.admin.OffsetSpec``.

Java's public nested subclasses ``EarliestSpec``, ``LatestSpec``,
``MaxTimestampSpec``, ``EarliestLocalSpec``, ``LatestTieredSpec``,
``EarliestPendingUploadSpec`` and ``TimestampSpec`` are nested classes of
``OffsetSpec`` that subclass it (CLAUDE.md, Python Binding Conventions, Idiom
translations). A nested class cannot subclass the class being
defined, so they are declared inside ``OffsetSpec`` for the type checker and
created right after it at run time; ``OffsetSpec.EarliestSpec.__qualname__`` is
``"OffsetSpec.EarliestSpec"``. ``OffsetSpec`` and every nested class but
``TimestampSpec`` have Java's implicit public no-argument constructor;
``TimestampSpec``'s constructor and its ``timestamp()`` are package-private, so
``OffsetSpec.TimestampSpec()`` raises ``TypeError`` naming
``OffsetSpec.for_timestamp(...)``, and the timestamp is the private
``_timestamp`` the client reads. Java declares no ``equals`` / ``hashCode`` /
``toString``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["OffsetSpec"]


class OffsetSpec:
    """This class allows to specify the desired offsets when using the admin
    client's ``list_offsets()``.

    Java: ``org.apache.kafka.clients.admin.OffsetSpec``.
    """

    __slots__ = ()

    if TYPE_CHECKING:
        class EarliestSpec(OffsetSpec): ...

        class LatestSpec(OffsetSpec): ...

        class MaxTimestampSpec(OffsetSpec): ...

        class EarliestLocalSpec(OffsetSpec): ...

        class LatestTieredSpec(OffsetSpec): ...

        class EarliestPendingUploadSpec(OffsetSpec): ...

        class TimestampSpec(OffsetSpec):
            _timestamp: int

            def __init__(self, *args: Any, **kwargs: Any) -> None: ...

            @classmethod
            def _new(cls, *, timestamp: int) -> OffsetSpec.TimestampSpec: ...

    @staticmethod
    def latest() -> OffsetSpec:
        """Used to retrieve the latest offset of a partition."""
        return OffsetSpec.LatestSpec()

    @staticmethod
    def earliest() -> OffsetSpec:
        """Used to retrieve the earliest offset of a partition."""
        return OffsetSpec.EarliestSpec()

    @staticmethod
    def for_timestamp(*, timestamp: int) -> OffsetSpec:
        """Used to retrieve the earliest offset whose timestamp is greater than
        or equal to the given timestamp (in milliseconds) in the corresponding
        partition."""
        return OffsetSpec.TimestampSpec._new(timestamp=timestamp)

    @staticmethod
    def max_timestamp() -> OffsetSpec:
        """Used to retrieve the offset with the largest timestamp of a
        partition as message timestamps can be specified client side this may
        not match the log end offset returned by ``LatestSpec``."""
        return OffsetSpec.MaxTimestampSpec()

    @staticmethod
    def earliest_local() -> OffsetSpec:
        """Used to retrieve the local log start offset. Local log start offset
        is the offset of a log above which reads are guaranteed to be served
        from the disk of the leader broker.

        Note: When tiered Storage is not enabled, it behaves the same as
        retrieving the earliest timestamp offset.
        """
        return OffsetSpec.EarliestLocalSpec()

    @staticmethod
    def latest_tiered() -> OffsetSpec:
        """Used to retrieve the highest offset of data stored in remote
        storage.

        Note: When tiered storage is not enabled, we will return unknown
        offset.
        """
        return OffsetSpec.LatestTieredSpec()

    @staticmethod
    def earliest_pending_upload() -> OffsetSpec:
        """Used to retrieve the earliest offset of records that are pending
        upload to remote storage.

        Note: When tiered storage is not enabled, we will return unknown
        offset.
        """
        return OffsetSpec.EarliestPendingUploadSpec()


if not TYPE_CHECKING:
    def _nested(name: str, body: dict[str, Any]) -> type:
        cls = type(name, (OffsetSpec,), {"__slots__": (), "__module__": __name__,
                                         "__qualname__": "OffsetSpec." + name, **body})
        setattr(OffsetSpec, name, cls)
        return cls

    for _name in ("EarliestSpec", "LatestSpec", "MaxTimestampSpec", "EarliestLocalSpec",
                  "LatestTieredSpec", "EarliestPendingUploadSpec"):
        _nested(_name, {})

    def _timestamp_spec_init(self: Any, *args: Any, **kwargs: Any) -> None:
        raise TypeError("OffsetSpec.TimestampSpec cannot be constructed directly; use "
                        "OffsetSpec.for_timestamp(...)")

    def _timestamp_spec_new(cls: Any, *, timestamp: int) -> Any:
        """Java's package-private ``TimestampSpec(long timestamp)``."""
        spec = cls.__new__(cls)
        spec._timestamp = timestamp
        return spec

    _nested("TimestampSpec", {"__slots__": ("_timestamp",),
                              "__init__": _timestamp_spec_init,
                              "_new": classmethod(_timestamp_spec_new)})
    del _name
