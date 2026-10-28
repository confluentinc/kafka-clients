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

"""``AsyncKafkaProducer``: the asyncio peer of
:class:`~confluent_kafka.producer.KafkaProducer`.

By rule 3, ``KafkaProducer``'s constructor over :class:`AsyncProducer`'s
methods (CLAUDE.md, Python Binding Conventions, Class family).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, TypeVar, overload

from confluent_kafka._args import MISSING
from confluent_kafka._native_copy import CLIENT_HANDLE, not_copyable

from .async_producer import AsyncProducer

if TYPE_CHECKING:
    from confluent_kafka.common.serialization import Serializer

__all__ = ["AsyncKafkaProducer"]

K = TypeVar("K")
V = TypeVar("V")


class AsyncKafkaProducer(AsyncProducer[K, V]):
    """The asyncio peer of ``KafkaProducer``, a Kafka client that publishes
    records to the Kafka cluster: see ``KafkaProducer``."""

    NETWORK_THREAD_PREFIX: ClassVar[str] = "kafka-producer-network-thread"
    PRODUCER_METRIC_GROUP_NAME: ClassVar[str] = "producer-metrics"

    @overload
    def __init__(self: AsyncKafkaProducer[bytes, bytes], *, configs: dict[str, Any]) -> None: ...
    @overload
    def __init__(self: AsyncKafkaProducer[K, bytes], *, configs: dict[str, Any],
                 key_serializer: Serializer[K]) -> None: ...
    @overload
    def __init__(self: AsyncKafkaProducer[bytes, V], *, configs: dict[str, Any],
                 value_serializer: Serializer[V]) -> None: ...
    @overload
    def __init__(self, *, configs: dict[str, Any], key_serializer: Serializer[K],
                 value_serializer: Serializer[V]) -> None: ...

    def __init__(self, *, configs: dict[str, Any], key_serializer: Serializer[Any] | None = MISSING,
                 value_serializer: Serializer[Any] | None = MISSING) -> None:
        """See ``KafkaProducer``. The constructor does not wait, so it may run
        outside an event loop; every other waiting method is awaited on one."""
        AsyncProducer.__init__(self)
        # Left out or None (Java's null, which KafkaProducer reads as "use the
        # config key"): the config route (Signatures, the serde exception).
        self._start(configs, None if key_serializer is MISSING else key_serializer,
                    None if value_serializer is MISSING else value_serializer)

    # Java's class is neither ``Cloneable`` nor ``Serializable``, and the client
    # holds its native handle, which a copy would share and the original's
    # ``close()`` free under it: ``copy``, ``deepcopy`` and ``pickle`` are refused
    # (CLAUDE.md, Python Binding Conventions, Types, the ``java.io.Serializable``
    # row). The mocks hold no native object and keep Python's default.
    def __copy__(self) -> NoReturn:
        raise not_copyable("AsyncKafkaProducer", CLIENT_HANDLE)

    def __deepcopy__(self, memo: dict[int, Any]) -> NoReturn:
        raise not_copyable("AsyncKafkaProducer", CLIENT_HANDLE)

    def __reduce__(self) -> NoReturn:
        raise not_copyable("AsyncKafkaProducer", CLIENT_HANDLE)
