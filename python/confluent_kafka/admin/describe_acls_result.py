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

"""``DescribeAclsResult``: Java's ``org.apache.kafka.clients.admin.DescribeAclsResult``.

Java's constructor is package-private: ``__init__`` raises ``TypeError`` and ``_new``
builds it (CLAUDE.md, Python Binding Conventions, Class family). A ``Collection`` yields
a ``list``: Java's ``KafkaAdminClient`` builds no ``Set`` for it (Types).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync

if TYPE_CHECKING:
    from confluent_kafka.common import AclBinding

__all__ = ["DescribeAclsResult"]


class DescribeAclsResult(Generic[KindT]):
    """The result of the ``Admin.describe_acls()`` call.

    Java: ``org.apache.kafka.clients.admin.DescribeAclsResult``.
    """

    __slots__ = ("_future", "_kind")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError(
            "DescribeAclsResult cannot be constructed directly; use the admin client's "
            "describe_acls()")

    @classmethod
    def _new(cls, *, future: AnyFuture[list[AclBinding]],
             kind: type[Sync] | type[Async]) -> DescribeAclsResult[Any]:
        """Java's package-private ``DescribeAclsResult(future)``."""
        result = cls.__new__(cls)
        result._future = future
        result._kind = kind
        return result

    def values(self) -> AnyFuture[list[AclBinding]]:
        """Return a future containing the ACLs requested."""
        return self._future
