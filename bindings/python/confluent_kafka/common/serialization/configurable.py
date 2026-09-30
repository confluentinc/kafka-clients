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

"""``Configurable``: a serde's ``configure(Map<String, ?> configs, boolean
isKey)``, as a ``@runtime_checkable`` protocol.

The serde lifecycle is duck-typed *(deviation)*: ``configure(configs,
is_key)`` runs once after construction on the config route only,
``close()`` at client close (its exceptions logged, never raised), and an
absent method is a no-op (CLAUDE.md, Python Binding Conventions,
Serialization).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["Configurable"]


@runtime_checkable
class Configurable(Protocol):
    """A serde configured after construction on the config route.

    Java: the ``configure(Map<String, ?> configs, boolean isKey)`` method of
    ``Serializer`` / ``Deserializer``.
    """

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        """Configure this class: ``configs`` are the configs in key/value
        pairs, ``is_key`` whether it is for the key or the value."""
        ...
