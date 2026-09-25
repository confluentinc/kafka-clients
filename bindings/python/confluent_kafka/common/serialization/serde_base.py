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

"""``SerdeBase`` *(deviation)*: a no-op base with both lifecycle methods.

The serde lifecycle is duck-typed *(deviation)*: ``configure(configs,
is_key)`` runs once after construction on the config route only,
``close()`` at client close (its exceptions logged, never raised), and an
absent method is a no-op (CLAUDE.md, Python Binding Conventions,
Serialization).
"""

from __future__ import annotations

__all__ = ["SerdeBase"]


class SerdeBase:
    """A no-op base with both lifecycle methods, Java's ``default`` bodies,
    for a serde written as a class."""

    def configure(self, configs: dict[str, object], is_key: bool) -> None:
        """Configure this class; intentionally left blank."""

    def close(self) -> None:
        """Close this serde; intentionally left blank."""
