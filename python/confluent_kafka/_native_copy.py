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

"""The refusal of ``copy``, ``deepcopy`` and ``pickle`` for an object holding a
native core object (private).

Java's clients, futures, admin results and consumer records are neither
``Cloneable`` nor ``Serializable``. An object that holds a native core object
(a client's handle, a C future, an admin client's C result, a fetched batch)
refuses all three with one ``TypeError``, in ``ConsumerGroupMetadata``'s shape:
a copy would share the native object, which the original's ``close()`` frees
under it (CLAUDE.md, Python Binding Conventions, Types, the
``java.io.Serializable`` row). The mocks and the objects built in Python keep
Python's default.
"""

from __future__ import annotations

__all__ = ["CLIENT_HANDLE", "not_copyable"]

#: The reason a client gives.
CLIENT_HANDLE = "it holds the client's native handle"


def not_copyable(name: str, reason: str) -> TypeError:
    """The ``TypeError`` of a refused ``copy``, ``deepcopy`` or ``pickle``:
    ``"<name> cannot be copied or pickled; <reason>"``."""
    return TypeError(f"{name} cannot be copied or pickled; {reason}")
