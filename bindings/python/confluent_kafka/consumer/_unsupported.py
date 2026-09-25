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

"""Core-capability-gap helper (rule 10).

Where the Rust core does not implement a feature (KIP-714 client telemetry:
``clientInstanceId``, metric registration, and the mock telemetry helpers), the
Python method exists with the spec's signature and raises the mapped Java error
the core would raise — an explicit ``UnsupportedVersionError``, never a silent
no-op or a hang (CLAUDE.md §5). Each gap is logged in the clarifications file.
"""

from __future__ import annotations

from typing import NoReturn

from confluent_kafka.common.errors._generated import UnsupportedVersionError

__all__ = ["raise_unsupported"]


def raise_unsupported(method: str) -> NoReturn:
    """Raise ``UnsupportedVersionError`` for a method the Rust core does not yet
    implement (KIP-714 client telemetry / metric registration)."""
    raise UnsupportedVersionError(
        f"{method} is not supported by this client: the underlying Rust core "
        "does not implement KIP-714 client telemetry / metric registration yet"
    )
