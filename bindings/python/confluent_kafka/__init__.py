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

"""The ``confluent_kafka`` package root.

The Python client of the Java client's API (CLAUDE.md, Python Binding
Conventions). Each Java package is a module here, with ``clients`` dropped
(``org.apache.kafka.clients.consumer`` -> ``confluent_kafka.consumer``), and
every type is imported from its module (``from confluent_kafka.consumer import
KafkaConsumer``). By rule 3 the root exports only ``Duration`` and the Java
built-in exception classes the translated code throws (``IllegalStateError``,
``IllegalArgumentError``, ``ConcurrentModificationError``, ``TimeoutError``,
``NoSuchElementError``, ``NullPointerError``), generated with the rest of the
error hierarchy by ``cargo xtask generate-error-codes``.
"""

from __future__ import annotations

from datetime import timedelta

#: Java's ``java.time.Duration`` as an input: seconds, or a ``timedelta``
#: (CLAUDE.md, Python Binding Conventions, Types). A negative value raises
#: ``IllegalArgumentError`` where Java rejects it.
Duration = float | timedelta

__all__ = ["Duration"]

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .concurrent_modification_error import ConcurrentModificationError as ConcurrentModificationError
from .illegal_argument_error import IllegalArgumentError as IllegalArgumentError
from .illegal_state_error import IllegalStateError as IllegalStateError
from .no_such_element_error import NoSuchElementError as NoSuchElementError
from .null_pointer_error import NullPointerError as NullPointerError
from .timeout_error import TimeoutError as TimeoutError
__all__ += [
    "ConcurrentModificationError",
    "IllegalArgumentError",
    "IllegalStateError",
    "NoSuchElementError",
    "NullPointerError",
    "TimeoutError",
]
# END GENERATED ERRORS
