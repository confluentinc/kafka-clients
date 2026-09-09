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

"""``confluent_kafka.consumer`` — mirror of
``org.apache.kafka.clients.consumer`` (the ``clients`` segment dropped, spec §4).

Skeleton for this phase (P1). ``Consumer`` / ``KafkaConsumer`` / ``MockConsumer``
and their ``Async`` peers, the record and offset value types, ``CloseOptions``,
``ConsumerRebalanceListener`` and the rest of §6.2 land here in P2/P5. It already
carries the consumer-package exceptions, which Java places in this package
(``CommitFailedException``, ``OffsetOutOfRangeException``, …).
"""

from __future__ import annotations

from ._generated_errors import *  # noqa: F401,F403 -- re-export the consumer errors
from ._generated_errors import __all__ as _errors_all

__all__ = [*_errors_all]
