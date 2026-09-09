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

"""``confluent_kafka.common`` — mirror of ``org.apache.kafka.common``.

Common value types (``TopicPartition``, ``Node``, ``Headers``, …) land here in a
later phase (P2). This phase establishes the sub-packages ``common.errors``,
``common.config`` and ``common.serialization``.
"""

from __future__ import annotations

__all__: list[str] = []
