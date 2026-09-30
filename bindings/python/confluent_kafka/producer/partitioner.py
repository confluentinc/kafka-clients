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

"""``Partitioner``: a placeholder for Java's ``org.apache.kafka.clients.producer.Partitioner``.

A placeholder alias of ``object`` *(deviation)* (CLAUDE.md, Python Binding
Conventions, Types): Java's interface computes the partition of a record; the
binding does not translate it. ``MockProducer`` uses the one it is given as
Java's mock does, calling ``partition(topic, key, key_bytes, value,
value_bytes, cluster)`` positionally (Implementation over the FFI).
"""

from __future__ import annotations

__all__ = ["Partitioner"]

#: Java's ``org.apache.kafka.clients.producer.Partitioner``, not translated: any object.
Partitioner = object
