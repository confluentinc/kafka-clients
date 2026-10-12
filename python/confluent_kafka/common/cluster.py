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

"""``Cluster``: a placeholder for Java's ``org.apache.kafka.common.Cluster``.

A placeholder alias of ``object`` *(deviation)* (CLAUDE.md, Python Binding
Conventions, Types): Java's type is a view of the cluster's nodes, topics and partitions a mock is given; the binding does not translate
it, and code that receives one calls its Java methods by snake_case name.
"""

from __future__ import annotations

__all__ = ["Cluster"]

#: Java's ``org.apache.kafka.common.Cluster``, not translated: any object.
Cluster = object
