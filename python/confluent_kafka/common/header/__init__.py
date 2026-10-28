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

"""``confluent_kafka.common.header``: Java's ``org.apache.kafka.common.header``.

One file per class; import the classes from this module, never from the files
(CLAUDE.md, Python Binding Conventions, Modules).
"""

from __future__ import annotations

from .header import Header as Header
from .headers import Headers as Headers

__all__ = [
    "Header",
    "Headers",
]
