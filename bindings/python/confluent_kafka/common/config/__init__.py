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

"""``confluent_kafka.common.config``: Java's ``org.apache.kafka.common.config``.

It holds ``ConfigError`` (Java's ``ConfigException``). The config classes
themselves are not translated: their keys are the keys of ``configs``
(CLAUDE.md, Python Binding Conventions, Scope).
"""

from __future__ import annotations

__all__: list[str] = []

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .config_error import ConfigError as ConfigError
__all__ += [
    "ConfigError",
]
# END GENERATED ERRORS
