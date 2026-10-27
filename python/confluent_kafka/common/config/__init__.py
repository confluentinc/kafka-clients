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

It holds ``ConfigResource``, ``ConfigError`` (Java's ``ConfigException``), the
key-name and config-value classes ``TopicConfig``, ``SaslConfigs``,
``SslConfigs``, ``SecurityConfig``, ``LogLevelConfig`` and ``SslClientAuth``
(CLAUDE.md, Python Binding Conventions, Scope).
"""

from __future__ import annotations

from .config_resource import ConfigResource as ConfigResource
from .log_level_config import LogLevelConfig as LogLevelConfig
from .sasl_configs import SaslConfigs as SaslConfigs
from .security_config import SecurityConfig as SecurityConfig
from .ssl_client_auth import SslClientAuth as SslClientAuth
from .ssl_configs import SslConfigs as SslConfigs
from .topic_config import TopicConfig as TopicConfig

__all__: list[str] = [
    "ConfigResource",
    "LogLevelConfig",
    "SaslConfigs",
    "SecurityConfig",
    "SslClientAuth",
    "SslConfigs",
    "TopicConfig",
]

# BEGIN GENERATED ERRORS (cargo xtask generate-error-codes; do not edit)
from .config_error import ConfigError as ConfigError
__all__ += [
    "ConfigError",
]
# END GENERATED ERRORS
