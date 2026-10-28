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

"""The preview warning a client logs when it is constructed.

No Java counterpart *(deviation: Java logs no status warning; CLAUDE.md, Python
Binding Conventions, Behaviour with no other home)*: ``KafkaProducer``,
``KafkaConsumer``, ``KafkaAdminClient`` and their async peers log it once each,
on their own ``confluent_kafka.*`` logger, while the client is in preview. The
mocks log nothing.
"""

from __future__ import annotations

import logging

#: The text every client logs at WARNING when it is constructed.
PREVIEW_WARNING = (
    "The Python client is in preview and not yet recommended for production use. The public "
    "API is not stable before the GA release and may change. We welcome your feedback while "
    "the design is still open"
)


def log_preview_warning(logger: logging.Logger) -> None:
    """Log :data:`PREVIEW_WARNING` at WARNING on the client's ``logger``."""
    logger.warning("%s", PREVIEW_WARNING)
