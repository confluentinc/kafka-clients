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

"""``ListConfigResourcesOptions``: Java's
``org.apache.kafka.clients.admin.ListConfigResourcesOptions``.

Java builds it with setters, so the constructor also takes one keyword per setter field,
here only the inherited ``timeout_ms`` (CLAUDE.md, Python Binding Conventions,
Signatures). A keyword left out leaves its setter uncalled.
"""

from __future__ import annotations

from confluent_kafka._args import UNSET
from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["ListConfigResourcesOptions"]


class ListConfigResourcesOptions(AbstractOptions):
    """Options for ``Admin.list_config_resources()``.

    Java: ``org.apache.kafka.clients.admin.ListConfigResourcesOptions``.
    """

    __slots__ = ()

    def __init__(self, *, timeout_ms: int | None = UNSET) -> None:
        super().__init__()
        if timeout_ms is not UNSET:
            self.timeout_ms(timeout_ms=timeout_ms)
