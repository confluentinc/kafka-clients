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

# Stub for confluent_kafka.admin.DescribeDelegationTokenOptions.

from collections.abc import Sequence
from typing import overload

from confluent_kafka.admin.abstract_options import AbstractOptions
from confluent_kafka.common import KafkaPrincipal

__all__ = ["DescribeDelegationTokenOptions"]

class DescribeDelegationTokenOptions(AbstractOptions):
    def __init__(self, *, timeout_ms: int | None = None,
                 owners: Sequence[KafkaPrincipal] | None = None) -> None: ...
    @overload
    def owners(self) -> list[KafkaPrincipal] | None: ...
    @overload
    def owners(self, *, owners: Sequence[KafkaPrincipal] | None) -> DescribeDelegationTokenOptions: ...
