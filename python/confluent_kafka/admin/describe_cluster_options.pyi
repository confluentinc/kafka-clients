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

# Stub for confluent_kafka.admin.DescribeClusterOptions.

from typing import overload

from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeClusterOptions"]

class DescribeClusterOptions(AbstractOptions):
    def __init__(self, *, timeout_ms: int | None = None,
                 include_authorized_operations: bool = False,
                 include_fenced_brokers: bool = False) -> None: ...
    @overload
    def timeout_ms(self) -> int | None: ...
    @overload
    def timeout_ms(self, *, timeout_ms: int | None) -> DescribeClusterOptions: ...
    @overload
    def include_authorized_operations(self) -> bool: ...
    @overload
    def include_authorized_operations(
            self, *, include_authorized_operations: bool) -> DescribeClusterOptions: ...
    @overload
    def include_fenced_brokers(self) -> bool: ...
    @overload
    def include_fenced_brokers(self, *, include_fenced_brokers: bool) -> DescribeClusterOptions: ...
