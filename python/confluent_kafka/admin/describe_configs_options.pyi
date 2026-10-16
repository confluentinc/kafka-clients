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

# Stub for confluent_kafka.admin.DescribeConfigsOptions.

from typing import overload

from confluent_kafka.admin.abstract_options import AbstractOptions

__all__ = ["DescribeConfigsOptions"]

class DescribeConfigsOptions(AbstractOptions):
    def __init__(self, *, timeout_ms: int | None = None, include_synonyms: bool = False,
                 include_documentation: bool = False) -> None: ...
    @overload
    def timeout_ms(self) -> int | None: ...
    @overload
    def timeout_ms(self, *, timeout_ms: int | None) -> DescribeConfigsOptions: ...
    @overload
    def include_synonyms(self) -> bool: ...
    @overload
    def include_synonyms(self, *, include_synonyms: bool) -> DescribeConfigsOptions: ...
    @overload
    def include_documentation(self) -> bool: ...
    @overload
    def include_documentation(self, *, include_documentation: bool) -> DescribeConfigsOptions: ...
