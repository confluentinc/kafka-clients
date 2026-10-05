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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.common.errors.InvalidTopicError).

from collections.abc import Iterable
from typing import ClassVar, overload

from confluent_kafka.common.errors.invalid_configuration_error import InvalidConfigurationError

__all__ = ["InvalidTopicError"]

class InvalidTopicError(InvalidConfigurationError):
    _ffi_id: ClassVar[int]
    @overload
    def __init__(self, *, message: str | None = ..., cause: BaseException | None = None) -> None: ...
    @overload
    def __init__(self, *, invalid_topics: Iterable[str]) -> None: ...
    @overload
    def __init__(self, *, message: str | None, invalid_topics: Iterable[str]) -> None: ...
    def invalid_topics(self) -> set[str]: ...
