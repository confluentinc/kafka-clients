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

# GENERATED, DO NOT EDIT (stubs for the generated error hierarchy).

from typing import ClassVar
from confluent_kafka.common.errors._base import KafkaError
from confluent_kafka.common.errors._generated import RetriableError

__all__: list[str]

class CommitFailedError(KafkaError):
    _ffi_id: ClassVar[int]

class InvalidOffsetError(KafkaError):
    def __init__(self, *args: object) -> None: ...

class NoOffsetForPartitionError(InvalidOffsetError):
    _ffi_id: ClassVar[int]

class OffsetOutOfRangeError(InvalidOffsetError):
    _ffi_id: ClassVar[int]

class RetriableCommitFailedError(RetriableError):
    _ffi_id: ClassVar[int]

class LogTruncationError(OffsetOutOfRangeError):
    _ffi_id: ClassVar[int]

