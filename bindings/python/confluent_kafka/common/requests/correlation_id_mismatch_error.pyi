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

# GENERATED, DO NOT EDIT (stub for confluent_kafka.common.requests.CorrelationIdMismatchError).

from typing import ClassVar

from confluent_kafka.illegal_state_error import IllegalStateError

__all__ = ["CorrelationIdMismatchError"]

class CorrelationIdMismatchError(IllegalStateError):
    _ffi_id: ClassVar[int]
    def __init__(self, *, message: str, request_correlation_id: int, response_correlation_id: int) -> None: ...
    def request_correlation_id(self) -> int: ...
    def response_correlation_id(self) -> int: ...
