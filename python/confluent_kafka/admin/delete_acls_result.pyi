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

# Stub for confluent_kafka.admin.DeleteAclsResult.

import asyncio
import concurrent.futures
from collections.abc import Iterable, Mapping
from typing import Generic, overload

from confluent_kafka.admin._futures import AnyFuture, Async, KindT, Sync
from confluent_kafka.common import AclBinding, AclBindingFilter
from confluent_kafka.common.errors import ApiError

__all__ = ["DeleteAclsResult"]

class DeleteAclsResult(Generic[KindT]):
    class FilterResult:
        @classmethod
        def _new(cls, *, binding: AclBinding | None,
                 exception: ApiError | None) -> DeleteAclsResult.FilterResult: ...
        def binding(self) -> AclBinding | None: ...
        def exception(self) -> ApiError | None: ...

    class FilterResults:
        @classmethod
        def _new(cls, *, values: Iterable[DeleteAclsResult.FilterResult]
                 ) -> DeleteAclsResult.FilterResults: ...
        def values(self) -> list[DeleteAclsResult.FilterResult]: ...

    @classmethod
    def _new(cls, *,
             futures: Mapping[AclBindingFilter, AnyFuture[DeleteAclsResult.FilterResults]],
             kind: type[KindT]) -> DeleteAclsResult[KindT]: ...
    @overload
    def values(self: DeleteAclsResult[Sync]
               ) -> dict[AclBindingFilter,
                         concurrent.futures.Future[DeleteAclsResult.FilterResults]]: ...
    @overload
    def values(self: DeleteAclsResult[Async]
               ) -> dict[AclBindingFilter, asyncio.Future[DeleteAclsResult.FilterResults]]: ...
    @overload
    def all(self: DeleteAclsResult[Sync]) -> concurrent.futures.Future[list[AclBinding]]: ...
    @overload
    def all(self: DeleteAclsResult[Async]) -> asyncio.Future[list[AclBinding]]: ...
