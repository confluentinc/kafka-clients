// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
using System;
using System.Collections.Generic;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ListGroups(ListGroupsOptions)"/>, Java's <c>ListGroupsResult</c>: three views
/// of one listing. <see cref="All"/> fails if any broker answered with an error, <see cref="Valid"/>
/// yields the listings regardless, and <see cref="Errors"/> yields the errors.
/// </summary>
public sealed class AsyncListGroupsResult
{
    private readonly IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> _all;
    private readonly IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> _valid;
    private readonly IKafkaFutureImpl<IReadOnlyCollection<Exception>> _errors;
    private readonly SafeAdminResultHandle _native;

    private AsyncListGroupsResult(
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> all,
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid,
        IKafkaFutureImpl<IReadOnlyCollection<Exception>> errors,
        SafeAdminResultHandle native)
    {
        _all = all;
        _valid = valid;
        _errors = errors;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    internal static AsyncListGroupsResult OfClient(
        (IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> All,
            IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> Valid,
            IKafkaFutureImpl<IReadOnlyCollection<Exception>> Errors,
            SafeAdminResultHandle Result) native) =>
        new AsyncListGroupsResult(native.All, native.Valid, native.Errors, native.Result);

    /// <inheritdoc cref="ListGroupsResult.All"/>
    public AsyncKafkaFuture<IReadOnlyCollection<GroupListing>> All() =>
        AsyncAdminFutures.Of(_all);

    /// <inheritdoc cref="ListGroupsResult.Valid"/>
    public AsyncKafkaFuture<IReadOnlyCollection<GroupListing>> Valid() =>
        AsyncAdminFutures.Of(_valid);

    /// <inheritdoc cref="ListGroupsResult.Errors"/>
    public AsyncKafkaFuture<IReadOnlyCollection<Exception>> Errors() =>
        AsyncAdminFutures.Of(_errors);
}
