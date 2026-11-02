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

using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeLogDirsResult</c>: one future per broker id, each
/// yielding that broker's log directories keyed by path.
/// </summary>
/// <remarks>
/// A broker whose query failed fails only its own future. A log directory that is offline fails
/// nothing: the broker's future succeeds and that directory's <see cref="LogDirDescription.Error"/>
/// is non-null.
/// </remarks>
/// <seealso cref="AsyncDescribeLogDirsResult"/>
public sealed class DescribeLogDirsResult
{
    private readonly IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> _descriptions;
    private readonly IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>>? _nativeDescriptions;
    private readonly SafeAdminResultHandle? _native;

    internal DescribeLogDirsResult(
        IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> descriptions)
    {
        _descriptions = descriptions;
    }

    private DescribeLogDirsResult(
        IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> nativeDescriptions,
        SafeAdminResultHandle native)
    {
        Dictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> descriptions =
            new Dictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>>(nativeDescriptions.Count);
        foreach (KeyValuePair<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> entry in nativeDescriptions)
        {
            descriptions.Add(entry.Key, new KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>(entry.Value));
        }

        _descriptions = descriptions;
        _nativeDescriptions = nativeDescriptions;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <summary>
    /// One future per requested broker, each yielding that broker's log directories
    /// keyed by path — Java's <c>descriptions()</c>.
    /// </summary>
    public IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions =>
        _descriptions;

    /// <summary>
    /// Every broker's log directories in one map, succeeding only if every broker answered
    /// without error — Java's <c>allDescriptions()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure to complete.</returns>
    public KafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> AllDescriptions() =>
        _nativeDescriptions is not null
            ? new KafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>(
                KafkaFutureImpl.GatherAll(_nativeDescriptions, EqualityComparer<int>.Default))
            : SyncAdminFutures.GatherAll(_descriptions, EqualityComparer<int>.Default);

    internal static DescribeLogDirsResult OfClient(
        (IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions,
            SafeAdminResultHandle Result) native) =>
        new DescribeLogDirsResult(native.Descriptions, native.Result);
}
