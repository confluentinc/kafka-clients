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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeLogDirs"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeLogDirsResult</c>: one future per broker id, each
/// yielding that broker's log directories keyed by path.
/// </summary>
/// <remarks>
/// A broker whose query failed faults only its own future. A log directory that is offline faults
/// nothing: the broker's future succeeds and that directory's <see cref="LogDirDescription.Error"/>
/// is non-null.
/// </remarks>
/// <seealso cref="DescribeLogDirsResult"/>
public sealed class AsyncDescribeLogDirsResult
{
    private readonly IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> _futures;
    private readonly IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> _nativeDescriptions;
    private readonly SafeAdminResultHandle _native;


    private AsyncDescribeLogDirsResult(
        IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> nativeDescriptions,
        SafeAdminResultHandle native)
    {
        Dictionary<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> futures =
            new Dictionary<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>>(nativeDescriptions.Count);
        foreach (KeyValuePair<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> entry in nativeDescriptions)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>(entry.Value));
        }

        _futures = futures;
        _nativeDescriptions = nativeDescriptions;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// One future per requested broker, each yielding that broker's log directories
    /// keyed by path — Java's <c>descriptions()</c>.
    /// </summary>
    public IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions =>
        _futures;

    /// <summary>
    /// Every broker's log directories in one map, succeeding only if every broker answered
    /// without error — Java's <c>allDescriptions()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure to complete.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> AllDescriptions() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>>(
            KafkaFutureImpl.GatherAll(_nativeDescriptions, EqualityComparer<int>.Default));

    internal static AsyncDescribeLogDirsResult OfClient(
        (IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions,
            SafeAdminResultHandle Result) native) =>
        new AsyncDescribeLogDirsResult(native.Descriptions, native.Result);
}
