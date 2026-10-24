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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeClassicGroups"/>, Java's <c>DescribeClassicGroupsResult</c>:
/// one future per group id.
/// </summary>
public sealed class AsyncDescribeClassicGroupsResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> _futures;
    private readonly SafeAdminResultHandle? _native;

    /// <inheritdoc cref="DescribeClassicGroupsResult(IReadOnlyDictionary{string, KafkaFuture{ClassicGroupDescription}})"/>
    public AsyncDescribeClassicGroupsResult(IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> futures)
    {
        if (futures is null)
        {
            throw new ArgumentNullException(nameof(futures));
        }

        Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> copy = new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>(futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, AsyncKafkaFuture<ClassicGroupDescription>> entry in futures)
        {
            copy.Add(entry.Key, entry.Value);
        }

        _futures = copy;
    }

    private AsyncDescribeClassicGroupsResult(IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle? Native => _native;

    /// <inheritdoc cref="DescribeClassicGroupsResult.DescribedGroups"/>
    public IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> DescribedGroups => _futures;

    /// <inheritdoc cref="DescribeClassicGroupsResult.All"/>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, ClassicGroupDescription>> All()
    {
        IReadOnlyDictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> futures = _futures;
        AsyncKafkaFuture<ClassicGroupDescription>[] values = new AsyncKafkaFuture<ClassicGroupDescription>[futures.Count];
        int next = 0;
        foreach (AsyncKafkaFuture<ClassicGroupDescription> future in futures.Values)
        {
            values[next++] = future;
        }

        return AsyncKafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<string, ClassicGroupDescription>>(() =>
        {
            Dictionary<string, ClassicGroupDescription> descriptions = new Dictionary<string, ClassicGroupDescription>(futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, AsyncKafkaFuture<ClassicGroupDescription>> entry in futures)
            {
                descriptions.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return descriptions;
        });
    }

    internal static AsyncDescribeClassicGroupsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<ClassicGroupDescription>> DescribedGroups, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>> futures =
            new Dictionary<string, AsyncKafkaFuture<ClassicGroupDescription>>(native.DescribedGroups.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<ClassicGroupDescription>> entry in native.DescribedGroups)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<ClassicGroupDescription>(entry.Value));
        }

        return new AsyncDescribeClassicGroupsResult(futures, native.Result);
    }
}
