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
using System.Globalization;

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeTransactions</c> call — Java's
/// <c>org.apache.kafka.clients.admin.DescribeTransactionsResult</c>: one
/// future per transactional id.
/// </summary>
/// <remarks>
/// Java publishes no per-id map, only the <see cref="Description"/> lookup and <see cref="All"/>.
/// </remarks>
/// <seealso cref="DescribeTransactionsResult"/>
public sealed class AsyncDescribeTransactionsResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture<TransactionDescription>> _futures;
    private readonly SafeAdminResultHandle _native;

    private AsyncDescribeTransactionsResult(
        IReadOnlyDictionary<string, AsyncKafkaFuture<TransactionDescription>> futures, SafeAdminResultHandle native)
    {
        _futures = futures;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>One id's description — Java's <c>description(String)</c>.</summary>
    /// <param name="transactionalId">A transactional id from the request.</param>
    /// <returns>That id's future.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// The id was not in the request (Java's <c>IllegalArgumentException</c>), thrown at the call.
    /// </exception>
    public AsyncKafkaFuture<TransactionDescription> Description(string transactionalId)
    {
        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        return _futures.TryGetValue(transactionalId, out AsyncKafkaFuture<TransactionDescription> future)
            ? future
            : throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "TransactionalId `{0}` was not included in the request",
                    transactionalId),
                nameof(transactionalId));
    }

    /// <summary>
    /// Every id's description in one map, succeeding only if every id did — Java's
    /// <c>all()</c>.
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, TransactionDescription>> All()
    {
        IReadOnlyDictionary<string, AsyncKafkaFuture<TransactionDescription>> futures = _futures;
        AsyncKafkaFuture<TransactionDescription>[] values = new AsyncKafkaFuture<TransactionDescription>[futures.Count];
        int next = 0;
        foreach (AsyncKafkaFuture<TransactionDescription> future in futures.Values)
        {
            values[next++] = future;
        }

        return AsyncKafkaFuture.AllOf(values).ThenApply<IReadOnlyDictionary<string, TransactionDescription>>(() =>
        {
            Dictionary<string, TransactionDescription> descriptions =
                new Dictionary<string, TransactionDescription>(futures.Count, StringComparer.Ordinal);
            foreach (KeyValuePair<string, AsyncKafkaFuture<TransactionDescription>> entry in futures)
            {
                descriptions.Add(entry.Key, entry.Value.GetNow(null!));
            }

            return descriptions;
        });
    }

    internal static AsyncDescribeTransactionsResult OfClient(
        (IReadOnlyDictionary<string, KafkaFutureImpl<TransactionDescription>> Descriptions, SafeAdminResultHandle Result) native)
    {
        Dictionary<string, AsyncKafkaFuture<TransactionDescription>> futures =
            new Dictionary<string, AsyncKafkaFuture<TransactionDescription>>(native.Descriptions.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, KafkaFutureImpl<TransactionDescription>> entry in native.Descriptions)
        {
            futures.Add(entry.Key, new AsyncKafkaFuture<TransactionDescription>(entry.Value));
        }

        return new AsyncDescribeTransactionsResult(futures, native.Result);
    }
}
