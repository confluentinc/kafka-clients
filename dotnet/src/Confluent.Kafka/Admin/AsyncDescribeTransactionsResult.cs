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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of a <c>describeTransactions</c> call — Java's
/// <c>org.apache.kafka.clients.admin.DescribeTransactionsResult</c> (<c>:27</c>): one
/// future per transactional id.
/// </summary>
/// <remarks>
/// ⚠ Java publishes <b>no</b> per-id map, only the <see cref="Description"/> lookup and
/// <see cref="All"/> (<c>:43</c>, <c>:62</c>), so neither is added here.
/// </remarks>
/// <seealso cref="DescribeTransactionsResult"/>
public sealed class AsyncDescribeTransactionsResult
{
    private readonly IReadOnlyDictionary<string, Task<TransactionDescription>> _futures;

    /// <summary>Wraps one future per transactional id.</summary>
    /// <param name="futures">One future per requested id.</param>
    /// <exception cref="ArgumentNullException"><paramref name="futures"/> is null.</exception>
    internal AsyncDescribeTransactionsResult(IReadOnlyDictionary<string, Task<TransactionDescription>> futures)
    {
        _futures = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncDescribeTransactionsResult From(KeyedAdminOperation<string, TransactionDescription> operation) =>
        new AsyncDescribeTransactionsResult(operation.Tasks);

    /// <summary>One id's description — Java's <c>description(String)</c> (<c>:43</c>).</summary>
    /// <param name="transactionalId">A transactional id from the request.</param>
    /// <returns>That id's future.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transactionalId"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// The id was not in the request — Java's <c>IllegalArgumentException</c> (<c>:47</c>),
    /// thrown <b>synchronously</b> because it is a usage error, not a future's outcome.
    /// </exception>
    public AsyncKafkaFuture<TransactionDescription> Description(string transactionalId)
    {
        if (transactionalId is null)
        {
            throw new ArgumentNullException(nameof(transactionalId));
        }

        return _futures.TryGetValue(transactionalId, out Task<TransactionDescription>? future)
            ? new AsyncKafkaFuture<TransactionDescription>(future)
            : throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "TransactionalId `{0}` was not included in the request",
                    transactionalId),
                nameof(transactionalId));
    }

    /// <summary>
    /// Every id's description in one map, succeeding only if every id did — Java's
    /// <c>all()</c> (<c>:62</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or faulting with the first failure.</returns>
    /// <remarks>
    /// ⚠ <b>Recorded divergence.</b> Java's two sibling describe results wrap the aggregate's
    /// own gather failure differently — this one in a plain <c>RuntimeException</c>
    /// (<c>:71</c>), <see cref="AsyncDescribeProducersResult.All"/> in a <c>KafkaException</c>
    /// (<c>DescribeProducersResult.java:54</c>). Neither is reproduced: both Java paths are
    /// reachable only through the checked <c>InterruptedException</c>/<c>ExecutionException</c>
    /// that <c>allOf</c> has already ruled out, and C# has no checked exception to re-wrap. The
    /// per-id failure itself propagates unchanged, which is what a caller observes in Java too.
    /// </remarks>
    public AsyncKafkaFuture<IReadOnlyDictionary<string, TransactionDescription>> All() =>
        new AsyncKafkaFuture<IReadOnlyDictionary<string, TransactionDescription>>(Gather(_futures));

    private static async Task<IReadOnlyDictionary<string, TransactionDescription>> Gather(
        IReadOnlyDictionary<string, Task<TransactionDescription>> futures)
    {
        // WhenAll first, so the aggregate faults with the first failure exactly as Java's
        // allOf(...).thenApply(...) does rather than with whichever key enumerates first.
        await Task.WhenAll(futures.Values).ConfigureAwait(false);

        Dictionary<string, TransactionDescription> descriptions =
            new Dictionary<string, TransactionDescription>(futures.Count, StringComparer.Ordinal);
        foreach (KeyValuePair<string, Task<TransactionDescription>> entry in futures)
        {
            descriptions.Add(entry.Key, entry.Value.Result);
        }

        return descriptions;
    }
}
