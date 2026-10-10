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
using System.Linq;
using Confluent.Kafka.Internal;

using Token = Confluent.Kafka.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeDelegationTokenResult</c>: one future over the
/// whole token list (<c>:29</c>). Java declares no <c>all()</c> here.
/// </summary>
/// <remarks>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the description has completed, and
/// needs no thread-pool thread: the future here is composed on the waiting thread, never over a <c>Task</c>.
/// </remarks>
/// <seealso cref="AsyncDescribeDelegationTokenResult"/>
public sealed class DescribeDelegationTokenResult
{
    private readonly KafkaFuture<IReadOnlyList<Token>> _tokens;

    internal DescribeDelegationTokenResult(KafkaFuture<IReadOnlyCollection<Token>> tokens)
    {
        // Java's accessor is List-typed (:38) while the collection walker produces
        // IReadOnlyCollection, so the one list it built is re-presented rather than rebuilt. The
        // projection is built once and memoized, so every call returns the same future and every
        // Get() the same list.
        _tokens = SyncAdminFutures.Project(
            tokens, described => described as IReadOnlyList<Token> ?? described.ToList());
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeDelegationTokenResult From(
        SingleAdminOperation<IReadOnlyCollection<DelegationToken>> operation) =>
        new DescribeDelegationTokenResult(new KafkaFuture<IReadOnlyCollection<Token>>(operation.SyncSource));

    /// <summary>The described tokens — Java's <c>delegationTokens()</c> (<c>:38</c>).</summary>
    /// <returns>A future over the token list, in the order the ABI delivered it.</returns>
    public KafkaFuture<IReadOnlyList<Token>> DelegationTokens() => _tokens;
}
