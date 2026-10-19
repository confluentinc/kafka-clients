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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AbortTransaction"/> — Java's
/// <c>org.apache.kafka.clients.admin.AbortTransactionResult</c> (<c>:27</c>).
/// </summary>
/// <remarks>
/// <para>
/// Java retains a single-entry <c>Map&lt;TopicPartition, KafkaFuture&lt;Void&gt;&gt;</c>
/// (<c>:28</c>) but publishes only <see cref="All"/> (<c>:41</c>), so there is no per-partition
/// view to restore and the ABI exposes no result handle.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the abort has completed, and needs no
/// thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAbortTransactionResult"/>
public sealed class AbortTransactionResult
{
    private readonly KafkaFuture _all;

    internal AbortTransactionResult(KafkaFuture all)
    {
        _all = all;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// The <c>bool</c> is the void bridge's internal success token: the future waits on the same latch and does not
    /// carry it.
    /// </remarks>
    internal static AbortTransactionResult From(SingleAdminOperation<bool> operation) =>
        new AbortTransactionResult(new KafkaFuture(operation.SyncSource));

    /// <summary>
    /// Whether the transaction was aborted — Java's <c>all()</c> (<c>:41</c>).
    /// </summary>
    /// <returns>A future that completes when the abort has.</returns>
    /// <remarks>
    /// This is a future over the operation's own latch, so every call returns an <b>equal</b> future — no second
    /// completion.
    /// </remarks>
    public KafkaFuture All() => _all;
}
