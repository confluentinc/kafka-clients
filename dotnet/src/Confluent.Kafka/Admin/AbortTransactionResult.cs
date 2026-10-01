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

using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AbortTransaction"/> — Java's
/// <c>org.apache.kafka.clients.admin.AbortTransactionResult</c> (<c>:27</c>).
/// </summary>
/// <remarks>
/// Java retains a single-entry <c>Map&lt;TopicPartition, KafkaFuture&lt;Void&gt;&gt;</c>
/// (<c>:28</c>) but publishes only <see cref="All"/> (<c>:41</c>), so there is no per-partition
/// view to restore and the ABI exposes no result handle.
/// </remarks>
public sealed class AbortTransactionResult
{
    private readonly Task _all;

    internal AbortTransactionResult(Task all)
    {
        _all = all;
    }

    /// <summary>
    /// Whether the transaction was aborted — Java's <c>all()</c> (<c>:41</c>).
    /// </summary>
    /// <returns>An awaitable that completes when the abort has.</returns>
    public Task All() => _all;
}
