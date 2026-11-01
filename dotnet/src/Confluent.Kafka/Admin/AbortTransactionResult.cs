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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AbortTransaction"/> — Java's
/// <c>org.apache.kafka.clients.admin.AbortTransactionResult</c>.
/// </summary>
/// <remarks>
/// Java publishes only <see cref="All"/>, so there is no per-partition view.
/// </remarks>
/// <seealso cref="AsyncAbortTransactionResult"/>
public sealed class AbortTransactionResult
{
    private readonly KafkaFutureImpl<object?> _future;
    private readonly SafeAdminResultHandle _native;

    private AbortTransactionResult(KafkaFutureImpl<object?> future, SafeAdminResultHandle native)
    {
        _future = future;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Whether the transaction was aborted — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future that completes when the abort has.</returns>
    /// <remarks>
    /// Each call builds a new future over the call's one future, as Java's <c>allOf</c> over its map does.
    /// </remarks>
    public KafkaFuture All() => KafkaFuture.AllOf(new KafkaFuture(_future));

    internal static AbortTransactionResult OfClient((KafkaFutureImpl<object?> All, SafeAdminResultHandle Result) native) =>
        new AbortTransactionResult(native.All, native.Result);
}
