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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.ForceTerminateTransaction"/> — Java's
/// <c>org.apache.kafka.clients.admin.TerminateTransactionResult</c>: one future over the termination.
/// </summary>
/// <seealso cref="AsyncTerminateTransactionResult"/>
public sealed class TerminateTransactionResult
{
    private readonly KafkaFutureImpl<object?> _future;
    private readonly SafeAdminResultHandle _native;

    private TerminateTransactionResult(KafkaFutureImpl<object?> future, SafeAdminResultHandle native)
    {
        _future = future;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// Whether the transaction was terminated — Java's <c>result()</c>.
    /// </summary>
    /// <returns>A future that completes when the termination has.</returns>
    /// <remarks>Every call returns an equal future over the call's one future, as Java's returns its own.</remarks>
    public KafkaFuture Result() => new KafkaFuture(_future);

    internal static TerminateTransactionResult OfClient((KafkaFutureImpl<object?> Future, SafeAdminResultHandle Result) native) =>
        new TerminateTransactionResult(native.Future, native.Result);
}
