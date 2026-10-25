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
/// The result of <see cref="IAsyncAdmin.UnregisterController"/> — Java's
/// <c>org.apache.kafka.clients.admin.UnregisterControllerResult</c>: one future over the whole call.
/// </summary>
/// <seealso cref="UnregisterControllerResult"/>
public sealed class AsyncUnregisterControllerResult
{
    private readonly KafkaFutureImpl<object?> _future;
    private readonly SafeAdminResultHandle _native;

    private AsyncUnregisterControllerResult(KafkaFutureImpl<object?> future, SafeAdminResultHandle native)
    {
        _future = future;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// A future that succeeds if the controller was unregistered — Java's <c>all()</c>.
    /// </summary>
    /// <returns>A future over the call.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(_future);

    internal static AsyncUnregisterControllerResult OfClient(
        (KafkaFutureImpl<object?> All, SafeAdminResultHandle Result) native) =>
        new AsyncUnregisterControllerResult(native.All, native.Result);
}
