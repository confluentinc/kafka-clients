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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.ExpireDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.ExpireDelegationTokenResult</c>: one future over the
/// token's expiry timestamp (<c>:26</c>).
/// </summary>
/// <remarks>
/// ⚠ See <see cref="AsyncRenewDelegationTokenResult"/> on the byte-identical accessor sets and the
/// wiring guard that keeps the two readers apart.
/// </remarks>
/// <seealso cref="ExpireDelegationTokenResult"/>
public sealed class AsyncExpireDelegationTokenResult
{
    private readonly Task<long> _expiryTimestamp;

    internal AsyncExpireDelegationTokenResult(Task<long> expiryTimestamp)
    {
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncExpireDelegationTokenResult From(SingleAdminOperation<long> operation) =>
        new AsyncExpireDelegationTokenResult(operation.Task);

    /// <summary>
    /// The expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <returns>A future over the timestamp.</returns>
    public AsyncKafkaFuture<long> ExpiryTimestamp() => new AsyncKafkaFuture<long>(_expiryTimestamp);
}
