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
/// The result of <see cref="IAsyncAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenResult</c>: one future over the
/// token's new expiry timestamp (<c>:26</c>).
/// </summary>
/// <remarks>
/// ⚠ This type and <see cref="AsyncExpireDelegationTokenResult"/> sit on <b>byte-identical</b> ABI
/// accessor sets, so a cross-wired reader would return a plausible answer for either. Their
/// readers are pinned apart by a wiring guard in the unit tests.
/// </remarks>
/// <seealso cref="RenewDelegationTokenResult"/>
public sealed class AsyncRenewDelegationTokenResult
{
    private readonly Task<long> _expiryTimestamp;

    internal AsyncRenewDelegationTokenResult(Task<long> expiryTimestamp)
    {
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncRenewDelegationTokenResult From(SingleAdminOperation<long> operation) =>
        new AsyncRenewDelegationTokenResult(operation.Task);

    /// <summary>
    /// The new expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <returns>A future over the timestamp.</returns>
    public AsyncKafkaFuture<long> ExpiryTimestamp() => new AsyncKafkaFuture<long>(_expiryTimestamp);
}
