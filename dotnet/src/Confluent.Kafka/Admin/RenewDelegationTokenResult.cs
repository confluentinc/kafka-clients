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
/// The result of <see cref="IAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenResult</c>: one future over the
/// token's new expiry timestamp (<c>:26</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ This type and <see cref="ExpireDelegationTokenResult"/> sit on <b>byte-identical</b> ABI
/// accessor sets, so a cross-wired reader would return a plausible answer for either. Their
/// readers are pinned apart by a wiring guard in the unit tests.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the renewal has completed, and
/// needs no thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncRenewDelegationTokenResult"/>
public sealed class RenewDelegationTokenResult
{
    private readonly KafkaFuture<long> _expiryTimestamp;

    internal RenewDelegationTokenResult(KafkaFuture<long> expiryTimestamp)
    {
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static RenewDelegationTokenResult From(SingleAdminOperation<long> operation) =>
        new RenewDelegationTokenResult(new KafkaFuture<long>(operation.SyncSource));

    /// <summary>
    /// The new expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <returns>A future over the timestamp.</returns>
    /// <remarks>
    /// This is a future over the operation's own latch, so every call returns an <b>equal</b> future — no second
    /// completion.
    /// </remarks>
    public KafkaFuture<long> ExpiryTimestamp() => _expiryTimestamp;
}
