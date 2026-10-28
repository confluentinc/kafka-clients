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
/// The result of <see cref="IAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenResult</c>: one future over the
/// token's new expiry timestamp.
/// </summary>
/// <seealso cref="AsyncRenewDelegationTokenResult"/>
public sealed class RenewDelegationTokenResult
{
    private readonly KafkaFutureImpl<long> _expiryTimestamp;
    private readonly SafeAdminResultHandle _native;

    private RenewDelegationTokenResult(KafkaFutureImpl<long> expiryTimestamp, SafeAdminResultHandle native)
    {
        _expiryTimestamp = expiryTimestamp;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The new expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>.
    /// </summary>
    /// <returns>A future over the timestamp.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public KafkaFuture<long> ExpiryTimestamp() => new KafkaFuture<long>(_expiryTimestamp);

    internal static RenewDelegationTokenResult OfClient(
        (KafkaFutureImpl<long> ExpiryTimestamp, SafeAdminResultHandle Result) native) =>
        new RenewDelegationTokenResult(native.ExpiryTimestamp, native.Result);
}
