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
/// The result of <see cref="IAsyncAdmin.ExpireDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.ExpireDelegationTokenResult</c>: one future over the
/// token's expiry timestamp.
/// </summary>
/// <seealso cref="ExpireDelegationTokenResult"/>
public sealed class AsyncExpireDelegationTokenResult
{
    private readonly KafkaFutureImpl<long> _expiryTimestamp;
    private readonly SafeAdminResultHandle _native;

    private AsyncExpireDelegationTokenResult(KafkaFutureImpl<long> expiryTimestamp, SafeAdminResultHandle native)
    {
        _expiryTimestamp = expiryTimestamp;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>
    /// The expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>.
    /// </summary>
    /// <returns>A future over the timestamp.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<long> ExpiryTimestamp() => new AsyncKafkaFuture<long>(_expiryTimestamp);

    internal static AsyncExpireDelegationTokenResult OfClient(
        (KafkaFutureImpl<long> ExpiryTimestamp, SafeAdminResultHandle Result) native) =>
        new AsyncExpireDelegationTokenResult(native.ExpiryTimestamp, native.Result);
}
