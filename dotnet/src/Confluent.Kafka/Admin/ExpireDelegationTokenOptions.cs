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
/// Options for <see cref="IAdmin.ExpireDelegationToken(byte[], ExpireDelegationTokenOptions)"/> — Java's
/// <c>org.apache.kafka.clients.admin.ExpireDelegationTokenOptions</c>.
/// </summary>
public sealed class ExpireDelegationTokenOptions
{
    private int? _timeoutMs;
    private long _expiryTimePeriodMs = -1L;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public ExpireDelegationTokenOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// How long until the token expires, in milliseconds — Java's
    /// <c>expiryTimePeriodMs()</c>. Defaults to <c>-1</c>, as Java's does; a negative value
    /// expires it immediately.
    /// </summary>
    /// <returns>How long until the token expires, in milliseconds — Java's <c>expiryTimePeriodMs()</c>. Defaults to
    /// <c>-1</c>, as Java's does; a negative value expires it immediately.</returns>
    public long ExpiryTimePeriodMs() => _expiryTimePeriodMs;

    /// <summary>Sets the time period until the token expires.</summary>
    /// <param name="expiryTimePeriodMs">The time period until we should expire this token. At or above 0, the
    /// token's expiration timestamp becomes min(now + <paramref name="expiryTimePeriodMs"/>, maxTimestamp); below 0,
    /// the token is expired immediately.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public ExpireDelegationTokenOptions ExpiryTimePeriodMs(long expiryTimePeriodMs)
    {
        _expiryTimePeriodMs = expiryTimePeriodMs;
        Setters.Ran(nameof(ExpiryTimePeriodMs));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
