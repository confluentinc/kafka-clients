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
/// Options for <see cref="IAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenOptions</c>.
/// </summary>
public sealed class RenewDelegationTokenOptions
{
    private int? _timeoutMs;
    private long _renewTimePeriodMs = -1L;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs
    {
        get => _timeoutMs;
        set
        {
            _timeoutMs = value;
            Setters.Ran(nameof(TimeoutMs));
        }
    }

    /// <summary>
    /// How much longer the token should live, in milliseconds, or <c>-1</c>, the default, for
    /// the server default — Java's <c>renewTimePeriodMs()</c>.
    /// </summary>
    public long RenewTimePeriodMs
    {
        get => _renewTimePeriodMs;
        set
        {
            _renewTimePeriodMs = value;
            Setters.Ran(nameof(RenewTimePeriodMs));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
