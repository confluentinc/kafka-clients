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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenOptions</c>.
/// </summary>
public sealed class RenewDelegationTokenOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset —
    /// Java's <c>AbstractOptions.timeoutMs()</c>. Must not be negative.
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// How much longer the token should live, in milliseconds, or <c>-1</c> for the server
    /// default — Java's <c>renewTimePeriodMs()</c> (<c>:31</c>). Java's own sentinel, passed
    /// through verbatim.
    /// </summary>
    public long RenewTimePeriodMs { get; set; } = -1L;
}
