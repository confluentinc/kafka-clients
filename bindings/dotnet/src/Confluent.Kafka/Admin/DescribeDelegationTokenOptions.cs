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

using System.Collections.Generic;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeDelegationTokenOptions</c>.
/// </summary>
public sealed class DescribeDelegationTokenOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset —
    /// Java's <c>AbstractOptions.timeoutMs()</c>. Must not be negative.
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The owners to filter by — Java's <c>owners()</c> (<c>:41</c>).
    /// <para>
    /// ⚠⚠ <b><see langword="null"/> and an empty list are DIFFERENT requests.</b>
    /// <see langword="null"/> is "describe every token I am allowed to see"; an empty list
    /// filters by nothing and matches none. Java's field has no initializer (<c>:28</c>), so
    /// it really is null when unset — unlike
    /// <see cref="CreateDelegationTokenOptions.Renewers"/>, which defaults to empty. The ABI
    /// carries the distinction in <c>has_owners_filter</c>
    /// (<c>confluent_kafka.h:9177-9181</c>), never in the count.
    /// </para>
    /// </summary>
    public IReadOnlyList<KafkaPrincipal>? Owners { get; set; }
}
