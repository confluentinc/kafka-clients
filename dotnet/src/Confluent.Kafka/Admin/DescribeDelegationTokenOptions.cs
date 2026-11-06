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

using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeDelegationTokenOptions</c>.
/// </summary>
public sealed class DescribeDelegationTokenOptions
{
    private int? _timeoutMs;
    private IReadOnlyList<KafkaPrincipal>? _owners;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public DescribeDelegationTokenOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// The owners to filter by — Java's <c>owners()</c>.
    /// </summary>
    /// <returns>The owners to filter by.</returns>
    /// <remarks>
    /// <see langword="null"/>, the default as in Java, describes every token the caller may
    /// see; an empty list matches none.
    /// </remarks>
    public IReadOnlyList<KafkaPrincipal>? Owners() => _owners;

    /// <summary>Sets <see cref="Owners()"/>.</summary>
    /// <param name="owners">The owners to filter by.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public DescribeDelegationTokenOptions Owners(IReadOnlyList<KafkaPrincipal>? owners)
    {
        _owners = owners;
        Setters.Ran(nameof(Owners));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
