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
    /// The owners to filter by — Java's <c>owners()</c>.
    /// </summary>
    /// <remarks>
    /// <see langword="null"/>, the default as in Java, describes every token the caller may
    /// see; an empty list matches none.
    /// </remarks>
    public IReadOnlyList<KafkaPrincipal>? Owners
    {
        get => _owners;
        set
        {
            _owners = value;
            Setters.Ran(nameof(Owners));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
