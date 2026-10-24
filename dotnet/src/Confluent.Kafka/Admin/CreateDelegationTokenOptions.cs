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

using System;
using System.Collections.Generic;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.CreateDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>Java's deprecated <c>maxlifeTimeMs</c> pair, a misspelling of <see cref="MaxLifetimeMs"/>, is not bound.</para>
/// </remarks>
public sealed class CreateDelegationTokenOptions
{
    private int? _timeoutMs;
    private IReadOnlyList<KafkaPrincipal> _renewers = Array.Empty<KafkaPrincipal>();
    private KafkaPrincipal? _owner;
    private long _maxLifetimeMs = -1L;

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
    /// The principals allowed to renew the token — Java's <c>renewers()</c>. Never null, as in Java: an empty list is
    /// a request for no renewers, unlike an unset <see cref="DescribeDelegationTokenOptions.Owners"/>.
    /// </summary>
    public IReadOnlyList<KafkaPrincipal> Renewers
    {
        get => _renewers;
        set
        {
            _renewers = value;
            Setters.Ran(nameof(Renewers));
        }
    }

    /// <summary>
    /// The token owner, or <see langword="null"/> to use the authenticated principal — Java's
    /// <c>owner()</c>, an <c>Optional</c>.
    /// </summary>
    public KafkaPrincipal? Owner
    {
        get => _owner;
        set
        {
            _owner = value;
            Setters.Ran(nameof(Owner));
        }
    }

    /// <summary>
    /// The token's maximum lifetime in milliseconds, or <c>-1</c> for the server default —
    /// Java's <c>maxLifetimeMs()</c>, default <c>-1</c>.
    /// </summary>
    public long MaxLifetimeMs
    {
        get => _maxLifetimeMs;
        set
        {
            _maxLifetimeMs = value;
            Setters.Ran(nameof(MaxLifetimeMs));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
