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

using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.CreateDelegationToken(CreateDelegationTokenOptions)"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>Java's deprecated <c>maxlifeTimeMs</c> pair, a misspelling of <see cref="MaxLifetimeMs()"/>, is not bound.</para>
/// </remarks>
public sealed class CreateDelegationTokenOptions
{
    private int? _timeoutMs;
    private IReadOnlyList<KafkaPrincipal> _renewers = Array.Empty<KafkaPrincipal>();
    private KafkaPrincipal? _owner;
    private long _maxLifetimeMs = -1L;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public CreateDelegationTokenOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// The principals allowed to renew the token — Java's <c>renewers()</c>. Never null, as in Java: an empty list is
    /// a request for no renewers, unlike an unset <see cref="DescribeDelegationTokenOptions.Owners()"/>.
    /// </summary>
    /// <returns>The principals allowed to renew the token — Java's <c>renewers()</c>. Never null, as in Java: an
    /// empty list is a request for no renewers, unlike an unset
    /// <see cref="DescribeDelegationTokenOptions.Owners()"/>.</returns>
    public IReadOnlyList<KafkaPrincipal> Renewers() => _renewers;

    /// <summary>Sets the principals allowed to renew the token.</summary>
    /// <param name="renewers">The renewers; an empty list, the default, means none.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateDelegationTokenOptions Renewers(IReadOnlyList<KafkaPrincipal> renewers)
    {
        _renewers = renewers;
        Setters.Ran(nameof(Renewers));
        return this;
    }

    /// <summary>
    /// The token owner, or <see langword="null"/> to use the authenticated principal — Java's
    /// <c>owner()</c>, an <c>Optional</c>.
    /// </summary>
    /// <returns>The token owner, or <see langword="null"/> to use the authenticated principal — Java's
    /// <c>owner()</c>, an <c>Optional</c>.</returns>
    public KafkaPrincipal? Owner() => _owner;

    /// <summary>Sets the token's owner.</summary>
    /// <param name="owner">The owner, or <see langword="null"/>, the default, for the authenticated principal.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateDelegationTokenOptions Owner(KafkaPrincipal? owner)
    {
        _owner = owner;
        Setters.Ran(nameof(Owner));
        return this;
    }

    /// <summary>
    /// The token's maximum lifetime in milliseconds, or <c>-1</c> for the server default —
    /// Java's <c>maxLifetimeMs()</c>, default <c>-1</c>.
    /// </summary>
    /// <returns>The token's maximum lifetime in milliseconds, or <c>-1</c> for the server default — Java's
    /// <c>maxLifetimeMs()</c>, default <c>-1</c>.</returns>
    public long MaxLifetimeMs() => _maxLifetimeMs;

    /// <summary>Sets the token's maximum lifetime.</summary>
    /// <param name="maxLifetimeMs">The maximum lifetime in milliseconds, or <c>-1</c>, the default, for the
    /// server's.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateDelegationTokenOptions MaxLifetimeMs(long maxLifetimeMs)
    {
        _maxLifetimeMs = maxLifetimeMs;
        Setters.Ran(nameof(MaxLifetimeMs));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
