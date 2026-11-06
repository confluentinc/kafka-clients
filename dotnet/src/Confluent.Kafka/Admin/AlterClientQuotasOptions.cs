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
/// Options for <see cref="IAdmin.AlterClientQuotas"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.AlterClientQuotasOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class AlterClientQuotasOptions
{
    private int? _timeoutMs;
    private bool _validateOnly;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public AlterClientQuotasOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Whether to validate the alterations without applying them — Java's
    /// <c>validateOnly()</c>, default
    /// <see langword="false"/>.
    /// </summary>
    /// <returns>Whether to validate the alterations without applying them — Java's <c>validateOnly()</c>, default
    /// <see langword="false"/>.</returns>
    public bool ValidateOnly() => _validateOnly;

    /// <summary>Sets <see cref="ValidateOnly()"/>.</summary>
    /// <param name="validateOnly">Whether to validate the alterations without applying them — Java's
    /// <c>validateOnly()</c>, default <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public AlterClientQuotasOptions ValidateOnly(bool validateOnly)
    {
        _validateOnly = validateOnly;
        Setters.Ran(nameof(ValidateOnly));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
