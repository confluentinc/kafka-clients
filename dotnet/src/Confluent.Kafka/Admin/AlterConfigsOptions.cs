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
/// Options for <see cref="IAdmin.IncrementalAlterConfigs(System.Collections.Generic.IReadOnlyDictionary{Common.Config.ConfigResource, System.Collections.Generic.IReadOnlyCollection{AlterConfigOp}}, AlterConfigsOptions)"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterConfigsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class AlterConfigsOptions
{
    private int? _timeoutMs;

    private bool _validateOnly;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public AlterConfigsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Validate the request without applying it — Java's <c>validateOnly(boolean)</c> /
    /// <c>shouldValidateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <returns>Validate the request without applying it — Java's <c>validateOnly(boolean)</c> /
    /// <c>shouldValidateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.</returns>
    public bool ShouldValidateOnly() => _validateOnly;

    /// <summary>
    /// Set to <see langword="true"/> if the request should be validated without altering the configs.
    /// </summary>
    /// <param name="validateOnly">Whether to validate without altering. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public AlterConfigsOptions ValidateOnly(bool validateOnly)
    {
        _validateOnly = validateOnly;
        Setters.Ran(nameof(ValidateOnly));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
