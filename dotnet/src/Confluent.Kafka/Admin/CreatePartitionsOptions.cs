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
/// Options for <see cref="IAdmin.CreatePartitions"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreatePartitionsOptions</c>.
/// </summary>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks"/>
public sealed class CreatePartitionsOptions
{
    private int? _timeoutMs;

    private bool _validateOnly;

    private bool _retryOnQuotaViolation = true;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public CreatePartitionsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Validate the request without actually creating the partitions — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <returns>Validate the request without actually creating the partitions — Java's <c>validateOnly()</c>.
    /// Defaults to <see langword="false"/>, as Java's does.</returns>
    public bool ValidateOnly() => _validateOnly;

    /// <summary>
    /// Set to <see langword="true"/> if the request should be validated without creating new partitions.
    /// </summary>
    /// <param name="validateOnly">Whether to validate without creating. Defaults to <see langword="false"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreatePartitionsOptions ValidateOnly(bool validateOnly)
    {
        _validateOnly = validateOnly;
        Setters.Ran(nameof(ValidateOnly));
        return this;
    }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>shouldRetryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.
    /// </summary>
    /// <returns>Retry automatically when the broker reports a quota violation — Java's
    /// <c>shouldRetryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.</returns>
    public bool ShouldRetryOnQuotaViolation() => _retryOnQuotaViolation;

    /// <summary>Set to <see langword="true"/> if quota violation should be automatically retried.</summary>
    /// <param name="retryOnQuotaViolation">Whether to retry. Defaults to <see langword="true"/>.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreatePartitionsOptions RetryOnQuotaViolation(bool retryOnQuotaViolation)
    {
        _retryOnQuotaViolation = retryOnQuotaViolation;
        Setters.Ran(nameof(RetryOnQuotaViolation));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
