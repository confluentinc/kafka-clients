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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DeleteTopics(TopicCollection, DeleteTopicsOptions)"/> —
/// the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteTopicsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DeleteTopicsOptions
{
    private int? _timeoutMs;

    private bool _retryOnQuotaViolation = true;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs()"/>
    public int? TimeoutMs() => _timeoutMs;

    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs(int?)"/>
    public DeleteTopicsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
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
    public DeleteTopicsOptions RetryOnQuotaViolation(bool retryOnQuotaViolation)
    {
        _retryOnQuotaViolation = retryOnQuotaViolation;
        Setters.Ran(nameof(RetryOnQuotaViolation));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
