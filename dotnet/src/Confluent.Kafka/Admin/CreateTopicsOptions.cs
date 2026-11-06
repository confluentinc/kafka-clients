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
/// Options for <see cref="IAdmin.CreateTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreateTopicsOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// Built as in Java, by chaining setters: <c>new CreateTopicsOptions().TimeoutMs(5000).ValidateOnly(true)</c>.
/// Each Java getter and setter of one name is a pair of overloads, the setter returning the options. Every
/// default is Java's, so <c>options: null</c> behaves as a new instance.
/// </para>
/// </remarks>
public sealed class CreateTopicsOptions
{
    private int? _timeoutMs;

    private bool _validateOnly;

    private bool _retryOnQuotaViolation = true;

    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it
    /// unset so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>, an <c>Integer</c> that is likewise nullable.
    /// </summary>
    /// <returns>The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset so the
    /// client's <c>default.api.timeout.ms</c> applies — Java's <c>AbstractOptions.timeoutMs()</c>, an
    /// <c>Integer</c> that is likewise nullable.</returns>
    /// <remarks>
    /// A negative value is sent as 0, Java's <c>Math.max(0, timeoutMs)</c>: the call is
    /// already expired and fails through its result with the core's timeout error.
    /// <see langword="null"/> is how you ask for the default.
    /// </remarks>
    public int? TimeoutMs() => _timeoutMs;

    /// <summary>Sets <see cref="TimeoutMs()"/>.</summary>
    /// <param name="timeoutMs">The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset
    /// so the client's <c>default.api.timeout.ms</c> applies — Java's <c>AbstractOptions.timeoutMs()</c>, an
    /// <c>Integer</c> that is likewise nullable.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateTopicsOptions TimeoutMs(int? timeoutMs)
    {
        _timeoutMs = timeoutMs;
        Setters.Ran(nameof(TimeoutMs));
        return this;
    }

    /// <summary>
    /// Validate the request without actually creating the topics — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    /// <returns>Validate the request without actually creating the topics — Java's <c>validateOnly()</c>. Defaults
    /// to <see langword="false"/>, as Java's does.</returns>
    public bool ValidateOnly() => _validateOnly;

    /// <summary>Sets <see cref="ValidateOnly()"/>.</summary>
    /// <param name="validateOnly">Validate the request without actually creating the topics — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateTopicsOptions ValidateOnly(bool validateOnly)
    {
        _validateOnly = validateOnly;
        Setters.Ran(nameof(ValidateOnly));
        return this;
    }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.
    /// </summary>
    /// <returns>Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.</returns>
    public bool RetryOnQuotaViolation() => _retryOnQuotaViolation;

    /// <summary>Sets <see cref="RetryOnQuotaViolation()"/>.</summary>
    /// <param name="retryOnQuotaViolation">Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.</param>
    /// <returns>This instance, so calls can be chained.</returns>
    public CreateTopicsOptions RetryOnQuotaViolation(bool retryOnQuotaViolation)
    {
        _retryOnQuotaViolation = retryOnQuotaViolation;
        Setters.Ran(nameof(RetryOnQuotaViolation));
        return this;
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
