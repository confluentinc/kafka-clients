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
/// A settable class rather than Java's fluent builder, since C# object initializers read as Java's chained setters
/// do. Every default is Java's, so <c>options: null</c> behaves as a new instance.
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
    /// <remarks>
    /// A negative value is sent as 0, Java's <c>Math.max(0, timeoutMs)</c>: the call is
    /// already expired and fails through its result with the core's timeout error.
    /// <see langword="null"/> is how you ask for the default.
    /// </remarks>
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
    /// Validate the request without actually creating the topics — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ValidateOnly
    {
        get => _validateOnly;
        set
        {
            _validateOnly = value;
            Setters.Ran(nameof(ValidateOnly));
        }
    }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <see langword="true"/>, as Java's does.
    /// </summary>
    public bool RetryOnQuotaViolation
    {
        get => _retryOnQuotaViolation;
        set
        {
            _retryOnQuotaViolation = value;
            Setters.Ran(nameof(RetryOnQuotaViolation));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
