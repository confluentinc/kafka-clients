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
/// Options for <see cref="IAdmin.DescribeConfigs"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeConfigsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class DescribeConfigsOptions
{
    private int? _timeoutMs;

    private bool _includeSynonyms;

    private bool _includeDocumentation;

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
    /// Return each entry's synonyms, in precedence order — Java's
    /// <c>includeSynonyms(boolean)</c> / <c>includeSynonyms()</c>. Defaults to
    /// <see langword="false"/>, as Java's does, in which case
    /// <see cref="ConfigEntry.Synonyms"/> comes back empty.
    /// </summary>
    public bool IncludeSynonyms
    {
        get => _includeSynonyms;
        set
        {
            _includeSynonyms = value;
            Setters.Ran(nameof(IncludeSynonyms));
        }
    }

    /// <summary>
    /// Return each entry's documentation — Java's <c>includeDocumentation(boolean)</c> /
    /// <c>includeDocumentation()</c>. Defaults to <see langword="false"/>, as Java's does,
    /// in which case <see cref="ConfigEntry.Documentation"/> comes back
    /// <see langword="null"/>.
    /// </summary>
    public bool IncludeDocumentation
    {
        get => _includeDocumentation;
        set
        {
            _includeDocumentation = value;
            Setters.Ran(nameof(IncludeDocumentation));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
