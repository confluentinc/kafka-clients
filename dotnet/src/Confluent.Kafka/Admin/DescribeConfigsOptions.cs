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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.DescribeConfigs"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeConfigsOptions</c>
/// (<c>DescribeConfigsOptions.java:25-69</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java declares exactly two fields of its own — <c>includeSynonyms</c> (<c>:27</c>) and
/// <c>includeDocumentation</c> (<c>:28</c>), both defaulting to <see langword="false"/> —
/// beside the inherited timeout, and <c>describe_configs_async</c> takes exactly those two
/// booleans plus <c>timeout_ms</c>.
/// </para>
/// </remarks>
public sealed class DescribeConfigsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Return each entry's synonyms, in precedence order — Java's
    /// <c>includeSynonyms(boolean)</c> / <c>includeSynonyms()</c>. Defaults to
    /// <see langword="false"/>, as Java's does, in which case
    /// <see cref="ConfigEntry.Synonyms"/> comes back empty.
    /// </summary>
    public bool IncludeSynonyms { get; set; }

    /// <summary>
    /// Return each entry's documentation — Java's <c>includeDocumentation(boolean)</c> /
    /// <c>includeDocumentation()</c>. Defaults to <see langword="false"/>, as Java's does,
    /// in which case <see cref="ConfigEntry.Documentation"/> comes back
    /// <see langword="null"/>.
    /// </summary>
    public bool IncludeDocumentation { get; set; }
}
