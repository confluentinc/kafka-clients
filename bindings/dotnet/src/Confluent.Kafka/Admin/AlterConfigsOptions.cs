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
/// Options for <see cref="IAdmin.IncrementalAlterConfigs"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterConfigsOptions</c>
/// (<c>AlterConfigsOptions.java:25-53</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java declares exactly one field of its own — <c>validateOnly</c> (<c>:27</c>),
/// defaulting to <see langword="false"/> — beside the inherited timeout.
/// </para>
/// </remarks>
public sealed class AlterConfigsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Validate the request without applying it — Java's <c>validateOnly(boolean)</c> /
    /// <c>shouldValidateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ValidateOnly { get; set; }
}
