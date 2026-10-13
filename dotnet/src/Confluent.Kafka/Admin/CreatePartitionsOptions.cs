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
/// Options for <see cref="IAdmin.CreatePartitions"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreatePartitionsOptions</c>.
/// </summary>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks"/>
public sealed class CreatePartitionsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Validate the request without actually creating the partitions — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ValidateOnly { get; set; }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <b><see langword="true"/></b>, as
    /// Java's does; note this is the one option whose default is not the C# default for
    /// its type.
    /// </summary>
    public bool RetryOnQuotaViolation { get; set; } = true;
}
