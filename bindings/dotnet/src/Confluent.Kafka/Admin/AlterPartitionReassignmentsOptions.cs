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
/// Options for <see cref="IAdmin.AlterPartitionReassignments"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class AlterPartitionReassignmentsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Allow the call to change a partition's replication factor — Java's
    /// <c>allowReplicationFactorChange(boolean)</c> /
    /// <c>allowReplicationFactorChange()</c>
    /// (<c>AlterPartitionReassignmentsOptions.java:27, :34, :44</c>). Defaults to
    /// <b><see langword="true"/></b>, as Java's field does; note this is one of the
    /// options whose default is not the C# default for its type.
    /// </summary>
    /// <remarks>
    /// When <see langword="false"/>, Java documents that a reassignment which would
    /// change the replication factor fails with <c>InvalidReplicationFactorException</c>,
    /// and that a broker too old to understand the option fails with
    /// <c>UnsupportedVersionException</c> (<c>Admin.java</c>'s
    /// <c>alterPartitionReassignments</c> javadoc). Both arrive here as an ordinary
    /// per-partition <see cref="KafkaException"/> on that partition's awaitable.
    /// </remarks>
    public bool AllowReplicationFactorChange { get; set; } = true;
}
