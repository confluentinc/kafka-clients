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
/// Options for <see cref="IAdmin.AlterPartitionReassignments"/> — the .NET realization of
/// Java's <c>org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class AlterPartitionReassignmentsOptions
{
    private int? _timeoutMs;
    private bool _allowReplicationFactorChange = true;

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
    /// Allow the call to change a partition's replication factor — Java's
    /// <c>allowReplicationFactorChange(boolean)</c> /
    /// <c>allowReplicationFactorChange()</c>. Defaults to <b><see langword="true"/></b>, as Java's field does.
    /// </summary>
    /// <remarks>
    /// When <see langword="false"/>, a reassignment that would change the replication factor fails with
    /// <c>InvalidReplicationFactorException</c>, and a broker too old for the option fails with
    /// <c>UnsupportedVersionException</c>, each as a <see cref="KafkaException"/> on that partition's future.
    /// </remarks>
    public bool AllowReplicationFactorChange
    {
        get => _allowReplicationFactorChange;
        set
        {
            _allowReplicationFactorChange = value;
            Setters.Ran(nameof(AllowReplicationFactorChange));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
