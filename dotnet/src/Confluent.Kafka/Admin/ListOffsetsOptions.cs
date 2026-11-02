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
/// Options for <see cref="IAdmin.ListOffsets"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListOffsetsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class ListOffsetsOptions
{
    private int? _timeoutMs;

    private IsolationLevel _isolationLevel = IsolationLevel.ReadUncommitted;

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
    /// The read isolation the query observes — Java's <c>isolationLevel()</c>. Defaults to
    /// <see cref="Confluent.Kafka.Common.IsolationLevel.ReadUncommitted"/>, as Java's does.
    /// </summary>
    public IsolationLevel IsolationLevel
    {
        get => _isolationLevel;
        set
        {
            _isolationLevel = value;
            Setters.Ran(nameof(IsolationLevel));
        }
    }

    internal OptionSetters Setters { get; } = new OptionSetters();
}
