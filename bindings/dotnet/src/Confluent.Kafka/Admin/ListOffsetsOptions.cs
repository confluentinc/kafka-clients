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
/// Options for <see cref="IAdmin.ListOffsets"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListOffsetsOptions</c>.
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// </remarks>
public sealed class ListOffsetsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The read isolation the query observes — Java's <c>isolationLevel()</c>. Defaults to
    /// <see cref="Confluent.Kafka.IsolationLevel.ReadUncommitted"/>, as Java's does.
    /// </summary>
    /// <remarks>
    /// That default is also the C# default for the underlying value (<c>0</c>), so
    /// <c>options: null</c> and a freshly constructed instance agree — but it is Java's
    /// default that decides it, not the language's.
    /// </remarks>
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadUncommitted;
}
