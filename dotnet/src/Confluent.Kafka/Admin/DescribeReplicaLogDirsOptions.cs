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
/// Options for <see cref="IAdmin.DescribeReplicaLogDirs"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeReplicaLogDirsOptions</c> (<c>DescribeReplicaLogDirsOptions.java:25</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java's <c>DescribeReplicaLogDirsOptions</c> declares <b>no members of its own</b> — it is an
/// empty subclass of <c>AbstractOptions</c>, so <see cref="TimeoutMs"/> is the whole
/// surface, exactly as for <see cref="DeleteRecordsOptions"/>. The type still exists
/// because Java's does, and because a later Kafka release adding a field must not become a
/// signature change here.
/// </para>
/// </remarks>
public sealed class DescribeReplicaLogDirsOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }
}
