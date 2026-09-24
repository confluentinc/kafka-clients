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
/// Options for <see cref="IAdmin.DescribeFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeFeaturesOptions</c>.
/// </summary>
public sealed class DescribeFeaturesOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset —
    /// Java's <c>AbstractOptions.timeoutMs()</c>. Must not be negative.
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The broker to query, or <see langword="null"/> to let the client choose — Java's
    /// <c>nodeId()</c> (<c>:39</c>), an <c>OptionalInt</c> (<c>:25</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Absence is carried by the ABI's <c>has_node_id</c> flag, never by a sentinel:
    /// <c>0</c> is a legal broker id.
    /// </remarks>
    public int? NodeId { get; set; }
}
