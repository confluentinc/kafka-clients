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
/// Options for <see cref="IAdmin.DescribeTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DescribeTopicsOptions</c>.
/// </summary>
/// <remarks>
/// A plain settable POCO rather than Java's fluent builder, and every default matches
/// Java's — the same shape and the same reasoning as
/// <see cref="CreateTopicsOptions"/>. The ABI has no options handle, so this is
/// destructured at the P/Invoke site.
/// </remarks>
public sealed class DescribeTopicsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it
    /// unset so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>, an <c>Integer</c> that is likewise nullable.
    /// </summary>
    /// <remarks>
    /// Must not be negative, for the reason given on
    /// <see cref="DeleteTopicsOptions.TimeoutMs"/>.
    /// </remarks>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Ask the broker to report each topic's authorized operations — Java's
    /// <c>includeAuthorizedOperations()</c>. Defaults to <see langword="false"/>, as
    /// Java's does.
    /// </summary>
    /// <remarks>
    /// While this is <see langword="false"/> the broker reports nothing, which is exactly
    /// the case <see cref="TopicDescription.AuthorizedOperations"/> renders as
    /// <see langword="null"/> rather than as an empty collection.
    /// </remarks>
    public bool IncludeAuthorizedOperations { get; set; }

    /// <summary>
    /// The maximum number of partitions the broker may return in a single response —
    /// Java's <c>partitionSizeLimitPerResponse()</c>. Defaults to <b>2000</b>, Java's own
    /// default (<c>DescribeTopicsOptions.java:28</c>), and is an <c>int</c> rather than a
    /// nullable one because Java's is too.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Java notes the option is effective only for a by-<em>name</em> request, and is
    /// capped by the broker's <c>max.request.partition.size.limit</c>.
    /// </para>
    /// <para>
    /// Must not be negative — <b>stricter than Java</b>, which accepts any <c>int</c>.
    /// The ABI maps a negative back to Java's 2000 default
    /// (<c>src/ffi/admin.rs:3712</c>, a private helper whose doc the generated header does
    /// not carry), so a negative would be silently reinterpreted rather than honoured.
    /// That is the same reasoning, and the same
    /// <see cref="System.ArgumentOutOfRangeException"/>, as
    /// <see cref="TimeoutMs"/>. Zero is passed through untouched, as Java would.
    /// </para>
    /// </remarks>
    public int PartitionSizeLimitPerResponse { get; set; } = 2000;
}
