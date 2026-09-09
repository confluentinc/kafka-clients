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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The real Kafka admin client — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.KafkaAdminClient</c>, created from a
/// configuration map (Java's <c>Admin.create(Properties)</c>).
/// </summary>
/// <remarks>
/// Every RPC returns immediately with one awaitable per key; only
/// <see cref="Close(TimeSpan)"/> is awaited for the operation itself. See
/// <see cref="IAdmin"/> for why.
/// </remarks>
public sealed class KafkaAdminClient : IAdmin
{
    private readonly NativeAdminClient _native;

    /// <summary>
    /// Creates an admin client from a configuration map — Java's
    /// <c>Admin.create(Properties)</c>. Keys are the Java dotted names;
    /// <c>bootstrap.servers</c> is required.
    /// </summary>
    /// <param name="config">Configuration keyed by Java dotted names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is null.</exception>
    /// <exception cref="ArgumentException">A configuration value is null.</exception>
    /// <exception cref="KafkaException">The core rejected the configuration.</exception>
    public KafkaAdminClient(IReadOnlyDictionary<string, string> config)
    {
        _native = NativeAdminClient.Create(config);
    }

    /// <inheritdoc/>
    public CreateTopicsResult CreateTopics(IEnumerable<NewTopic> newTopics, CreateTopicsOptions? options = null) =>
        _native.CreateTopics(newTopics, options);

    /// <inheritdoc/>
    public DeleteTopicsResult DeleteTopics(TopicCollection topics, DeleteTopicsOptions? options = null) =>
        _native.DeleteTopics(topics, options);

    /// <inheritdoc/>
    public DescribeTopicsResult DescribeTopics(TopicCollection topics, DescribeTopicsOptions? options = null) =>
        _native.DescribeTopics(topics, options);

    /// <inheritdoc/>
    public Task Close(TimeSpan timeout) => _native.Close(timeout);

    /// <inheritdoc/>
    public void Dispose() => _native.Dispose();

    /// <inheritdoc/>
    public ValueTask DisposeAsync() => _native.DisposeAsync();
}
