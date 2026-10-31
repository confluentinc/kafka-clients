// Copyright 2026 Confluent Inc.
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

using System.Collections.Concurrent;
using System.Threading;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// The group metadata a consumer's <c>GroupMetadata</c> RPC handed out, by id from 1, kept until
/// <c>ReleaseGroupMetadata</c> and shared by one server's servicers, as <c>grpc_translate.py</c>'s
/// <c>GroupMetadataStore</c>.
/// </summary>
internal sealed class GroupMetadataStore
{
    private readonly ConcurrentDictionary<ulong, ConsumerGroupMetadata> _entries =
        new ConcurrentDictionary<ulong, ConsumerGroupMetadata>();

    private long _lastId;

    internal ulong Add(ConsumerGroupMetadata groupMetadata)
    {
        ulong groupMetadataId = (ulong)Interlocked.Increment(ref _lastId);
        _entries[groupMetadataId] = groupMetadata;
        return groupMetadataId;
    }

    internal ConsumerGroupMetadata? Get(ulong groupMetadataId) =>
        _entries.TryGetValue(groupMetadataId, out ConsumerGroupMetadata? groupMetadata) ? groupMetadata : null;

    internal void Release(ulong groupMetadataId) => _entries.TryRemove(groupMetadataId, out _);
}
