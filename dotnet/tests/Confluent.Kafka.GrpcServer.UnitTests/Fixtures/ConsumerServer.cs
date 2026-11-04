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

using System;
using System.Collections;
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Consumer;
using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// One consumer servicer, sync (<see cref="ConsumerServiceImpl"/>) or async
/// (<see cref="AsyncConsumerServiceImpl"/>), driven directly with a <see cref="TestServerCallContext"/>;
/// its mock consumers come from its own <c>CreateConsumer</c> with an empty config, as the harness
/// selects one, or are a scripted double over the same mock that records the calls it gets.
/// </summary>
internal sealed class ConsumerServer : IDisposable
{
    private readonly IDisposable _servicer;

    private ConsumerServer(Proto.ConsumerService.ConsumerServiceBase service, IDisposable servicer, GroupMetadataStore groupMetadata)
    {
        Service = service;
        _servicer = servicer;
        GroupMetadata = groupMetadata;
    }

    internal Proto.ConsumerService.ConsumerServiceBase Service { get; }

    internal GroupMetadataStore GroupMetadata { get; }

    internal TestServerCallContext Context { get; } = new TestServerCallContext(new RecordingStreamWriter());

    internal static ConsumerServer Create(string flavour)
    {
        GroupMetadataStore store = new GroupMetadataStore();
        switch (flavour)
        {
            case "sync":
                ConsumerServiceImpl sync = new ConsumerServiceImpl(store);
                return new ConsumerServer(sync, sync, store);
            case "async":
                AsyncConsumerServiceImpl async = new AsyncConsumerServiceImpl(store);
                return new ConsumerServer(async, async, store);
            default:
                throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "sync or async");
        }
    }

    internal async Task<ulong> CreateMockConsumer()
    {
        Proto.CreateConsumerResponse response =
            await Service.CreateConsumer(new Proto.CreateConsumerRequest(), Context).ConfigureAwait(false);
        Assert.Null(response.Error);
        return response.ConsumerId;
    }

    internal void UpdateBeginningOffset(ulong consumerId, string topic, int partition, long offset)
    {
        switch (Consumer(consumerId))
        {
            case MockConsumer<byte[], byte[]> sync:
                sync.UpdateBeginningOffset(topic, partition, offset);
                break;
            case AsyncMockConsumer<byte[], byte[]> async:
                async.UpdateBeginningOffset(topic, partition, offset);
                break;
            default:
                throw new InvalidOperationException("not a mock consumer");
        }
    }

    internal void UpdateEndOffset(ulong consumerId, string topic, int partition, long offset)
    {
        switch (Consumer(consumerId))
        {
            case MockConsumer<byte[], byte[]> sync:
                sync.UpdateEndOffset(topic, partition, offset);
                break;
            case AsyncMockConsumer<byte[], byte[]> async:
                async.UpdateEndOffset(topic, partition, offset);
                break;
            default:
                throw new InvalidOperationException("not a mock consumer");
        }
    }

    internal ulong RegisterScripted(ConsumerScript script)
    {
        object consumer = _servicer is ConsumerServiceImpl
            ? new SyncScriptedConsumer(script)
            : new AsyncScriptedConsumer(script);
        Type entryType = _servicer.GetType().GetNestedType("ConsumerEntry", BindingFlags.NonPublic)!;
        object entry = Activator.CreateInstance(
            entryType, BindingFlags.NonPublic | BindingFlags.Instance, null, new[] { consumer }, null)!;
        ulong consumerId = ulong.MaxValue;
        Consumers()[consumerId] = entry;
        return consumerId;
    }

    private object Consumer(ulong consumerId)
    {
        object entry = Consumers()[consumerId]!;
        return entry.GetType().GetProperty("Consumer", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(entry)!;
    }

    private IDictionary Consumers() =>
        (IDictionary)_servicer.GetType().GetField("_consumers", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(_servicer)!;

    public void Dispose()
    {
        _servicer.Dispose();
        Context.Dispose();
    }
}
