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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// One producer servicer, sync (<see cref="ProducerServiceImpl"/>) or async
/// (<see cref="AsyncProducerServiceImpl"/>), driven directly with a <see cref="TestServerCallContext"/>,
/// with a test producer or a mock producer that leaves its sends pending, of the servicer's flavour, placed
/// in its registry.
/// </summary>
internal sealed class ProducerServer : IDisposable
{
    private readonly IDisposable _servicer;

    private object? _pending;

    private ProducerServer(Proto.ProducerService.ProducerServiceBase service, IDisposable servicer)
    {
        Service = service;
        _servicer = servicer;
    }

    internal Proto.ProducerService.ProducerServiceBase Service { get; }

    internal TestServerCallContext Context { get; } = new TestServerCallContext(new RecordingStreamWriter());

    internal static ProducerServer Create(string flavour)
    {
        switch (flavour)
        {
            case "sync":
                ProducerServiceImpl sync = new ProducerServiceImpl();
                return new ProducerServer(sync, sync);
            case "async":
                AsyncProducerServiceImpl async = new AsyncProducerServiceImpl();
                return new ProducerServer(async, async);
            default:
                throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "sync or async");
        }
    }

    internal ulong RegisterTestProducer(ProducerProbe probe)
    {
        TestProducerBehaviour behaviour = new TestProducerBehaviour();
        return Register(_servicer is ProducerServiceImpl
            ? new SyncTestProducer(behaviour, probe)
            : new AsyncTestProducer(behaviour, probe));
    }

    internal ulong RegisterPendingMock()
    {
        _pending = _servicer is ProducerServiceImpl
            ? new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false)
            : new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        return Register(_pending);
    }

    internal int PendingHistoryCount() => _pending switch
    {
        MockProducer<byte[], byte[]> sync => sync.HistoryCount(),
        AsyncMockProducer<byte[], byte[]> async => async.HistoryCount(),
        _ => throw new InvalidOperationException("no pending mock registered"),
    };

    internal bool ErrorPendingNext(Exception exception) => _pending switch
    {
        MockProducer<byte[], byte[]> sync => sync.ErrorNext(exception),
        AsyncMockProducer<byte[], byte[]> async => async.ErrorNext(exception),
        _ => throw new InvalidOperationException("no pending mock registered"),
    };

    internal void SetSendTimeout(TimeSpan timeout) =>
        _servicer.GetType().GetField("_sendTimeout", BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(_servicer, timeout);

    private ulong Register(object producer)
    {
        IDictionary producers = (IDictionary)_servicer.GetType()
            .GetField("_producers", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(_servicer)!;
        ulong producerId = ulong.MaxValue;
        producers[producerId] = producer;
        return producerId;
    }

    public void Dispose()
    {
        _servicer.Dispose();
        Context.Dispose();
    }
}
