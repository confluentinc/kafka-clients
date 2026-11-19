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
using System.Collections.Generic;
using System.Threading.Tasks;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>The servicer flavour a behavioural test runs against; every one runs on both.</summary>
public enum ChaosFlavour
{
    Sync,

    Async,
}

/// <summary>The <c>[Theory]</c> rows for the flavours: every flavour theory runs once per row.</summary>
public static class ChaosFlavours
{
    public static TheoryData<ChaosFlavour> All => new TheoryData<ChaosFlavour> { ChaosFlavour.Sync, ChaosFlavour.Async };
}

/// <summary>
/// One chaos servicer under test, built through its internal client-factory seam over the flavour's
/// test doubles, or, from <see cref="CreateReal"/>, through its public constructor over the real clients.
/// </summary>
internal sealed class ChaosHarness : IDisposable
{
    internal const string Topic = "chaos-topic";

    private ChaosHarness(Proto.ChaosWorkloadService.ChaosWorkloadServiceBase service, ProducerProbe producer, ConsumerScript consumer)
    {
        Service = service;
        Producer = producer;
        Consumer = consumer;
    }

    internal Proto.ChaosWorkloadService.ChaosWorkloadServiceBase Service { get; }

    internal ProducerProbe Producer { get; }

    internal ConsumerScript Consumer { get; }

    internal static ChaosHarness Create(ChaosFlavour flavour, TestProducerBehaviour? producer = null)
    {
        TestProducerBehaviour behaviour = producer ?? new TestProducerBehaviour();
        ProducerProbe probe = new ProducerProbe();
        ConsumerScript script = new ConsumerScript();
        Proto.ChaosWorkloadService.ChaosWorkloadServiceBase service = flavour switch
        {
            ChaosFlavour.Sync => new ChaosWorkloadServiceImpl(
                config =>
                {
                    probe.Config = config;
                    return new SyncTestProducer(behaviour, probe);
                },
                config =>
                {
                    script.Config = config;
                    return new SyncScriptedConsumer(script);
                }),
            ChaosFlavour.Async => new AsyncChaosWorkloadServiceImpl(
                config =>
                {
                    probe.Config = config;
                    return new AsyncTestProducer(behaviour, probe);
                },
                config =>
                {
                    script.Config = config;
                    return new AsyncScriptedConsumer(script);
                }),
            _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
        };
        return new ChaosHarness(service, probe, script);
    }

    internal static ChaosHarness CreateReal(ChaosFlavour flavour)
    {
        Proto.ChaosWorkloadService.ChaosWorkloadServiceBase service = flavour switch
        {
            ChaosFlavour.Sync => new ChaosWorkloadServiceImpl(),
            ChaosFlavour.Async => new AsyncChaosWorkloadServiceImpl(),
            _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
        };
        return new ChaosHarness(service, new ProducerProbe(), new ConsumerScript());
    }

    internal static Proto.RunProducerRequest ProducerRequest(string workloadId, uint rps = 200, uint msgSize = 16)
    {
        Proto.RunProducerRequest request = new Proto.RunProducerRequest
        {
            WorkloadId = workloadId,
            Topic = Topic,
            TargetRps = rps,
            MsgSize = msgSize,
        };
        request.Config.Add("bootstrap.servers", "unused:9092");
        return request;
    }

    internal static Proto.RunConsumerRequest ConsumerRequest(
        string workloadId,
        Proto.CommitMode commitMode = Proto.CommitMode.Sync,
        uint commitCheckIntervalMs = 0,
        uint msgSize = 16,
        uint pollTimeoutMs = 10)
    {
        Proto.RunConsumerRequest request = new Proto.RunConsumerRequest
        {
            WorkloadId = workloadId,
            CommitMode = commitMode,
            PollTimeoutMs = pollTimeoutMs,
            CommitCheckIntervalMs = commitCheckIntervalMs,
            MsgSize = msgSize,
        };
        request.Topics.Add(Topic);
        request.Config.Add("bootstrap.servers", "unused:9092");
        request.Config.Add("group.id", "chaos-group");
        return request;
    }

    internal RunningCall RunProducer(Proto.RunProducerRequest request)
    {
        RecordingStreamWriter stream = new RecordingStreamWriter();
        TestServerCallContext context = new TestServerCallContext(stream);
        return new RunningCall(Service.RunProducer(request, stream, context), stream, context);
    }

    internal RunningCall RunConsumer(Proto.RunConsumerRequest request)
    {
        RecordingStreamWriter stream = new RecordingStreamWriter();
        TestServerCallContext context = new TestServerCallContext(stream);
        return new RunningCall(Service.RunConsumer(request, stream, context), stream, context);
    }

    internal async Task<Proto.StatusResponse> Stop(string workloadId)
    {
        using TestServerCallContext context = new TestServerCallContext(new RecordingStreamWriter());
        return await Service.StopWorkload(new Proto.StopWorkloadRequest { WorkloadId = workloadId }, context).ConfigureAwait(false);
    }

    internal async Task<bool> Mark(string workloadId, ulong marker)
    {
        using TestServerCallContext context = new TestServerCallContext(new RecordingStreamWriter());
        Proto.MarkWorkloadResponse response = await Service
            .MarkWorkload(new Proto.MarkWorkloadRequest { WorkloadId = workloadId, Marker = marker }, context)
            .ConfigureAwait(false);
        return response.Found;
    }

    public void Dispose() => ((IDisposable)Service).Dispose();
}

/// <summary>One in-progress <c>Run*</c> call: the handler's task, its stream and its context.</summary>
internal sealed class RunningCall : IDisposable
{
    internal RunningCall(Task handler, RecordingStreamWriter stream, TestServerCallContext context)
    {
        Handler = handler;
        Stream = stream;
        Context = context;
    }

    internal Task Handler { get; }

    internal RecordingStreamWriter Stream { get; }

    internal TestServerCallContext Context { get; }

    internal async Task<IReadOnlyList<Proto.WorkloadEvent>> Completed()
    {
        await Handler.WaitAsync(RecordingStreamWriter.DefaultTimeout).ConfigureAwait(false);
        return Stream.Events;
    }

    public void Dispose() => Context.Dispose();
}
