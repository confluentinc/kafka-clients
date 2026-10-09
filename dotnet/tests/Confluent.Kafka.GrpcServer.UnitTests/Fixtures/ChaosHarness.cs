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

/// <summary>The servicer flavour a behavioural test runs against (PLAN §7.1: every one runs on both).</summary>
public enum ChaosFlavour
{
    /// <summary><see cref="ChaosWorkloadServiceImpl"/> over the sync clients (the <c>dotnet</c> backend).</summary>
    Sync,
}

/// <summary>The <c>[Theory]</c> rows for the flavours; S3 adds the async row here and nowhere else.</summary>
public static class ChaosFlavours
{
    /// <summary>Every flavour.</summary>
    public static TheoryData<ChaosFlavour> All => new TheoryData<ChaosFlavour> { ChaosFlavour.Sync };
}

/// <summary>
/// One chaos servicer under test, built through its internal client-factory seam (PLAN §5.1, D10)
/// over the flavour's test doubles — or, from <see cref="CreateReal"/>, through its public
/// constructor over the real clients. The only flavour-specific code in the suite lives in
/// <see cref="Create"/> and the doubles it builds.
/// </summary>
/// <remarks>
/// This test project is a <b>local gate, not run in CI</b> (PLAN Q2): run it with
/// <c>make -C dotnet test-grpc-server-dotnet</c>, which <c>test-dotnet</c> deliberately does not
/// call.
/// </remarks>
internal sealed class ChaosHarness : IDisposable
{
    internal const string Topic = "chaos-topic";

    private ChaosHarness(Proto.ChaosWorkloadService.ChaosWorkloadServiceBase service, ProducerProbe producer, ConsumerScript consumer)
    {
        Service = service;
        Producer = producer;
        Consumer = consumer;
    }

    /// <summary>The servicer.</summary>
    internal Proto.ChaosWorkloadService.ChaosWorkloadServiceBase Service { get; }

    /// <summary>What the producer double observed (empty for <see cref="CreateReal"/>).</summary>
    internal ProducerProbe Producer { get; }

    /// <summary>The consumer double's script and observations (empty for <see cref="CreateReal"/>).</summary>
    internal ConsumerScript Consumer { get; }

    /// <summary>A servicer of <paramref name="flavour"/> whose clients are that flavour's doubles.</summary>
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
            _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
        };
        return new ChaosHarness(service, probe, script);
    }

    /// <summary>A servicer of <paramref name="flavour"/> built as DI builds it, over the real clients.</summary>
    internal static ChaosHarness CreateReal(ChaosFlavour flavour)
    {
        Proto.ChaosWorkloadService.ChaosWorkloadServiceBase service = flavour switch
        {
            ChaosFlavour.Sync => new ChaosWorkloadServiceImpl(),
            _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
        };
        return new ChaosHarness(service, new ProducerProbe(), new ConsumerScript());
    }

    /// <summary>A producer request: <paramref name="rps"/> records per second of <paramref name="msgSize"/> bytes.</summary>
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

    /// <summary>A consumer request on <see cref="Topic"/>.</summary>
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

    /// <summary>Starts a <c>RunProducer</c> call; the returned call owns its stream and context.</summary>
    internal RunningCall RunProducer(Proto.RunProducerRequest request)
    {
        RecordingStreamWriter stream = new RecordingStreamWriter();
        TestServerCallContext context = new TestServerCallContext(stream);
        return new RunningCall(Service.RunProducer(request, stream, context), stream, context);
    }

    /// <summary>Starts a <c>RunConsumer</c> call; the returned call owns its stream and context.</summary>
    internal RunningCall RunConsumer(Proto.RunConsumerRequest request)
    {
        RecordingStreamWriter stream = new RecordingStreamWriter();
        TestServerCallContext context = new TestServerCallContext(stream);
        return new RunningCall(Service.RunConsumer(request, stream, context), stream, context);
    }

    /// <summary><c>StopWorkload</c>, through the servicer.</summary>
    internal async Task<Proto.StatusResponse> Stop(string workloadId)
    {
        using TestServerCallContext context = new TestServerCallContext(new RecordingStreamWriter());
        return await Service.StopWorkload(new Proto.StopWorkloadRequest { WorkloadId = workloadId }, context).ConfigureAwait(false);
    }

    /// <summary><c>MarkWorkload</c>, through the servicer; returns <c>found</c>.</summary>
    internal async Task<bool> Mark(string workloadId, ulong marker)
    {
        using TestServerCallContext context = new TestServerCallContext(new RecordingStreamWriter());
        Proto.MarkWorkloadResponse response = await Service
            .MarkWorkload(new Proto.MarkWorkloadRequest { WorkloadId = workloadId, Marker = marker }, context)
            .ConfigureAwait(false);
        return response.Found;
    }

    /// <inheritdoc/>
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

    /// <summary>The RPC handler's task: complete once the stream has ended.</summary>
    internal Task Handler { get; }

    /// <summary>The call's response stream.</summary>
    internal RecordingStreamWriter Stream { get; }

    /// <summary>The call's context.</summary>
    internal TestServerCallContext Context { get; }

    /// <summary>Waits for the handler to return (a hang guard), and returns every event written.</summary>
    internal async Task<IReadOnlyList<Proto.WorkloadEvent>> Completed()
    {
        await Handler.WaitAsync(RecordingStreamWriter.DefaultTimeout).ConfigureAwait(false);
        return Stream.Events;
    }

    /// <inheritdoc/>
    public void Dispose() => Context.Dispose();
}
