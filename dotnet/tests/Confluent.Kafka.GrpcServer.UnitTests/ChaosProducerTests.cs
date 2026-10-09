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
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The producer workload (PLAN §3.3, §5.5; §7.1 T8, T9, T11, T12, T20) over the flavour's mock
/// producer behind the factory seam.
/// </summary>
public sealed class ChaosProducerTests
{
    /// <summary>How long to keep watching after the stream ended, before asserting "exactly once".</summary>
    private static readonly TimeSpan s_settleWindow = TimeSpan.FromMilliseconds(300);

    // ---- T8 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task HappyPath_EverySentIsDeliveredOnce_StatsBeforeFinished_FinishedLast(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        Proto.RunProducerRequest request = ChaosHarness.ProducerRequest("p-happy");
        using RunningCall call = harness.RunProducer(request);

        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Delivered, 20);
        await harness.Stop("p-happy");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Assert.Empty(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.SendFailed));

        List<ulong> sent = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Sent).Select(e => e.Sent.Index).ToList();
        Assert.Equal(Enumerable.Range(0, sent.Count).Select(i => (ulong)i), sent);
        foreach (ulong index in sent)
        {
            int sentAt = EventAssert.IndexOf(events, e => e.Sent?.Index == index && e.EventCase == Proto.WorkloadEvent.EventOneofCase.Sent, $"Sent({index})");
            List<int> deliveredAt = Enumerable.Range(0, events.Count)
                .Where(i => events[i].EventCase == Proto.WorkloadEvent.EventOneofCase.Delivered && events[i].Delivered.Index == index)
                .ToList();
            int delivered = Assert.Single(deliveredAt);
            Assert.True(sentAt < delivered, $"Delivered({index}) at {delivered} precedes its Sent at {sentAt}");
        }

        Proto.WorkloadEvent stats = Assert.Single(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.ProducerStats));
        Assert.Equal((ulong)sent.Count, stats.ProducerStats.Sent);
        Assert.True(stats.ProducerStats.ElapsedSeconds > 0);
        int statsAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.ProducerStats, "ProducerStats");
        int lastSentAt = events.ToList().FindLastIndex(e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Sent);
        Assert.True(lastSentAt < statsAt, "ProducerStats precedes a Sent");

        // The client was built from the request's config verbatim, closed once and disposed.
        Assert.Equal(request.Config.OrderBy(kv => kv.Key), harness.Producer.Config!.OrderBy(kv => kv.Key));
        Assert.Equal(1, harness.Producer.CloseCalls);
        Assert.Equal(1, harness.Producer.DisposeCalls);
        Assert.Equal(1, call.Stream.HeaderWrites);
        Assert.Equal(0, call.Stream.HeaderPosition);
        Assert.Equal(0, call.Stream.OverlappingWrites);
    }

    // ---- T9 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ErrorNext_IsReportedAsSendFailedThroughTheCallback_SettledOnce(ChaosFlavour flavour)
    {
        const int Code = 7;
        const string Message = "injected delivery failure";
        using ChaosHarness harness = ChaosHarness.Create(flavour, new TestProducerBehaviour { FailEach = (Code, Message) });
        using RunningCall call = harness.RunProducer(ChaosHarness.ProducerRequest("p-fail"));

        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.SendFailed, 10);
        await harness.Stop("p-fail");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();
        await Task.Delay(s_settleWindow);

        EventAssert.EndsFinished(events);
        Assert.Empty(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Delivered));
        List<ulong> sent = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Sent).Select(e => e.Sent.Index).ToList();
        IReadOnlyList<Proto.WorkloadEvent> failures = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.SendFailed);
        Assert.Equal(sent, failures.Select(e => e.SendFailed.Index).OrderBy(i => i));
        foreach (Proto.WorkloadEvent failure in failures)
        {
            Assert.Equal(Code, failure.SendFailed.Error.Code);
            Assert.Equal(Message, failure.SendFailed.Error.Message);
        }

        // Once per record at the client, too, after the settle window: no record's callback fired twice.
        Assert.Equal(sent.Count, harness.Producer.CallbackFires.Count);
        Assert.All(harness.Producer.CallbackFires, kv => Assert.Equal(1, kv.Value));
        Assert.Empty(harness.Producer.SendThrew);
    }

    // ---- T11 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ASendThatThrows_IsOneSendFailed_AndNoCallbackEverFollows(ChaosFlavour flavour)
    {
        const int Healthy = 5;
        using ChaosHarness harness = ChaosHarness.Create(flavour, new TestProducerBehaviour { DisposeAfterSends = Healthy });
        using RunningCall call = harness.RunProducer(ChaosHarness.ProducerRequest("p-throw"));

        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.SendFailed, 5);
        await harness.Stop("p-throw");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();
        await Task.Delay(s_settleWindow);

        EventAssert.EndsFinished(events);
        List<ulong> sent = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Sent).Select(e => e.Sent.Index).ToList();
        Assert.Equal(
            Enumerable.Range(0, Healthy).Select(i => (ulong)i),
            EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Delivered).Select(e => e.Delivered.Index).OrderBy(i => i));

        IReadOnlyList<Proto.WorkloadEvent> failures = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.SendFailed);
        Assert.Equal(sent.Skip(Healthy), failures.Select(e => e.SendFailed.Index));
        foreach (Proto.WorkloadEvent failure in failures)
        {
            Exception thrown = harness.Producer.SendThrew[failure.SendFailed.Index];
            Assert.IsType<ObjectDisposedException>(thrown);
            Assert.Equal(-4, failure.SendFailed.Error.Code);
            Assert.Equal($"dotnet server: ObjectDisposedException: {thrown.Message}", failure.SendFailed.Error.Message);
        }

        // The healthy records' callbacks fired once each; a throwing Send's never did.
        Assert.Equal(Enumerable.Range(0, Healthy).Select(i => (ulong)i), harness.Producer.CallbackFires.Keys.OrderBy(i => i));
        Assert.All(harness.Producer.CallbackFires, kv => Assert.Equal(1, kv.Value));
        Assert.Equal(sent.Count - Healthy, harness.Producer.SendThrew.Count);
    }

    // ---- T6b ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task StopWorkload_ReturnsPromptly_WhileTheDrainIsHeldInClose(ChaosFlavour flavour)
    {
        using ManualResetEventSlim closeGate = new ManualResetEventSlim();
        using ChaosHarness harness = ChaosHarness.Create(flavour, new TestProducerBehaviour { HoldClose = closeGate });
        using RunningCall call = harness.RunProducer(ChaosHarness.ProducerRequest("p-held"));
        try
        {
            call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Delivered, 3);

            // On another thread with a bound: a stop that ran the drain inline would block on the gate.
            await Task.Run(() => harness.Stop("p-held")).WaitAsync(TimeSpan.FromSeconds(5));

            Assert.True(harness.Producer.CloseEntered.Wait(RecordingStreamWriter.DefaultTimeout), "the drain never reached Close");
            Assert.False(call.Handler.IsCompleted, "the handler returned while its client's close was still held");
            Assert.Empty(EventAssert.OfKind(call.Stream.Events, Proto.WorkloadEvent.EventOneofCase.Finished));
        }
        finally
        {
            // Released on every path, so a failed assertion cannot leave the workload thread parked.
            closeGate.Set();
        }

        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Assert.Equal(1, harness.Producer.CloseCalls);
    }

    // ---- T17 (producer side) ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task CancellingTheCall_StopsTheWorkload_AndTheHandlerReturnsOnlyAfterTheClientIsClosed(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunProducer(ChaosHarness.ProducerRequest("p-cancel"));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Delivered, 3);

        call.Context.Cancel();
        await call.Handler.WaitAsync(RecordingStreamWriter.DefaultTimeout);

        Assert.Equal(1, harness.Producer.CloseCalls);
        Assert.Equal(1, harness.Producer.DisposeCalls);
        Assert.False(await harness.Mark("p-cancel", 1), "the cancelled workload is still registered");
        Assert.Empty(EventAssert.OfKind(call.Stream.Events, Proto.WorkloadEvent.EventOneofCase.Failed));
    }

    // ---- T12 (sync: structural) ----

    [Fact]
    public void Sync_NoStopTokenCanReachTheClient_ByConstruction()
    {
        // D6 / D7: the sync servicer holds no token to pass, and the sync client surface it calls
        // accepts none, so a stop can only be observed between calls, never inside one.
        const BindingFlags All = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
        Type servicer = typeof(ChaosWorkloadServiceImpl);
        Assert.DoesNotContain(servicer.GetFields(All), f => IsToken(f.FieldType));
        Assert.DoesNotContain(servicer.GetMethods(All).SelectMany(m => m.GetParameters()), p => IsToken(p.ParameterType));

        IEnumerable<Type> surface = new[] { typeof(IProducer<byte[], byte[]>), typeof(IConsumer<byte[], byte[]>), typeof(IConsumerCommon) };
        Assert.DoesNotContain(surface.SelectMany(t => t.GetMethods()).SelectMany(m => m.GetParameters()), p => IsToken(p.ParameterType));
    }

    // ---- T20 (producer side) ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ARealClientThatCannotBeBuilt_EndsTheWorkloadFailed_WithItsKafkaException(ChaosFlavour flavour)
    {
        KafkaException expected = Assert.Throws<KafkaException>(
            () => new KafkaProducer<byte[], byte[]>(new Dictionary<string, string>(), Serdes.ByteArray, Serdes.ByteArray));
        using ChaosHarness harness = ChaosHarness.CreateReal(flavour);
        Proto.RunProducerRequest request = ChaosHarness.ProducerRequest("p-real");
        request.Config.Clear();
        using RunningCall call = harness.RunProducer(request);

        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        Proto.WorkloadEvent failed = Assert.Single(events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Failed, failed.EventCase);
        Assert.Equal(expected.Code, failed.Failed.Error.Code);
        Assert.Equal(expected.Message, failed.Failed.Error.Message);
        Assert.Equal(0, call.Stream.HeaderPosition);
    }

    private static bool IsToken(Type type) =>
        type == typeof(CancellationToken) || type == typeof(CancellationToken?) || type == typeof(CancellationTokenSource);
}
