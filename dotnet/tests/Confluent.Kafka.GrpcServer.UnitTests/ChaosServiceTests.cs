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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.Chaos;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The RPC lifecycle shared by every workload (PLAN §5.2–§5.4; §7.1 T4–T7, T17): the id registry,
/// the response headers, stop and mark, and cancellation. Each runs on every flavour.
/// </summary>
public sealed class ChaosServiceTests
{
    // ---- T4 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ADuplicateWorkloadId_GetsOneFailedBatch_AndTheFirstWorkloadIsUnaffected(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall first = harness.RunProducer(ChaosHarness.ProducerRequest("w"));
        first.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Delivered, 2);

        using RunningCall duplicate = harness.RunProducer(ChaosHarness.ProducerRequest("w"));
        await duplicate.Completed();

        Proto.WorkloadEventBatch batch = Assert.Single(duplicate.Stream.Batches);
        Proto.WorkloadEvent failed = Assert.Single(batch.Events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Failed, failed.EventCase);
        Assert.Equal(-3, failed.Failed.Error.Code);
        Assert.Equal("dotnet server: workload_id 'w' is already running", failed.Failed.Error.Message);
        Assert.Equal(0, duplicate.Stream.HeaderWrites);

        // The first keeps running and still owns the id: it sends more, takes a mark, and stops cleanly.
        int deliveredSoFar = EventAssert.OfKind(first.Stream.Events, Proto.WorkloadEvent.EventOneofCase.Delivered).Count;
        first.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Delivered, deliveredSoFar + 2);
        Assert.True(await harness.Mark("w", 11));
        await harness.Stop("w");
        IReadOnlyList<Proto.WorkloadEvent> events = await first.Completed();

        EventAssert.EndsFinished(events);
        Assert.Single(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Marker));
        Assert.Empty(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.SendFailed));
    }

    // ---- T5 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task StopAndMark_OnAnUnknownId_AreOk_AndNotFound(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);

        Proto.StatusResponse stopped = await harness.Stop("nobody");
        bool found = await harness.Mark("nobody", 1);

        Assert.Null(stopped.Error);
        Assert.False(found);
    }

    // ---- T6 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task TheHeaders_AreWrittenBeforeAnyMessage_ForAConsumerThatNeverGetsARecord(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-idle"));

        call.Stream.WaitForHeaders();
        Assert.Equal(0, call.Stream.HeaderPosition);

        await harness.Stop("c-idle");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        Assert.Equal(1, call.Stream.HeaderWrites);
        Assert.Equal(0, call.Stream.HeaderPosition);
        Assert.Equal(
            new[] { "ConsumerClosing", "ConsumerClosed", "Finished" },
            EventAssert.Kinds(events));
    }

    // ---- T7 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task Marks_KeepTheirPlaceInTheEventOrder(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        TopicPartition partition = new TopicPartition(ChaosHarness.Topic, 0);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-mark"));
        call.Stream.WaitForHeaders();

        // An idle consumer emits nothing, so the two marks are the first events; the rebalance and
        // the record are only queued after they were taken, so they must come after both.
        Assert.True(await harness.Mark("c-mark", 1));
        Assert.True(await harness.Mark("c-mark", 2));
        harness.Consumer.Rebalance(partition);
        harness.Consumer.AddRecord(ChaosHarness.Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 1);

        // Taken after the record was observed, so it comes after it.
        Assert.True(await harness.Mark("c-mark", 3));
        await harness.Stop("c-mark");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        int mark1 = EventAssert.IndexOf(events, e => e.Marker?.Marker_ == 1, "Marker(1)");
        int mark2 = EventAssert.IndexOf(events, e => e.Marker?.Marker_ == 2, "Marker(2)");
        int rebalance = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Rebalance, "Rebalance");
        int consumed = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Consumed, "Consumed");
        int mark3 = EventAssert.IndexOf(events, e => e.Marker?.Marker_ == 3, "Marker(3)");
        int closing = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.ConsumerClosing, "ConsumerClosing");

        Assert.Equal(0, mark1);
        Assert.Equal(1, mark2);
        Assert.True(mark2 < rebalance && rebalance < consumed && consumed < mark3 && mark3 < closing, RecordingStreamWriter.Describe(events));
        Assert.Equal(3, EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Marker).Count);

        // A finished workload is deregistered: a mark no longer finds it.
        Assert.False(await harness.Mark("c-mark", 4));
    }

    [Fact]
    public void Mark_IsQueuedFifoBehindTheEventsAlreadyEmitted()
    {
        // The registry-level form of T7, read straight off the workload's channel.
        ChaosRegistry registry = new ChaosRegistry();
        ChaosWorkload workload = new ChaosWorkload("fifo");
        Assert.True(registry.TryAdd(workload));

        workload.Emit(ChaosEvents.Sent(0));
        workload.Emit(ChaosEvents.Sent(1));
        Assert.True(registry.Mark("fifo", 9));
        workload.Emit(ChaosEvents.Sent(2));

        List<string> order = new List<string>();
        while (workload.Events.TryRead(out Proto.WorkloadEvent? next))
        {
            order.Add(next.EventCase == Proto.WorkloadEvent.EventOneofCase.Marker ? $"Marker({next.Marker.Marker_})" : $"Sent({next.Sent.Index})");
        }

        Assert.Equal(new[] { "Sent(0)", "Sent(1)", "Marker(9)", "Sent(2)" }, order);
        Assert.False(registry.Mark("other", 9));
    }

    [Fact]
    public void Registry_RemovesOnlyTheEntryItWasGiven()
    {
        // A duplicate's handler must never deregister the running workload that owns the id.
        ChaosRegistry registry = new ChaosRegistry();
        ChaosWorkload owner = new ChaosWorkload("id");
        ChaosWorkload duplicate = new ChaosWorkload("id");
        Assert.True(registry.TryAdd(owner));
        Assert.False(registry.TryAdd(duplicate));

        registry.Remove(duplicate);
        Assert.Same(owner, registry.Get("id"));

        registry.Remove(owner);
        Assert.Null(registry.Get("id"));
    }

    // ---- T17 (consumer side) ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task CancellingTheCall_DrainsAndClosesTheConsumer_BeforeTheHandlerReturns(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-cancel"));
        harness.Consumer.Rebalance(new TopicPartition(ChaosHarness.Topic, 0));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Rebalance, 1);

        call.Context.Cancel();
        await call.Handler.WaitAsync(RecordingStreamWriter.DefaultTimeout);

        // The drain ran to the end before the handler returned: final commit, close, dispose.
        IReadOnlyList<string> calls = harness.Consumer.Calls;
        Assert.Equal(new[] { "Commit", "Close", "Dispose" }, calls.Skip(calls.Count - 3));
        Assert.False(await harness.Mark("c-cancel", 1), "the cancelled workload is still registered");
        Assert.Empty(EventAssert.OfKind(call.Stream.Events, Proto.WorkloadEvent.EventOneofCase.Failed));
        Assert.Equal(0, call.Stream.OverlappingWrites);

        // The id is free again.
        using RunningCall again = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-cancel"));
        again.Stream.WaitForHeaders();
        await harness.Stop("c-cancel");
        EventAssert.EndsFinished(await again.Completed());
    }
}
