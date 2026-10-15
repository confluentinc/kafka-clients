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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.Chaos;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The consumer workload (PLAN §3.4, §5.6; §7.1 T12, T14–T16, T18–T20) over the flavour's mock
/// consumer behind the factory seam, driven by steps applied on the workload's own loop.
/// </summary>
public sealed class ChaosConsumerTests
{
    private const uint MsgSize = 16;
    private const string Topic = ChaosHarness.Topic;

    /// <summary>The exact text a mock-derived handle's async operations fail with (core behaviour, ffi §B5).</summary>
    private const string MockHandleUnsupported =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.";

    // ---- T14 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task EachRecord_IsConsumedOrCorrupted_WithTheExactText_AndAPollErrorIsReportedAndSurvived(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-check", msgSize: MsgSize));
        harness.Consumer.Rebalance(new TopicPartition(Topic, 0));
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, MsgSize));
        harness.Consumer.AddRecord(Topic, 0, 1, new byte[3], ChaosEvents.Value(1, MsgSize));
        harness.Consumer.AddRecord(Topic, 0, 2, null, ChaosEvents.Value(2, MsgSize));
        harness.Consumer.AddRecord(Topic, 0, 3, Array.Empty<byte>(), ChaosEvents.Value(3, MsgSize));
        harness.Consumer.AddRecord(Topic, 0, 4, ChaosEvents.Key(4), ChaosEvents.Value(4, MsgSize - 1));
        harness.Consumer.AddRecord(Topic, 0, 5, ChaosEvents.Key(5), null);
        harness.Consumer.AddRecord(Topic, 0, 6, ChaosEvents.Key(6), Array.Empty<byte>());
        call.Stream.WaitFor(
            e => e.Count(x => x.EventCase is Proto.WorkloadEvent.EventOneofCase.Consumed or Proto.WorkloadEvent.EventOneofCase.Corrupted) == 7,
            "the seven records");

        harness.Consumer.SetPollError("injected poll failure");
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.ConsumerError, 1);

        // The loop goes on after the poll error.
        harness.Consumer.AddRecord(Topic, 0, 7, ChaosEvents.Key(7), ChaosEvents.Value(7, MsgSize));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 2);
        await harness.Stop("c-check");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Assert.Empty(harness.Consumer.StepFailures);
        Assert.Equal(
            new[] { (0UL, 0L), (7UL, 7L) },
            EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Consumed).Select(e => (e.Consumed.Index, e.Consumed.Offset)));
        Proto.WorkloadEvent first = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Consumed)[0];
        Assert.Equal((Topic, 0), (first.Consumed.Topic, first.Consumed.Partition));

        Assert.Equal(
            new[]
            {
                (1L, "key is 3 byte(s), expected the 8-byte index"),

                // T14c: an absent key and an empty key are different records end to end.
                (2L, "key is missing (expected the 8-byte index)"),
                (3L, "key is 0 byte(s), expected the 8-byte index"),
                (4L, "value of 15 byte(s) does not match the producer's encoding of index 4 (16 byte(s))"),

                // ...and so are an absent value and an empty one.
                (5L, "value is missing (expected 16 byte(s) encoding index 5)"),
                (6L, "value of 0 byte(s) does not match the producer's encoding of index 6 (16 byte(s))"),
            },
            EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Corrupted).Select(e => (e.Corrupted.Offset, e.Corrupted.Detail)));
        Assert.All(
            EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Corrupted),
            e => Assert.Equal((Topic, 0), (e.Corrupted.Topic, e.Corrupted.Partition)));

        Proto.WorkloadEvent pollError = Assert.Single(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.ConsumerError));
        Assert.Equal(Proto.ConsumerOp.Poll, pollError.ConsumerError.Op);
        Assert.Equal("injected poll failure", pollError.ConsumerError.Error.Message);
        // The mock injects a LocalIllegalState error (rust/src/ffi/consumer.rs,
        // kafka_consumer_MockConsumer_set_poll_error), reported with the core's own code.
        Assert.Equal(-4, pollError.ConsumerError.Error.Code);
    }

    // ---- T15 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ListenerEvents_PrecedeThatPollsRecords_AndARevokeCommitsThroughTheHandle(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        TopicPartition p0 = new TopicPartition(Topic, 0);
        TopicPartition p1 = new TopicPartition(Topic, 1);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-listen", commitCheckIntervalMs: 1));

        // The rebalance is applied before (or in the same poll as) the record, and fires inside
        // that poll, so the listener's event precedes the record either way.
        harness.Consumer.Rebalance(p0, p1);
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 1);

        harness.Consumer.Rebalance(p1);
        call.Stream.WaitFor(
            e => e.Any(x => x.ConsumerError?.Op == Proto.ConsumerOp.RevokeCommit),
            "the revoke-time commit's error");
        await harness.Stop("c-listen");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Assert.Empty(harness.Consumer.StepFailures);
        List<Proto.WorkloadEvent> rebalances = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Rebalance).ToList();
        Assert.True(rebalances.Count >= 3, RecordingStreamWriter.Describe(rebalances));

        Proto.Rebalance assigned = rebalances[0].Rebalance;
        Assert.Equal(Proto.RebalanceKind.Assigned, assigned.Kind);
        Assert.Equal(new[] { 0, 1 }, assigned.Partitions.Select(p => p.Partition));
        int assignedAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Rebalance, "Rebalance");
        int consumedAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Consumed, "Consumed");
        Assert.True(assignedAt < consumedAt, "the poll's records came before its rebalance");

        Proto.Rebalance revoked = rebalances[1].Rebalance;
        Assert.Equal(Proto.RebalanceKind.Revoked, revoked.Kind);
        Assert.Equal(new[] { (Topic, 0) }, revoked.Partitions.Select(p => (p.Topic, p.Partition)));
        Assert.Equal(Proto.RebalanceKind.Assigned, rebalances[2].Rebalance.Kind);

        // The revoke committed through the handle, which on a mock-derived handle is core
        // behaviour: UnsupportedVersion with this exact text. Being an error, it ends that
        // callback before any read-back.
        int revokedAt = events.ToList().IndexOf(rebalances[1]);
        Proto.WorkloadEvent revokeCommit = events[revokedAt + 1];
        Assert.Equal(Proto.ConsumerOp.RevokeCommit, revokeCommit.ConsumerError?.Op);
        Assert.Equal(35, revokeCommit.ConsumerError!.Error.Code);
        Assert.Equal(MockHandleUnsupported, revokeCommit.ConsumerError.Error.Message);
        Assert.DoesNotContain(events, e => e.ConsumerError?.Op == Proto.ConsumerOp.ReadCommitted);
    }

    // ---- T16 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task TheDrain_CommitsReadsBackClosesThenDisposesTheHandleBeforeTheConsumer(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-close", commitCheckIntervalMs: 60_000));
        harness.Consumer.Rebalance(new TopicPartition(Topic, 0));
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 1);

        await harness.Stop("c-close");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        int consumedAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Consumed, "Consumed");
        Assert.Equal(
            new[] { "Consumed", "Committed", "ConsumerClosing", "ConsumerClosed", "Finished" },
            EventAssert.Kinds(events.Skip(consumedAt)));
        Proto.Committed committed = EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Committed).Single().Committed;
        Assert.Equal((Topic, 0, 1L), (committed.Topic, committed.Partition, committed.Offset));

        // The loop's commit after the poll, then the drain: final commit, its read-back, close, dispose.
        Assert.Equal(new[] { "Commit", "Commit", "Committed", "Close", "Dispose" }, harness.Consumer.Calls);
        Assert.Equal(flavour == ChaosFlavour.Async, harness.Consumer.DisposedAsync);

        // The handle outlived the close (the close-time revoke may still commit through it) and
        // was disposed once the workload ended.
        Assert.True(harness.Consumer.HandleUsableAtClose);
        Assert.Throws<ObjectDisposedException>(() => harness.Consumer.Handle!.Assignment());
        Assert.DoesNotContain(events, e => e.ConsumerError is not null);
    }

    // ---- T18 ----

    [Fact]
    public void TheCommitCallback_ReportsAFailedCommit_WithItsCodeAndMessage_AndIsSilentOnSuccess()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosCommitCallback callback = new ChaosCommitCallback(events.Add);
        Dictionary<TopicPartition, OffsetAndMetadata> offsets = new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition(Topic, 0)] = new OffsetAndMetadata(4),
        };

        callback.OnComplete(offsets, null);
        Assert.Empty(events);

        KafkaException failure = TestValues.Coded(27, "rebalance in progress");
        callback.OnComplete(offsets, failure);

        Proto.WorkloadEvent error = Assert.Single(events);
        Assert.Equal(Proto.ConsumerOp.Commit, error.ConsumerError.Op);
        Assert.Equal(27, error.ConsumerError.Error.Code);
        Assert.Equal(failure.Message, error.ConsumerError.Error.Message);
        Assert.Equal("rebalance in progress", failure.Message);
    }

    // ---- T19 ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ReadBack_SyncCommitsWithACheckInterval_ReadBackWhileRunning(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-sync-check", commitCheckIntervalMs: 1));
        harness.Consumer.Rebalance(new TopicPartition(Topic, 0));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Rebalance, 1);
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));

        // A Committed before any stop is the periodic read-back, not the drain's.
        IReadOnlyList<Proto.WorkloadEvent> running = call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Committed, 1);
        Proto.Committed periodic = EventAssert.OfKind(running, Proto.WorkloadEvent.EventOneofCase.Committed)[0].Committed;
        Assert.Equal((Topic, 0, 1L), (periodic.Topic, periodic.Partition, periodic.Offset));
        Assert.True(
            EventAssert.IndexOf(running, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Consumed, "Consumed")
                < EventAssert.IndexOf(running, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.Committed, "Committed"),
            "the read-back preceded the record it reads back");

        await harness.Stop("c-sync-check");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        int closingAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.ConsumerClosing, "ConsumerClosing");
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Committed, events[closingAt - 1].EventCase);
    }

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ReadBack_AsyncCommits_ReadBackOnlyInTheDrain(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(
            ChaosHarness.ConsumerRequest("c-async-check", commitMode: Proto.CommitMode.Async, commitCheckIntervalMs: 1));
        harness.Consumer.Rebalance(new TopicPartition(Topic, 0));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Rebalance, 1);
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 1);

        // Let the loop run well past the interval, then fence the stream with a mark: everything
        // the loop emitted before the mark is in the stream once the mark is.
        int polls = harness.Consumer.Polls;
        // An idle poll writes nothing to the stream, so this waits on the script, not the stream.
        Assert.True(
            SpinWait.SpinUntil(() => harness.Consumer.Polls >= polls + 5, RecordingStreamWriter.DefaultTimeout),
            "the loop stopped polling");
        Assert.True(await harness.Mark("c-async-check", 99));
        IReadOnlyList<Proto.WorkloadEvent> running = call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Marker, 1);
        Assert.Empty(EventAssert.OfKind(running, Proto.WorkloadEvent.EventOneofCase.Committed));

        await harness.Stop("c-async-check");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Proto.WorkloadEvent final = Assert.Single(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Committed));
        Assert.Equal((Topic, 0, 1L), (final.Committed.Topic, final.Committed.Partition, final.Committed.Offset));
        int closingAt = EventAssert.IndexOf(events, e => e.EventCase == Proto.WorkloadEvent.EventOneofCase.ConsumerClosing, "ConsumerClosing");
        Assert.Same(final, events[closingAt - 1]);
        Assert.Equal(new[] { "CommitAsync", "Commit", "Committed", "Close", "Dispose" }, harness.Consumer.Calls);
        Assert.DoesNotContain(events, e => e.ConsumerError is not null);
    }

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ReadBack_NoCheckInterval_NeverReadsBack(ChaosFlavour flavour)
    {
        using ChaosHarness harness = ChaosHarness.Create(flavour);
        using RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest("c-no-check", commitCheckIntervalMs: 0));
        harness.Consumer.Rebalance(new TopicPartition(Topic, 0));
        harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, 16));
        call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Consumed, 1);

        await harness.Stop("c-no-check");
        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        EventAssert.EndsFinished(events);
        Assert.Empty(EventAssert.OfKind(events, Proto.WorkloadEvent.EventOneofCase.Committed));
        Assert.DoesNotContain("Committed", harness.Consumer.Calls);
    }

    // ---- T12 (async: at runtime, consumer side) ----

    [Fact]
    public async Task Async_NoTokenThatCanFireReachesTheConsumer_AtRuntime()
    {
        // D7: no token that can fire on Subscribe / Poll / Commit / Committed / Close. A sync-commit
        // workload with a read-back interval reaches all five, and the poll-error and revoke paths
        // too. One workload ends through StopWorkload and one through the call's cancellation; the
        // double records each token by whether it CAN fire (T12).
        using ChaosHarness stopped = ChaosHarness.Create(ChaosFlavour.Async);
        using RunningCall stoppedCall = RunToCommittedReadBack(stopped, "c-tokens-stop");
        await stopped.Stop("c-tokens-stop");
        EventAssert.EndsFinished(await stoppedCall.Completed());

        using ChaosHarness cancelled = ChaosHarness.Create(ChaosFlavour.Async);
        using RunningCall cancelledCall = RunToCommittedReadBack(cancelled, "c-tokens-cancel");
        cancelledCall.Context.Cancel();
        await cancelledCall.Handler.WaitAsync(RecordingStreamWriter.DefaultTimeout);

        foreach (ConsumerScript script in new[] { stopped.Consumer, cancelled.Consumer })
        {
            Assert.Empty(script.StepFailures);
            Assert.Empty(script.Tokens.Cancelable);
            Assert.Subset(
                new HashSet<string> { "Subscribe", "Poll", "Commit", "Committed", "Close" },
                new HashSet<string>(script.Tokens.Operations));
            Assert.Equal("Close", script.Calls[^2]);
        }

        static RunningCall RunToCommittedReadBack(ChaosHarness harness, string id)
        {
            RunningCall call = harness.RunConsumer(ChaosHarness.ConsumerRequest(id, commitCheckIntervalMs: 1));
            harness.Consumer.Rebalance(new TopicPartition(Topic, 0));

            // The record comes a poll later, so the read-back interval has elapsed by its commit (T19).
            call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Rebalance, 1);
            harness.Consumer.AddRecord(Topic, 0, 0, ChaosEvents.Key(0), ChaosEvents.Value(0, MsgSize));
            call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.Committed, 1);
            harness.Consumer.SetPollError("injected poll failure");
            call.Stream.WaitForCount(Proto.WorkloadEvent.EventOneofCase.ConsumerError, 1);
            harness.Consumer.Rebalance(new TopicPartition(Topic, 1));
            call.Stream.WaitFor(e => e.Any(x => x.ConsumerError?.Op == Proto.ConsumerOp.RevokeCommit), "the revoke-time commit's error");
            return call;
        }
    }

    // ---- T20 (consumer side) ----

    [Theory]
    [MemberData(nameof(ChaosFlavours.All), MemberType = typeof(ChaosFlavours))]
    public async Task ARealClientThatCannotBeBuilt_EndsTheWorkloadFailed_WithItsKafkaException(ChaosFlavour flavour)
    {
        // The flavour's own real client, built the way its servicer builds it.
        KafkaException expected = flavour == ChaosFlavour.Async
            ? Assert.Throws<KafkaException>(
                () => new AsyncKafkaConsumer<byte[], byte[]>(new Dictionary<string, string>(), Serdes.ByteArray, Serdes.ByteArray))
            : Assert.Throws<KafkaException>(
                () => new KafkaConsumer<byte[], byte[]>(new Dictionary<string, string>(), Serdes.ByteArray, Serdes.ByteArray));
        using ChaosHarness harness = ChaosHarness.CreateReal(flavour);
        Proto.RunConsumerRequest request = ChaosHarness.ConsumerRequest("c-real");
        request.Config.Clear();
        using RunningCall call = harness.RunConsumer(request);

        IReadOnlyList<Proto.WorkloadEvent> events = await call.Completed();

        Proto.WorkloadEvent failed = Assert.Single(events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Failed, failed.EventCase);
        Assert.Equal(expected.Code, failed.Failed.Error.Code);
        Assert.Equal(expected.Message, failed.Failed.Error.Message);
        Assert.Equal(0, call.Stream.HeaderPosition);
    }
}
