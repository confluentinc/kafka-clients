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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.ShareConsumer.Internal;

using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests.Interop;

/// <summary>
/// Wakeup, cancellation, and the guard-participating synchronous state read
/// (ffi-marshalling.md §B5). Scope note (source-verified): a <c>MockConsumer</c>
/// observes <c>wakeup()</c> <b>only</b> inside <c>poll()</c> — the proof ops
/// <c>subscribe</c> / <c>seek</c> do not check the flag, and <c>poll</c> is out of
/// scope this phase — so the "in-flight op faults with a Wakeup error" assertion is
/// not reachable here. These tests exercise the reachable slices: wakeup is safe /
/// leaves the consumer reusable, cancellation maps deterministically on the
/// pre-canceled path, and the guarded group-metadata read round-trips.
/// </summary>
public sealed class ConsumerAsyncOperationTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConsumerConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public async Task Wakeup_WhenIdle_IsSafe_AndConsumerRemainsReusable()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        // Safe with no op in flight (bypasses the guard, one-shot on the core).
        consumer.Wakeup();

        // Reusable: subscribe still succeeds after a wakeup (the "then the op works
        // again" half of the one-shot contract — the Mock observes wakeup only in
        // poll(), which is out of scope, so subscribe does not fault).
        await TestTimeout.Run(() => consumer.SubscribeAsync(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task Wakeup_DuringInFlightOp_DoesNotCorrupt()
    {
        // wakeup() is cross-thread and bypasses the guard; firing it while an op is
        // in flight must not corrupt the bridge. (On the Mock the op does not observe
        // the wakeup, so it completes normally — best-effort, Java-faithful.)
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        Task op = consumer.SubscribeAsync(ProofTopic());
        consumer.Wakeup();

        await TestTimeout.Run(() => op, s_deadline);
    }

    [Fact]
    public async Task SubscribeAsync_PreCanceledToken_ThrowsOperationCanceled()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        // Deterministic: an already-canceled token is honored before the native call
        // (OperationCanceledException, distinct from a wakeup KafkaException). The
        // in-flight-cancel → wakeup → OperationCanceledException translation is wired
        // (RegisterCancellation) but only deterministically reachable once a
        // wakeup-observing op (poll) lands.
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.SubscribeAsync(ProofTopic(), cts.Token));
    }

    [Fact]
    public async Task SeekAsync_PreCanceledToken_ThrowsOperationCanceled()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.SeekAsync("proof-topic", 0, 0L, cts.Token));
    }

    [Fact]
    public void GroupId_WhenIdle_ReturnsConfiguredId()
    {
        // The guarded synchronous state read (Category-3 owned handle → marshal →
        // destroy). A real KIP-848 consumer stubs group_metadata from the configured
        // id before join, so this round-trips broker-free (M2/P1 D5 precedent).
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("proof-group"));

        Assert.Equal("proof-group", consumer.GroupId());
    }

    [Fact]
    public void GroupId_NonAsciiId_RoundTripsThroughGuardedRead()
    {
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("grüße-café-Ω"));

        Assert.Equal("grüße-café-Ω", consumer.GroupId());
    }
}
