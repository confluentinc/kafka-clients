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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Wakeup, cancellation, and the synchronous state read (ffi-marshalling.md §B5)
/// under the single-owner (not-thread-safe) model — no managed access guard; the
/// Rust core serializes ops. Scope note (source-verified): a <c>MockConsumer</c>
/// observes <c>wakeup()</c> <b>only</b> inside <c>poll()</c> — the proof ops
/// <c>subscribe</c> / <c>seek</c> do not check the flag, and <c>poll</c> is out of
/// scope this phase — so the "in-flight op faults with a Wakeup error" assertion is
/// not reachable here. Likewise a genuine concurrent core-guard rejection of
/// <c>GroupId</c> (→ <see cref="InvalidOperationException"/>) is not deterministically
/// reproducible broker-free: the near-instant Mock ops hold the core guard only for
/// microseconds and the one guard-holding op with a controllable duration is
/// <c>poll</c> (out of scope) — the concurrent → <see cref="InvalidOperationException"/>
/// mapping is verified by code inspection of the null-handle path and documented in
/// <c>COMMENTS.DONE.6.md</c> (D-Q4). These tests exercise the reachable slices: wakeup
/// is safe / leaves the consumer reusable, cancellation maps deterministically on the
/// pre-canceled path, and the group-metadata read round-trips (incl. non-ASCII).
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
        await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task Wakeup_DuringInFlightOp_DoesNotCorrupt()
    {
        // wakeup() is cross-thread and bypasses the guard; firing it while an op is
        // in flight must not corrupt the bridge. (On the Mock the op does not observe
        // the wakeup, so it completes normally — best-effort, Java-faithful.)
        using NativeConsumer consumer = NativeConsumer.CreateMock();

        Task op = consumer.SubscribeWithCallback(ProofTopic());
        consumer.Wakeup();

        await TestTimeout.Run(() => op, s_deadline);
    }

    [Fact]
    public async Task SubscribeWithCallback_PreCanceledToken_ThrowsOperationCanceled()
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
            () => consumer.SubscribeWithCallback(ProofTopic(), cts.Token));
    }

    [Fact]
    public async Task SeekWithCallback_PreCanceledToken_ThrowsOperationCanceled()
    {
        using NativeConsumer consumer = NativeConsumer.CreateMock();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.SeekWithCallback("proof-topic", 0, 0L, cts.Token));
    }

    [Fact]
    public void GroupId_WhenIdle_ReturnsConfiguredId()
    {
        // The synchronous state read (Category-3 owned handle → marshal → destroy),
        // now unguarded (no managed access guard under single-owner). A real KIP-848
        // consumer stubs group_metadata from the configured id before join, so this
        // round-trips broker-free (M2/P1 D5 precedent). On a genuine concurrent
        // core-guard rejection the null-handle path throws InvalidOperationException
        // (mirrors Python _concurrent_error) — not deterministically reproducible
        // broker-free (D-Q4), verified by code inspection.
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("proof-group"));

        Assert.Equal("proof-group", consumer.GroupId());
    }

    [Fact]
    public void GroupId_NonAsciiId_RoundTrips()
    {
        using NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("grüße-café-Ω"));

        Assert.Equal("grüße-café-Ω", consumer.GroupId());
    }
}
