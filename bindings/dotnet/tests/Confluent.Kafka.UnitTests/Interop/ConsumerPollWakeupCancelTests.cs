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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Wakeup + cancellation on the poll path (ffi-marshalling.md §B5), <b>now reachable</b>
/// because <c>poll</c> is the one op that observes <c>wakeup()</c> broker-free (the
/// M3/P1 D1 deferral). A <c>MockConsumer</c> checks the wakeup flag inside
/// <c>poll()</c> and clears it; a poll submitted with the flag set faults with a Wakeup
/// <see cref="KafkaException"/> <b>once</b>, then the consumer is reusable — Java's
/// one-shot <c>WakeupException</c> semantics, deterministically.
/// </summary>
/// <remarks>
/// <b>Determinism note (source-verified against <c>src/consumer/mock_consumer.rs</c>).</b>
/// The mock <c>poll</c> future runs to completion synchronously (no external-input
/// await): it drains one poll task, then checks-and-clears the wakeup flag, then takes
/// any injected poll error, then drains records. So the wakeup is observed regardless of
/// whether it "arrives before" or "during" the instant poll — the deterministic driver
/// is simply "set the flag, then poll", which is behaviorally identical to Java's
/// one-shot contract. A genuinely <em>in-flight</em> wakeup (fired while the poll is
/// blocked mid-flight) is NOT reachable, because the mock poll cannot be blocked at a
/// test-controlled point through the C ABI (no <c>schedule_poll_task</c> / block hook is
/// exposed) — see the in-flight-cancel residual below and COMMENTS.DONE.7.md.
/// </remarks>
public sealed class ConsumerPollWakeupCancelTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "wakeup-topic";
    private const int Partition = 0;

    private static async Task<NativeConsumer> MockReadyToPoll()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (Topic, Partition) });
        await consumer.SeekAsync(Topic, Partition, offset: 0);
        return consumer;
    }

    [Fact]
    public async Task Wakeup_ThenPoll_FaultsOnce_ThenReusable()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        // Queue a record so we can prove the SUBSEQUENT poll (after the one-shot wakeup
        // is consumed) actually returns data — not just an empty success.
        consumer.AddRecord(Topic, Partition, offset: 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

        // Set the one-shot wakeup flag, then poll: the mock observes it (Step 4, before
        // draining records) and faults with a Wakeup KafkaException.
        consumer.Wakeup();
        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => consumer.PollAsync(s_pollTimeout), s_deadline));
        Assert.False(string.IsNullOrEmpty(ex.Message));

        // One-shot: the flag was cleared by the faulted poll, so the next poll succeeds
        // and returns the queued record — the "then the op works again" half.
        ConsumerRecords records = await TestTimeoutResult(consumer.PollAsync(s_pollTimeout));
        Assert.Single(records);
    }

    [Fact]
    public async Task PollAsync_PreCanceledToken_ThrowsOperationCanceled()
    {
        // Deterministic: an already-canceled token is honored before the native call
        // (OperationCanceledException, distinct from a wakeup KafkaException), via the
        // synchronous ThrowIfCancellationRequested pre-check in SubmitOperation.
        using NativeConsumer consumer = await MockReadyToPoll();
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.PollAsync(s_pollTimeout, cts.Token));
    }

    [Fact]
    public async Task Wakeup_DuringInFlightPoll_DoesNotCorrupt()
    {
        // wakeup() is cross-thread and bypasses the guard; firing it concurrently with a
        // poll must not corrupt the bridge. Because the mock poll runs to completion
        // instantly, the outcome is EITHER a Wakeup fault (flag seen) OR a normal empty
        // success (poll finished first) — both are correct/Java-faithful. This asserts
        // no corruption (the op always resolves under the hang guard), not a specific
        // outcome; the deterministic one-shot behavior is covered above.
        using NativeConsumer consumer = await MockReadyToPoll();

        Task<ConsumerRecords> op = consumer.PollAsync(s_pollTimeout);
        consumer.Wakeup();

        try
        {
            await TestTimeout.Run(() => op, s_deadline);
        }
        catch (KafkaException)
        {
            // Wakeup fault — acceptable outcome (the flag was observed).
        }

        // The consumer remains reusable either way.
        ConsumerRecords after = await TestTimeoutResult(consumer.PollAsync(s_pollTimeout));
        Assert.Empty(after);
    }

    private static async Task<ConsumerRecords> TestTimeoutResult(Task<ConsumerRecords> op)
    {
        ConsumerRecords result = default!;
        await TestTimeout.Run(async () => result = await op, s_deadline);
        return result;
    }
}
