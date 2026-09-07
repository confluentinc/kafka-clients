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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Deterministic coverage of <c>SendCompletionPump.CloseGate()</c> — the M11/P8 Major-5 fix.
/// </summary>
/// <remarks>
/// <para>
/// The public-API sibling (<c>PublicProducerSendTests.ConcurrentSendAndDispose_OnManualMock_DoesNotHang</c>)
/// races real sends against a real teardown, so whether it lands in the narrow post-flush /
/// pre-<c>Stop</c> window is timing-dependent. These tests assert the gate's <b>semantics</b>
/// directly and deterministically instead: after <c>CloseGate()</c>, <c>Enqueue</c> must take the
/// fault-in-place branch <b>synchronously</b> rather than queueing into a pump that teardown is
/// about to join. If the gate did not close, the send would be queued and the task would still be
/// pending when <c>Enqueue</c> returns — which is exactly the state that hung <c>_thread.Join()</c>.
/// </para>
/// <para>
/// A <see cref="IntPtr.Zero"/> future stands in for a real one: the fault-in-place branch frees the
/// future through <c>FutureRecordMetadata_destroy_all</c>, which skips null entries (null-safe by
/// contract), so no native handle is needed to exercise the branch.
/// </para>
/// </remarks>
public sealed class SendCompletionPumpGateTests
{
    [Fact]
    public void CloseGate_ThenEnqueue_FaultsInPlaceSynchronously()
    {
        SendCompletionPump pump = new SendCompletionPump();
        try
        {
            pump.CloseGate();

            TaskCompletionSource<RecordMetadata> completion =
                new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);

            // delivery: null — the plain Send(record) shape. The delivery-callback carrier (M14/P1)
            // is deliberately NOT fired on this fault-in-place branch; see Enqueue's remarks.
            pump.Enqueue(IntPtr.Zero, completion, delivery: null);

            // Synchronously faulted: the gate is closed, so Enqueue never handed this to the loop.
            Assert.True(completion.Task.IsFaulted);
            Assert.IsType<KafkaException>(completion.Task.Exception!.InnerException);
        }
        finally
        {
            pump.Stop();
        }
    }

    [Fact]
    public void Enqueue_WithoutCloseGate_IsNotFaultedInPlace()
    {
        // The control for the test above: with the gate OPEN the same call is queued, so the task is
        // still pending when Enqueue returns. This is what makes the assertion above meaningful —
        // it fails if CloseGate() stops closing the gate.
        SendCompletionPump pump = new SendCompletionPump();
        TaskCompletionSource<RecordMetadata> completion =
            new TaskCompletionSource<RecordMetadata>(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            pump.Enqueue(IntPtr.Zero, completion, delivery: null);
            Assert.False(completion.Task.IsFaulted);
        }
        finally
        {
            // Stop's terminal drain faults whatever the loop had not yet taken; either way the task
            // settles and the pump thread joins without hanging.
            pump.Stop();
        }
    }

    [Fact]
    public void CloseGate_IsIdempotent_AndStopStillReturns()
    {
        SendCompletionPump pump = new SendCompletionPump();
        pump.CloseGate();
        pump.CloseGate();

        // Stop() re-sets the same flag under the same lock — harmless, and the join still returns
        // (the loop was left running by CloseGate, exactly as teardown needs it to be).
        pump.Stop();
    }
}
