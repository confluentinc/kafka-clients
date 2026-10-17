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

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// Settles each record from its delivery callback, exactly once against a <c>Send</c> that
/// threw — the .NET twin of Python's <c>_RecordOutcomes</c> (<c>python/grpc_chaos.py</c>),
/// shared by both flavours (PLAN §5.5.3, decision D5).
/// </summary>
/// <remarks>
/// <para>
/// <b>The state machine, verbatim from the anchor.</b> A throw is reported as
/// <c>SendFailed</c> only if the record's callback has not fired yet, and the first callback
/// after such a report is <b>absorbed</b> (logged): either way the record is settled once. Any
/// other callback is reported — including a second one for the same record, so the verifier
/// sees a client that settles a record twice.
/// </para>
/// <para>
/// <b>The absorb branch is unreachable in .NET</b>, and kept for parity (§3.5 item 5). The
/// delivery callback is managed-only (ffi §A6 form C): it fires only where the binding reads a
/// core completion, and a <c>Send</c> that throws hands the completion pump no future, so a
/// throw means no callback, ever (<c>IDeliveryCallback</c>'s remarks). The one "throws, yet the
/// record is sent" case — the async stage-1 <see cref="OperationCanceledException"/> from the
/// caller's token — is ruled out by never passing a token (D6). So if the branch is ever taken
/// it logs that the D5 contract was violated, and a binding regression shows up in the server
/// log instead of vanishing.
/// </para>
/// <para>
/// An error while building the outcome event is not left to the binding (which traces and
/// swallows whatever a delivery callback throws, so the record would look unsettled and the
/// client would be blamed): it is logged and ends the workload with <c>Failed</c>, a
/// server-side fault, as in Python.
/// </para>
/// <para>Test-server scaffolding with no Java class (DoD §7).</para>
/// </remarks>
internal sealed class ChaosRecordOutcomes
{
    private readonly Action<Proto.WorkloadEvent> _emit;
    private readonly string _workloadId;
    private readonly object _lock = new object();

    internal ChaosRecordOutcomes(Action<Proto.WorkloadEvent> emit, string workloadId)
    {
        _emit = emit;
        _workloadId = workloadId;
    }

    /// <summary>
    /// The delivery callback for record <paramref name="index"/>, which also carries its
    /// settlement state for <see cref="SendRaised"/> (Python <c>on_delivery(index)</c>).
    /// </summary>
    internal RecordCallback ForRecord(ulong index) => new RecordCallback(this, index);

    /// <summary>The <c>Send</c> of <paramref name="record"/>'s record threw <paramref name="error"/>.</summary>
    internal void SendRaised(RecordCallback record, Exception error)
    {
        bool report;
        lock (_lock)
        {
            report = record.Fired == 0 && !record.SendFailureReported;
            record.SendFailureReported = true;
        }

        if (report)
        {
            _emit(ChaosEvents.SendFailed(record.Index, error));
        }
        else
        {
            ChaosEvents.Log(
                $"chaos producer {_workloadId}: Send() of record {record.Index} threw after its callback " +
                $"had settled it: {error.GetType().Name}: {error.Message}");
        }
    }

    private void OnCompletion(RecordCallback record, RecordMetadata? metadata, KafkaException? exception)
    {
        bool absorbed;
        lock (_lock)
        {
            record.Fired++;
            absorbed = record.SendFailureReported && record.Fired == 1;
        }

        if (absorbed)
        {
            ChaosEvents.Log(
                $"chaos producer {_workloadId}: WARNING: record {record.Index} settled by its failed Send(); " +
                $"its callback fired afterwards with {(exception is not null ? "an error" : "metadata")}. " +
                "The binding's contract that a throwing Send never fires its callback (PLAN D5) was violated.");
            return;
        }

        Proto.WorkloadEvent outcome;
        try
        {
            outcome = ChaosEvents.Outcome(record.Index, metadata, exception);
        }
        catch (Exception e)
        {
            ChaosEvents.Log($"chaos producer {_workloadId}: building record {record.Index}'s outcome failed: {e}");
            outcome = FailedQuietly(e);
        }

        _emit(outcome);
    }

    private static Proto.WorkloadEvent FailedQuietly(Exception error)
    {
        try
        {
            return ChaosEvents.Failed(error);
        }
        catch (Exception)
        {
            return new Proto.WorkloadEvent
            {
                Failed = new Proto.Failed
                {
                    Error = new Proto.KafkaError
                    {
                        Code = ChaosEvents.LocalIllegalStateCode,
                        Message = "dotnet server: building a delivery outcome failed",
                    },
                },
            };
        }
    }

    /// <summary>
    /// One record's <see cref="IDeliveryCallback"/>: it fires on the producer's completion pump
    /// thread, so it only builds the event and queues it (PLAN §5.7) — anything slower would
    /// delay every other completion of the producer (ffi §A1).
    /// </summary>
    internal sealed class RecordCallback : IDeliveryCallback
    {
        private readonly ChaosRecordOutcomes _owner;

        internal RecordCallback(ChaosRecordOutcomes owner, ulong index)
        {
            _owner = owner;
            Index = index;
        }

        /// <summary>The record's logical index.</summary>
        internal ulong Index { get; }

        /// <summary>Callbacks fired so far (guarded by the owner's lock).</summary>
        internal int Fired { get; set; }

        /// <summary>Whether a <c>Send</c> failure was already reported (guarded by the owner's lock).</summary>
        internal bool SendFailureReported { get; set; }

        /// <inheritdoc/>
        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            try
            {
                _owner.OnCompletion(this, metadata, exception);
            }
            catch (Exception e)
            {
                // Unreachable (the owner's path is total), and the binding would swallow it
                // anyway; logged so a server fault is never silent.
                ChaosEvents.Log($"chaos producer {_owner._workloadId}: delivery callback failed: {e}");
            }
        }
    }
}
