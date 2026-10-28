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
/// Settles each record exactly once between its delivery callback and a <c>Send</c> that threw, Python's
/// <c>_RecordOutcomes</c>: a throw is reported only if the callback has not fired, the first callback after
/// such a report is absorbed and logged, and any other callback is reported, a second one included.
/// </summary>
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

    internal RecordCallback ForRecord(ulong index) => new RecordCallback(this, index);

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

    private void OnCompletion(RecordCallback record, RecordMetadata? metadata, Exception? exception)
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
                $"chaos producer {_workloadId}: record {record.Index} settled by its failed Send(); " +
                $"its callback fired afterwards with {(exception is not null ? "an error" : "metadata")}");
            return;
        }

        Proto.WorkloadEvent outcome;
        try
        {
            outcome = ChaosEvents.Outcome(record.Index, metadata, exception);
        }
        catch (Exception e)
        {
            // A server-side fault, reported as Failed: thrown into the binding, it would leave the
            // record unsettled and the client would be blamed.
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
    /// One record's delivery callback and settlement state. It fires on the producer's completion thread,
    /// so it only builds the event and queues it.
    /// </summary>
    internal sealed class RecordCallback : IDeliveryCallback
    {
        private readonly ChaosRecordOutcomes _owner;

        internal RecordCallback(ChaosRecordOutcomes owner, ulong index)
        {
            _owner = owner;
            Index = index;
        }

        internal ulong Index { get; }

        // Fired and SendFailureReported are read and written under the owner's lock.
        internal int Fired { get; set; }

        internal bool SendFailureReported { get; set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            try
            {
                _owner.OnCompletion(this, metadata, exception);
            }
            catch (Exception e)
            {
                // Unreachable, as the owner's path is total; logged so a server fault is never
                // silent.
                ChaosEvents.Log($"chaos producer {_owner._workloadId}: delivery callback failed: {e}");
            }
        }
    }
}
