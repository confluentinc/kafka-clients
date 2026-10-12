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
using System.Threading.Tasks;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// Real library values a test cannot construct itself: <see cref="RecordMetadata"/> and a coded
/// <see cref="KafkaException"/> have internal constructors, so they are obtained from a real
/// <see cref="MockProducer{TKey, TValue}"/> round-trip, exactly as the servicer receives them.
/// </summary>
internal static class TestValues
{
    /// <summary>A real delivery's metadata.</summary>
    internal static RecordMetadata Metadata()
    {
        using MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        RecordMetadata metadata = producer.Send(new ProducerRecord<byte[], byte[]>("t", new byte[] { 1 })).Get();
        producer.Close();
        return metadata;
    }

    /// <summary>The <see cref="KafkaException"/> a delivery callback receives for a record failed with <c>ErrorNext(code, message)</c>.</summary>
    internal static KafkaException Coded(int code, string message)
    {
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        CapturingCallback callback = new CapturingCallback();
        producer.Send(new ProducerRecord<byte[], byte[]>("t", new byte[] { 1 }), callback);
        Assert.True(producer.ErrorNext(code, message));
        KafkaException? error = callback.Outcome.Wait(RecordingStreamWriter.DefaultTimeout)
            ? callback.Outcome.Result
            : throw new TimeoutException("the mock's delivery callback never fired");
        producer.Close();
        return error ?? throw new InvalidOperationException("ErrorNext delivered a success");
    }

    private sealed class CapturingCallback : IDeliveryCallback
    {
        private readonly TaskCompletionSource<KafkaException?> _outcome =
            new TaskCompletionSource<KafkaException?>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<KafkaException?> Outcome => _outcome.Task;

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) => _outcome.TrySetResult(exception);
    }
}

/// <summary>Assertions over a recorded event stream.</summary>
internal static class EventAssert
{
    /// <summary>The position of the first event matching <paramref name="predicate"/>; fails when there is none.</summary>
    internal static int IndexOf(IReadOnlyList<Proto.WorkloadEvent> events, Func<Proto.WorkloadEvent, bool> predicate, string what)
    {
        for (int i = 0; i < events.Count; i++)
        {
            if (predicate(events[i]))
            {
                return i;
            }
        }

        Assert.Fail($"no {what} among {events.Count} event(s): {RecordingStreamWriter.Describe(events)}");
        return -1;
    }

    /// <summary>The events of one kind.</summary>
    internal static IReadOnlyList<Proto.WorkloadEvent> OfKind(IEnumerable<Proto.WorkloadEvent> events, Proto.WorkloadEvent.EventOneofCase kind) =>
        events.Where(e => e.EventCase == kind).ToList();

    /// <summary>The stream ended with exactly one terminal event, <c>Finished</c>, last.</summary>
    internal static void EndsFinished(IReadOnlyList<Proto.WorkloadEvent> events)
    {
        Assert.NotEmpty(events);
        Assert.True(
            events[^1].EventCase == Proto.WorkloadEvent.EventOneofCase.Finished,
            $"the last event is {events[^1]}, not Finished; stream: {RecordingStreamWriter.Describe(events.Skip(Math.Max(0, events.Count - 20)))}");
        Assert.Single(OfKind(events, Proto.WorkloadEvent.EventOneofCase.Finished));
        Assert.Empty(OfKind(events, Proto.WorkloadEvent.EventOneofCase.Failed));
    }

    /// <summary>The order of the named consumer events in the stream, as kind names.</summary>
    internal static IReadOnlyList<string> Kinds(IEnumerable<Proto.WorkloadEvent> events) =>
        events.Select(e => e.EventCase.ToString()).ToList();
}
