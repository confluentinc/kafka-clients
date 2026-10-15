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
using System.IO;

using Confluent.Kafka.GrpcServer.Chaos;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>Serializes the tests that swap the process-wide <see cref="Console.Error"/>.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ConsoleCollection
{
    /// <summary>The collection name.</summary>
    public const string Name = "Console.Error capture";
}

/// <summary>
/// <see cref="ChaosRecordOutcomes"/>' settle-once state machine (PLAN §5.5.3, D5; §7.1 T10),
/// driven directly: each record is settled exactly once against a <c>Send</c> that threw, and
/// whatever is not reported is logged.
/// </summary>
[Collection(ConsoleCollection.Name)]
public sealed class ChaosRecordOutcomesTests
{
    [Fact]
    public void AThrowThenACallback_ReportsTheThrowOnly_AndLogsTheAbsorbedCallback()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(events.Add, "w-absorb");
        ChaosRecordOutcomes.RecordCallback record = outcomes.ForRecord(3);
        RecordMetadata metadata = TestValues.Metadata();

        string log = CaptureLog(() =>
        {
            outcomes.SendRaised(record, new InvalidOperationException("x"));
            record.OnCompletion(metadata, null);
        });

        Proto.WorkloadEvent failed = Assert.Single(events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.SendFailed, failed.EventCase);
        Assert.Equal(3UL, failed.SendFailed.Index);
        Assert.Equal(-4, failed.SendFailed.Error.Code);
        Assert.Equal("dotnet server: InvalidOperationException: x", failed.SendFailed.Error.Message);
        Assert.Equal(
            "dotnet server: chaos producer w-absorb: WARNING: record 3 settled by its failed Send(); its callback " +
            "fired afterwards with metadata. The binding's contract that a throwing Send never fires its callback " +
            "(PLAN D5) was violated." + Environment.NewLine,
            log);
    }

    [Fact]
    public void ACallbackThenAThrow_ReportsTheCallbackOnly_AndLogsTheThrow()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(events.Add, "w-late");
        ChaosRecordOutcomes.RecordCallback record = outcomes.ForRecord(5);
        RecordMetadata metadata = TestValues.Metadata();

        string log = CaptureLog(() =>
        {
            record.OnCompletion(metadata, null);
            outcomes.SendRaised(record, new InvalidOperationException("late"));
        });

        Proto.WorkloadEvent delivered = Assert.Single(events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Delivered, delivered.EventCase);
        Assert.Equal(
            (5UL, metadata.Partition, metadata.Offset),
            (delivered.Delivered.Index, delivered.Delivered.Partition, delivered.Delivered.Offset));
        Assert.Equal(
            "dotnet server: chaos producer w-late: Send() of record 5 threw after its callback had settled it: " +
            "InvalidOperationException: late" + Environment.NewLine,
            log);
    }

    [Fact]
    public void TwoCallbacks_AreBothReported_SoTheVerifierSeesTheDoubleSettle()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(events.Add, "w-twice");
        ChaosRecordOutcomes.RecordCallback record = outcomes.ForRecord(8);
        RecordMetadata metadata = TestValues.Metadata();

        string log = CaptureLog(() =>
        {
            record.OnCompletion(metadata, null);
            record.OnCompletion(metadata, null);
        });

        Assert.Equal(2, events.Count);
        Assert.All(events, e =>
        {
            Assert.Equal(Proto.WorkloadEvent.EventOneofCase.Delivered, e.EventCase);
            Assert.Equal(8UL, e.Delivered.Index);
        });
        Assert.Equal(string.Empty, log);
    }

    [Fact]
    public void ACallbackWithAnError_IsReportedAsSendFailed_WithTheCoreCodeAndMessage()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(events.Add, "w-error");
        ChaosRecordOutcomes.RecordCallback record = outcomes.ForRecord(1);
        KafkaException error = TestValues.Coded(7, "injected delivery failure");

        record.OnCompletion(TestValues.Metadata(), error);

        Proto.WorkloadEvent failed = Assert.Single(events);
        Assert.Equal(1UL, failed.SendFailed.Index);
        Assert.Equal((7, error.Message), (failed.SendFailed.Error.Code, failed.SendFailed.Error.Message));
    }

    [Fact]
    public void ACallbackWithNeitherMetadataNorError_IsReportedAsAFailedSend()
    {
        List<Proto.WorkloadEvent> events = new List<Proto.WorkloadEvent>();
        ChaosRecordOutcomes outcomes = new ChaosRecordOutcomes(events.Add, "w-neither");
        ChaosRecordOutcomes.RecordCallback record = outcomes.ForRecord(2);

        record.OnCompletion(null!, null);

        Proto.WorkloadEvent failed = Assert.Single(events);
        Assert.Equal(Proto.WorkloadEvent.EventOneofCase.SendFailed, failed.EventCase);
        Assert.Equal(2UL, failed.SendFailed.Index);
        Assert.Equal(-4, failed.SendFailed.Error.Code);
        Assert.Equal("dotnet server: delivery callback fired with neither metadata nor error", failed.SendFailed.Error.Message);
    }

    private static string CaptureLog(Action action)
    {
        TextWriter original = Console.Error;
        using StringWriter captured = new StringWriter();
        Console.SetError(captured);
        try
        {
            action();
        }
        finally
        {
            Console.SetError(original);
        }

        return captured.ToString();
    }
}
