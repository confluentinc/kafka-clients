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

using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Confluent.Kafka.Producer;
using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The producer servicers' delivery-callback log entry, as Python's <c>make_logging_delivery_callback</c>
/// writes it (<c>dotnet/CLAUDE.md</c> §7.6).
/// </summary>
public sealed class LoggingDeliveryCallbackTests
{
    [Fact]
    public void OnCompletion_OfAFailedSend_LogsThePlaceholderAsItIs()
    {
        CallbackLog log = new CallbackLog();

        new LoggingDeliveryCallback(log, 7).OnCompletion(
            new RecordMetadata("t", -1, -1, -1), TestValues.Coded(13, "the record failed"));

        Proto.CallbackLogEntry entry = Assert.Single(log.Response(7).Entries);
        Assert.Equal("delivery", entry.Kind);
        Proto.CallbackLogPartition partition = Assert.Single(entry.Partitions);
        Assert.Equal("t", partition.Topic);
        Assert.Equal(-1, partition.Partition);
        KeyValuePair<string, long> offset = Assert.Single(entry.Offsets);
        Assert.Equal("t--1", offset.Key);
        Assert.Equal(-1L, offset.Value);
        Assert.Equal("the record failed", entry.Error);
    }
}
