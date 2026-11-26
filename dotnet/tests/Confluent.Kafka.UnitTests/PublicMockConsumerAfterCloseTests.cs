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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// After <c>Close</c>, each of the mock consumers' own methods but <c>Closed()</c> throws the closed consumer's
/// <see cref="IllegalStateException"/>: <c>AddRecord</c> and <c>UpdatePartitions</c> as Java's <c>MockConsumer</c>
/// does, the others because the native mock is gone, where Java's answers (the recorded "After close" list).
/// </summary>
public sealed class PublicMockConsumerAfterCloseTests
{
    private const string ClosedMessage = "This consumer has already been closed.";

    private static readonly TopicPartition s_partition = new TopicPartition("topic", 0);

    private static readonly Dictionary<TopicPartition, long> s_offsets =
        new Dictionary<TopicPartition, long> { [s_partition] = 1L };

    public static TheoryData<string> Helpers => new TheoryData<string>
    {
        "AddRecord",
        "UpdatePartitions",
        "Rebalance",
        "LosePartitions",
        "SetMaxPollRecords",
        "SetPollException",
        "SetOffsetsException",
        "UpdateBeginningOffsets",
        "UpdateEndOffsets",
        "UpdateDurationOffsets",
        "ScheduleNopPollTask",
        "LastPollTimeout",
    };

    [Theory]
    [MemberData(nameof(Helpers))]
    public void MockConsumer_AHelperAfterClose_ThrowsTheClosedConsumersIllegalState(string helper)
    {
        using MockConsumer<string, string> consumer = new MockConsumer<string, string>("earliest");
        consumer.Close();

        IllegalStateException e = Assert.Throws<IllegalStateException>(() => Call(consumer, helper));

        Assert.Equal(ClosedMessage, e.Message);
        Assert.True(consumer.Closed());
    }

    [Theory]
    [MemberData(nameof(Helpers))]
    public async Task AsyncMockConsumer_AHelperAfterClose_ThrowsTheClosedConsumersIllegalState(string helper)
    {
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>("earliest");
        try
        {
            await consumer.Close();

            IllegalStateException e = Assert.IsType<IllegalStateException>(
                await Record.ExceptionAsync(() => CallAsync(consumer, helper)));

            Assert.Equal(ClosedMessage, e.Message);
            Assert.True(consumer.Closed());
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    private static void Call(MockConsumer<string, string> consumer, string helper)
    {
        switch (helper)
        {
            case "AddRecord":
                consumer.AddRecord(new ConsumerRecord<string, string>("topic", 0, 0L, "key", "value"));
                break;
            case "UpdatePartitions":
                consumer.UpdatePartitions("topic", new List<PartitionInfo>());
                break;
            case "Rebalance":
                consumer.Rebalance(new[] { s_partition });
                break;
            case "LosePartitions":
                consumer.LosePartitions(new[] { s_partition });
                break;
            case "SetMaxPollRecords":
                consumer.SetMaxPollRecords(10L);
                break;
            case "SetPollException":
                consumer.SetPollException(new KafkaException("poll"));
                break;
            case "SetOffsetsException":
                consumer.SetOffsetsException(new KafkaException("offsets"));
                break;
            case "UpdateBeginningOffsets":
                consumer.UpdateBeginningOffsets(s_offsets);
                break;
            case "UpdateEndOffsets":
                consumer.UpdateEndOffsets(s_offsets);
                break;
            case "UpdateDurationOffsets":
                consumer.UpdateDurationOffsets(s_offsets);
                break;
            case "ScheduleNopPollTask":
                consumer.ScheduleNopPollTask();
                break;
            case "LastPollTimeout":
                _ = consumer.LastPollTimeout();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(helper), helper, null);
        }
    }

    private static Task CallAsync(AsyncMockConsumer<string, string> consumer, string helper)
    {
        switch (helper)
        {
            case "AddRecord":
                consumer.AddRecord(new ConsumerRecord<string, string>("topic", 0, 0L, "key", "value"));
                break;
            case "UpdatePartitions":
                consumer.UpdatePartitions("topic", new List<PartitionInfo>());
                break;
            case "Rebalance":
                return consumer.Rebalance(new[] { s_partition });
            case "LosePartitions":
                return consumer.LosePartitions(new[] { s_partition });
            case "SetMaxPollRecords":
                consumer.SetMaxPollRecords(10L);
                break;
            case "SetPollException":
                consumer.SetPollException(new KafkaException("poll"));
                break;
            case "SetOffsetsException":
                consumer.SetOffsetsException(new KafkaException("offsets"));
                break;
            case "UpdateBeginningOffsets":
                consumer.UpdateBeginningOffsets(s_offsets);
                break;
            case "UpdateEndOffsets":
                consumer.UpdateEndOffsets(s_offsets);
                break;
            case "UpdateDurationOffsets":
                consumer.UpdateDurationOffsets(s_offsets);
                break;
            case "ScheduleNopPollTask":
                consumer.ScheduleNopPollTask();
                break;
            case "LastPollTimeout":
                _ = consumer.LastPollTimeout();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(helper), helper, null);
        }

        return Task.CompletedTask;
    }
}
