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
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.AotSmoke;

/// <summary>
/// The Native AOT check of the Confluent.Kafka library: its publish compiles the whole library ahead of time, and
/// its run drives the library's AOT-sensitive paths in the native executable, each against the text it must give.
/// The real clients run against an unreachable broker, so no broker is needed.
/// </summary>
internal static class Program
{
    private const string Unreachable = "127.0.0.1:1";

    private static readonly ISerializer<string> s_serializer = Serdes.String().Serializer();

    private static readonly IDeserializer<string> s_deserializer = Serdes.String().Deserializer();

    private static int s_failures;

    private static async Task<int> Main()
    {
        if (RuntimeFeature.IsDynamicCodeSupported)
        {
            Console.Error.WriteLine("AotSmoke checks a Native AOT executable: run it through make test-aot-dotnet.");
            return 1;
        }

        Producers();
        await AsyncProducers().ConfigureAwait(false);
        Consumers();
        await AsyncConsumers().ConfigureAwait(false);
        AdminClients();
        ValuesAndText();
        RealClients();
        await AsyncRealClients().ConfigureAwait(false);

        Console.WriteLine(s_failures == 0
            ? "Native AOT checks of Confluent.Kafka: all passed"
            : $"Native AOT checks of Confluent.Kafka: {s_failures} failed");
        return s_failures == 0 ? 0 : 1;
    }

    private static void Producers()
    {
        Check("producer: Send and Get", "t-0@0", () =>
        {
            using var producer = new MockProducer<string, string>(s_serializer, s_serializer);
            RecordMetadata metadata = producer.Send(new ProducerRecord<string, string>("t", "k", "v")).Get();
            return $"{metadata.Topic()}-{metadata.Partition()}@{metadata.Offset()}";
        });
        Check("producer: delivery callback", "offset 0", () =>
        {
            using var producer = new MockProducer<string, string>(s_serializer, s_serializer);
            var callback = new DeliveryCallback();
            producer.Send(new ProducerRecord<string, string>("t", "k", "v"), callback).Get();
            return callback.Seen ?? "not called";
        });
        Check("producer: a Kafka error crosses the core", "same instance: Not authorized to access topics: [t1]", () =>
        {
            using var producer = new MockProducer<string, string>(s_serializer, s_serializer, autoComplete: false);
            KafkaFuture<RecordMetadata> future = producer.Send(new ProducerRecord<string, string>("t", "k", "v"));
            var injected = new TopicAuthorizationException(new HashSet<string> { "t1" });
            producer.ErrorNext(injected);
            return FailureOf(future, injected);
        });
        Check("producer: a Java built-in error crosses the core", "same instance: boom", () =>
        {
            using var producer = new MockProducer<string, string>(s_serializer, s_serializer, autoComplete: false);
            KafkaFuture<RecordMetadata> future = producer.Send(new ProducerRecord<string, string>("t", "k", "v"));
            var injected = new ExampleIllegalState("boom");
            producer.ErrorNext(injected);
            return FailureOf(future, injected);
        });
        Check("producer: transaction", "committed=True, commits=1", () =>
        {
            using var producer = new MockProducer<string, string>(s_serializer, s_serializer);
            producer.InitTransactions();
            producer.BeginTransaction();
            producer.Send(new ProducerRecord<string, string>("t", "k", "v")).Get();
            producer.CommitTransaction();
            return $"committed={producer.TransactionCommitted()}, commits={producer.CommitCount()}";
        });
    }

    private static async Task AsyncProducers()
    {
        await CheckAsync("async producer: Send and Get", "offset 0", async () =>
        {
            await using var producer = new AsyncMockProducer<string, string>(s_serializer, s_serializer);
            AsyncKafkaFuture<RecordMetadata> future =
                await producer.Send(new ProducerRecord<string, string>("t", "k", "v")).ConfigureAwait(false);
            RecordMetadata metadata = await future.Get().ConfigureAwait(false);
            return $"offset {metadata.Offset()}";
        }).ConfigureAwait(false);
    }

    private static void Consumers()
    {
        Check(
            "consumer: AddRecord, Poll and ConsumerRecord.ToString",
            "ConsumerRecord(topic = t, partition = 0, leaderEpoch = null, offset = 0, NoTimestampType = -1, "
                + "deliveryCount = null, serialized key size = -1, serialized value size = -1, "
                + "headers = RecordHeaders(headers = [], isReadOnly = false), key = k, value = v)",
            () =>
            {
                using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
                var partition = new TopicPartition("t", 0);
                consumer.Assign(new[] { partition });
                consumer.Seek(partition, 0);
                consumer.AddRecord("t", 0, 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
                return consumer.Poll(TimeSpan.FromSeconds(1)).First().ToString();
            });
        Check("consumer: Commit, Committed, Position, Pause", "position=5, committed=5, paused=1", () =>
        {
            using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
            var partition = new TopicPartition("t", 0);
            consumer.Assign(new[] { partition });
            consumer.Seek(partition, 5);
            consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(5) });
            consumer.Pause(new[] { partition });
            int paused = consumer.Paused().Count;
            consumer.Resume(new[] { partition });
            long? committed = consumer.Committed(new[] { partition })[partition]?.Offset();
            return $"position={consumer.Position(partition)}, committed={committed}, paused={paused}";
        });
        Check("consumer: commit callback", "offsets=1", () =>
        {
            using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
            var partition = new TopicPartition("t", 0);
            consumer.Assign(new[] { partition });
            var callback = new CommitCallback();
            consumer.CommitAsync(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [partition] = new OffsetAndMetadata(3) }, callback);
            consumer.Poll(TimeSpan.FromMilliseconds(100));
            return callback.Seen ?? "not called";
        });
        Check("consumer: rebalance listener", "assigned t-0", () =>
        {
            using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
            var listener = new AssignmentListener();
            consumer.Subscribe(new[] { "t" }, listener);
            consumer.Rebalance(new[] { new TopicPartition("t", 0) });
            return listener.Assigned;
        });
        Check("consumer: the user's OnPartitionsLost through its interface", "lost t-0", () =>
        {
            using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
            var listener = new LostListener();
            consumer.Subscribe(new[] { "t" }, listener);
            consumer.Rebalance(new[] { new TopicPartition("t", 0) });
            consumer.LosePartitions(new[] { new TopicPartition("t", 0) });
            return listener.Lost;
        });
        Check("consumer: the default-method guard does not fail", "no failure", () =>
        {
            // Native AOT can remove the uncalled method the guard looks for, so either answer is right.
            using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
            try
            {
                consumer.Subscribe(new[] { "t" }, new ForgottenInterfaceListener());
            }
            catch (IllegalArgumentException)
            {
            }

            return "no failure";
        });
        Check(
            "consumer: CloseOptions and AlterConfigOp",
            "AlterConfigOp{opType=SET, configEntry=ConfigEntry(name=a, value=b, source=UNKNOWN, isSensitive=false, "
                + "isReadOnly=false, synonyms=[], type=UNKNOWN, documentation=null)}",
            () =>
            {
                using var consumer = new MockConsumer<string, string>(s_deserializer, s_deserializer);
                consumer.Close(CloseOptions.GroupMembershipOperation(GroupMembershipOperation.LEAVE_GROUP));
                return new AlterConfigOp(new ConfigEntry("a", "b"), OpType.SET).ToString();
            });
    }

    private static async Task AsyncConsumers()
    {
        await CheckAsync("async consumer: Poll", "records=1, value=v", async () =>
        {
            await using var consumer = new AsyncMockConsumer<string, string>(s_deserializer, s_deserializer);
            var partition = new TopicPartition("t", 0);
            await consumer.Assign(new[] { partition }).ConfigureAwait(false);
            await consumer.Seek(partition, 0).ConfigureAwait(false);
            consumer.AddRecord("t", 0, 0, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
            ConsumerRecords<string, string> records =
                await consumer.Poll(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            return $"records={records.Count()}, value={records.First().Value()}";
        }).ConfigureAwait(false);
    }

    private static void AdminClients()
    {
        Check("admin: CreateTopics and ListTopics", "topics: t1", () =>
        {
            using var admin = new MockAdminClient.Builder().Build();
            admin.CreateTopics(new[] { new NewTopic("t1", 1, 1) }).All().Get();
            return "topics: " + string.Join(",", admin.ListTopics().Names().Get().OrderBy(name => name));
        });
    }

    private static void ValuesAndText()
    {
        Check("enums: Parse and ForName", "ONGOING,CREATE_TIME,STABLE,CONSUMER,EMPTY", () =>
            $"{TransactionState.Parse("Ongoing")},{TimestampType.ForName("CreateTime")},{GroupState.Parse("Stable")},"
                + $"{GroupType.Parse("consumer")},{ClassicGroupState.Parse("Empty")}");
        Check("futures: Equals, ThenApply, WhenComplete, AllOf", "equals=True, thenApply=2, whenComplete(1)", () =>
        {
            KafkaFuture<int> future = KafkaFuture.CompletedFuture(1);
            string seen = string.Empty;
            future.WhenComplete((value, exception) => seen = $"whenComplete({value})");
            KafkaFuture<int> next = future.ThenApply(value => value + 1);
            KafkaFuture.AllOf(future, next).Get();
            return $"equals={future.Equals((object)future)}, thenApply={next.Get()}, {seen}";
        });
        Check("serdes: Long, Double, UUID", "42,1.5,00000000-0000-0000-0000-000000000001", () =>
        {
            long? number = Serdes.Long().Deserializer().Deserialize("t", Serdes.Long().Serializer().Serialize("t", 42L)!.Value.Span);
            double? real = Serdes.Double().Deserializer().Deserialize("t", Serdes.Double().Serializer().Serialize("t", 1.5)!.Value.Span);
            Guid id = Guid.Parse("00000000-0000-0000-0000-000000000001");
            Guid? uuid = Serdes.UUID().Deserializer().Deserialize("t", Serdes.UUID().Serializer().Serialize("t", id)!.Value.Span);
            return string.Format(CultureInfo.InvariantCulture, "{0},{1},{2}", number, real, uuid);
        });
        Check("text: a dictionary", "Invalid value {a=1} for configuration k", () =>
            new ConfigException("k", new Dictionary<string, int> { ["a"] = 1 }, null).Message);
        Check("text: an ExpandoObject", "Invalid value {a=1} for configuration k", () =>
        {
            IDictionary<string, object?> expando = new ExpandoObject();
            expando["a"] = 1;
            return new ConfigException("k", expando, null).Message;
        });
        Check("text: a map that is only a generic IReadOnlyDictionary", "Invalid value [[a, 1]] for configuration k", () =>
            new ConfigException("k", new StringIntMap(), null).Message);
        Check("text: a set", "Not authorized to access topics: [t1]", () =>
            new TopicAuthorizationException(new HashSet<string> { "t1" }).Message);
    }

    private static void RealClients()
    {
        Check(
            "config: a value of the wrong type",
            "Invalid value 53 for configuration bootstrap.servers: Expected a comma separated list.",
            () =>
            {
                try
                {
                    using var producer = new KafkaProducer<string, string>(
                        new Dictionary<string, object?> { ["bootstrap.servers"] = 53 }, s_serializer, s_serializer);
                    return "accepted";
                }
                catch (ConfigException e)
                {
                    return e.Message;
                }
            });
        Check(
            "config: a value the core refuses",
            "ConfigException: Invalid value bogus for configuration acks: String must be one of: all, -1, 0, 1",
            () =>
            {
                try
                {
                    using var producer = new KafkaProducer<string, string>(
                        new Dictionary<string, object?> { ["bootstrap.servers"] = Unreachable, ["acks"] = "bogus" },
                        s_serializer,
                        s_serializer);
                    return "accepted";
                }
                catch (KafkaException e)
                {
                    return $"{e.GetType().Name}: {e.Message}";
                }
            });
        Check("real producer: a list and a Type in the configuration", "closed", () =>
        {
            using var producer = new KafkaProducer<string, string>(
                new Dictionary<string, object?>
                {
                    ["bootstrap.servers"] = new List<string> { Unreachable },
                    ["partitioner.class"] = typeof(object),
                    ["client.id"] = "aot-smoke",
                },
                s_serializer,
                s_serializer);
            producer.Close();
            return "closed";
        });
        Check(
            "real producer: a metadata timeout reaches the callback and the future",
            "Get threw TimeoutException; callback got TimeoutException",
            () =>
            {
                using var producer = new KafkaProducer<string, string>(
                    new Dictionary<string, object?> { ["bootstrap.servers"] = Unreachable, ["max.block.ms"] = 500 },
                    s_serializer,
                    s_serializer);
                var callback = new DeliveryCallback();
                try
                {
                    producer.Send(new ProducerRecord<string, string>("t", "k", "v"), callback).Get();
                    return "delivered";
                }
                catch (ExecutionException e)
                {
                    return $"Get threw {e.InnerException?.GetType().Name}; {callback.Seen}";
                }
            });
        Check("real consumer: created and closed", "group aot-smoke", () =>
        {
            using var consumer = new KafkaConsumer<string, string>(
                new Dictionary<string, object?>
                {
                    ["bootstrap.servers"] = Unreachable,
                    ["group.id"] = "aot-smoke",
                    ["group.protocol"] = "consumer",
                },
                s_deserializer,
                s_deserializer);
            return "group " + consumer.GroupMetadata().GroupId();
        });
    }

    private static async Task AsyncRealClients()
    {
        await CheckAsync("real async producer: created and disposed", "disposed", async () =>
        {
            await using (new AsyncKafkaProducer<string, string>(
                new Dictionary<string, object?> { ["bootstrap.servers"] = Unreachable }, s_serializer, s_serializer))
            {
            }

            return "disposed";
        }).ConfigureAwait(false);
    }

    private static string FailureOf(KafkaFuture<RecordMetadata> future, Exception injected)
    {
        try
        {
            future.Get();
            return "no failure";
        }
        catch (ExecutionException e)
        {
            return (ReferenceEquals(e.InnerException, injected) ? "same instance: " : "another instance: ")
                + e.InnerException?.Message;
        }
    }

    private static void Check(string name, string expected, Func<string> actual)
    {
        string value;
        try
        {
            value = actual();
        }
        catch (Exception e)
        {
            value = $"threw {e.GetType().FullName}: {e.Message}";
        }

        Report(name, expected, value);
    }

    private static async Task CheckAsync(string name, string expected, Func<Task<string>> actual)
    {
        string value;
        try
        {
            value = await actual().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            value = $"threw {e.GetType().FullName}: {e.Message}";
        }

        Report(name, expected, value);
    }

    private static void Report(string name, string expected, string actual)
    {
        if (actual == expected)
        {
            Console.WriteLine($"PASS {name}");
            return;
        }

        s_failures++;
        Console.WriteLine($"FAIL {name}{Environment.NewLine}  expected: {expected}{Environment.NewLine}  actual:   {actual}");
    }
}

/// <summary>A delivery callback that keeps what it was given.</summary>
internal sealed class DeliveryCallback : ICallback
{
    internal string? Seen { get; private set; }

    public void OnCompletion(RecordMetadata metadata, Exception? exception) =>
        Seen = exception is null ? $"offset {metadata.Offset()}" : $"callback got {exception.GetType().Name}";
}

/// <summary>An offset-commit callback that keeps what it was given.</summary>
internal sealed class CommitCallback : IOffsetCommitCallback
{
    internal string? Seen { get; private set; }

    public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
        Seen = exception is null ? $"offsets={offsets?.Count}" : $"callback got {exception.GetType().Name}";
}

/// <summary>A user's subclass of a Java built-in exception, which crosses the core as itself.</summary>
internal sealed class ExampleIllegalState(string message) : IllegalStateException(message);

/// <summary>A rebalance listener that keeps the partitions it was assigned.</summary>
internal sealed class AssignmentListener : IConsumerRebalanceListener
{
    internal string Assigned { get; private set; } = "none";

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) =>
        Assigned = "assigned " + string.Join(",", partitions);
}

/// <summary>A rebalance listener that replaces Java's default OnPartitionsLost through its interface.</summary>
internal sealed class LostListener : IConsumerRebalanceListenerWithPartitionsLost
{
    internal string Lost { get; private set; } = "none";

    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions) =>
        Lost = "lost " + string.Join(",", partitions);
}

/// <summary>A rebalance listener with an OnPartitionsLost but not the interface that would make it called.</summary>
internal sealed class ForgottenInterfaceListener : IConsumerRebalanceListener
{
    public void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    public void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions)
    {
    }

    public void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions)
    {
    }
}

/// <summary>A user's map that implements only the generic read-only dictionary interface.</summary>
internal sealed class StringIntMap : IReadOnlyDictionary<string, int>
{
    private readonly Dictionary<string, int> _entries = new() { ["a"] = 1 };

    public int Count => _entries.Count;

    public IEnumerable<string> Keys => _entries.Keys;

    public IEnumerable<int> Values => _entries.Values;

    public int this[string key] => _entries[key];

    public bool ContainsKey(string key) => _entries.ContainsKey(key);

    public bool TryGetValue(string key, out int value) => _entries.TryGetValue(key, out value);

    public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => _entries.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
