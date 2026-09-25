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
using System.Globalization;
using System.Threading;
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Performance.V3;

/// <summary>
/// (Re)creates a topic before a v3 run — the C# analog of <c>performance_common.recreate_topic</c>: delete
/// <paramref name="topic"/> (ignoring "does not exist"), wait 10s, re-create it (broker-default partitions
/// unless <paramref name="partitions"/> &gt; 0, broker-default replication factor), wait 10s. The two
/// sleeps let the delete/create metadata propagate across the cluster.
/// </summary>
/// <remarks>
/// Uses our OWN <see cref="KafkaAdminClient"/> (Java-form config/SASL), not Python's librdkafka-form admin
/// — this exe references only our binding's <c>Confluent.Kafka</c> assembly. This is a per-exe file (like
/// <c>V3Config</c>/<c>V2Config</c>), not a <c>PerformanceCommon</c> helper: <c>PerformanceCommon</c> ships
/// with NO client dependency (M13/P1 D8) so that <c>PerfV2</c> and <c>PerfV3</c> — both literally the
/// <c>Confluent.Kafka</c> assembly name, ours vs ckd's — never collide; an admin client is unavoidably
/// client-specific, so each exe gets its own <c>TopicProvisioning</c>.
/// </remarks>
internal static class TopicProvisioning
{
    private const int OperationTimeoutMs = 30000;
    private const int PropagationDelayMs = 10000;

    /// <summary>UNKNOWN_TOPIC_OR_PARTITION (Kafka protocol error code 3) — "did not exist" is OK on delete.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    /// <summary>TOPIC_ALREADY_EXISTS (Kafka protocol error code 36) — "already exists" is OK on create.</summary>
    private const int TopicAlreadyExistsCode = 36;

    internal static void RecreateTopic(string bootstrapServers, string topic, int partitions)
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = bootstrapServers,
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
        {
            config[kv.Key] = kv.Value;
        }

        using var admin = new KafkaAdminClient(config);

        Console.WriteLine($">>> CREATE_TOPIC: deleting topic '{topic}' (ignored if absent) ...");
        DeleteTopicsResult deleteResult = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { topic }),
            new DeleteTopicsOptions { TimeoutMs = OperationTimeoutMs });
        try
        {
            deleteResult.TopicNameValues![topic].GetAwaiter().GetResult();
            Console.WriteLine($">>> deleted '{topic}'");
        }
        catch (KafkaException e) when (e.Code == UnknownTopicOrPartitionCode)
        {
            Console.WriteLine($">>> '{topic}' did not exist (ok)");
        }

        Console.WriteLine(">>> waiting 10s after delete ...");
        Thread.Sleep(PropagationDelayMs);

        string partitionsLabel = partitions < 0 ? "broker-default" : partitions.ToString(CultureInfo.InvariantCulture);
        Console.WriteLine($">>> CREATE_TOPIC: creating topic '{topic}' (partitions={partitionsLabel}, rf=broker-default) ...");
        var newTopic = new NewTopic(topic, partitions < 0 ? (int?)null : partitions, (short?)null);
        CreateTopicsResult createResult = admin.CreateTopics(
            new[] { newTopic },
            new CreateTopicsOptions { TimeoutMs = OperationTimeoutMs });
        try
        {
            createResult.Values[topic].GetAwaiter().GetResult();
            Console.WriteLine($">>> created '{topic}'");
        }
        catch (KafkaException e) when (e.Code == TopicAlreadyExistsCode)
        {
            Console.WriteLine($">>> '{topic}' already exists (ok)");
        }

        Console.WriteLine(">>> waiting 10s after create ...");
        Thread.Sleep(PropagationDelayMs);
    }
}
