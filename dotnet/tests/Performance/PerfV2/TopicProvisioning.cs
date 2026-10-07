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
using System.Linq;
using System.Threading;
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Performance.V2;

/// <summary>
/// (Re)creates a topic before a v2 run — the C# analog of <c>performance_common.recreate_topic</c>: delete
/// <paramref name="topic"/> (ignoring "does not exist"), wait 10s, re-create it (broker-default partitions
/// unless <paramref name="partitions"/> &gt; 0, broker-default replication factor), wait 10s. The two
/// sleeps let the delete/create metadata propagate across the cluster.
/// </summary>
/// <remarks>
/// Uses ckd's own <c>AdminClientBuilder</c> (librdkafka-form config/SASL) — this exe references ONLY ckd's
/// <c>Confluent.Kafka</c> assembly, never ours (see <c>PerfV3/TopicProvisioning.cs</c>'s remarks for why
/// admin provisioning is a per-exe file rather than a <c>PerformanceCommon</c> helper: an
/// AdminClient is unavoidably client-specific, and <c>PerformanceCommon</c> carries no client dependency,
/// M13/P1 D8).
/// </remarks>
internal static class TopicProvisioning
{
    private static readonly TimeSpan s_operationTimeout = TimeSpan.FromSeconds(30);
    private const int PropagationDelayMs = 10000;

    internal static void RecreateTopic(string bootstrapServers, string topic, int partitions)
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = bootstrapServers,
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Librdkafka))
        {
            config[kv.Key] = kv.Value;
        }

        using IAdminClient admin = new AdminClientBuilder(config).Build();

        Console.WriteLine($">>> CREATE_TOPIC: deleting topic '{topic}' (ignored if absent) ...");
        try
        {
            admin.DeleteTopicsAsync(new[] { topic }, new DeleteTopicsOptions { OperationTimeout = s_operationTimeout })
                .GetAwaiter().GetResult();
            Console.WriteLine($">>> deleted '{topic}'");
        }
        catch (DeleteTopicsException e) when (e.Results.Single().Error.Code == ErrorCode.UnknownTopicOrPart)
        {
            Console.WriteLine($">>> '{topic}' did not exist (ok)");
        }

        Console.WriteLine(">>> waiting 10s after delete ...");
        Thread.Sleep(PropagationDelayMs);

        string partitionsLabel = partitions < 0 ? "broker-default" : partitions.ToString(CultureInfo.InvariantCulture);
        Console.WriteLine($">>> CREATE_TOPIC: creating topic '{topic}' (partitions={partitionsLabel}, rf=broker-default) ...");
        var spec = new TopicSpecification
        {
            Name = topic,
            NumPartitions = partitions,
            ReplicationFactor = -1,
        };
        try
        {
            admin.CreateTopicsAsync(new[] { spec }, new CreateTopicsOptions { OperationTimeout = s_operationTimeout })
                .GetAwaiter().GetResult();
            Console.WriteLine($">>> created '{topic}'");
        }
        catch (CreateTopicsException e) when (e.Results.Single().Error.Code == ErrorCode.TopicAlreadyExists)
        {
            Console.WriteLine($">>> '{topic}' already exists (ok)");
        }

        Console.WriteLine(">>> waiting 10s after create ...");
        Thread.Sleep(PropagationDelayMs);
    }
}
