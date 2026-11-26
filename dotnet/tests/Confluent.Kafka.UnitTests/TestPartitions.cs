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

using Confluent.Kafka.Common;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The partition metadata the tests register on a mock consumer: partitions <c>0</c> to <c>partitionCount - 1</c>,
/// each led by one node, which is its sole replica and sole in-sync replica, with no offline replicas.
/// </summary>
internal static class TestPartitions
{
    internal static IReadOnlyList<PartitionInfo> Led(
        string topic,
        int partitionCount,
        int leaderId,
        string leaderHost,
        int leaderPort)
    {
        Node leader = new Node(leaderId, leaderHost, leaderPort);
        PartitionInfo[] partitions = new PartitionInfo[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            partitions[i] = new PartitionInfo(topic, i, leader, new[] { leader }, new[] { leader });
        }

        return partitions;
    }
}
