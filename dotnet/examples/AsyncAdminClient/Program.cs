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

// Example async Kafka admin client, in an await using block.
// Creates a topic, waits for the result of each topic, then lists the topics.
// Leaving the await using block closes the client.

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;

if (args.Length != 2)
{
    Console.Error.WriteLine("Usage: AsyncAdminClient <bootstrap-brokers> <topic>");
    return 1;
}

string broker = args[0];
string topic = args[1];

// Admin configuration.
// The keys are the Java client's: https://kafka.apache.org/documentation/#adminclientconfigs
Dictionary<string, object?> conf = new() { ["bootstrap.servers"] = broker };

await using (IAsyncAdmin admin = AsyncAdmin.Create(conf))
{
    // Admin calls return their result at once; its AsyncKafkaFutures are awaited through Get().
    AsyncCreateTopicsResult result = admin.CreateTopics([new NewTopic(topic, 3, (short)1)]);

    // One AsyncKafkaFuture per topic; Get() throws ExecutionException for a failed one, the error being its
    // InnerException.
    foreach ((string name, AsyncKafkaFuture future) in result.Values())
    {
        try
        {
            await future.Get();
            Console.WriteLine($"Topic {name} created");
        }
        catch (ExecutionException e) when (e.InnerException is TopicExistsException)
        {
            Console.WriteLine($"Topic {name} already exists");
        }
    }

    IReadOnlyCollection<string> names = await admin.ListTopics().Names().Get();
    Console.WriteLine($"Topics: [{string.Join(", ", names.OrderBy(n => n, StringComparer.Ordinal))}]");
}

return 0;
