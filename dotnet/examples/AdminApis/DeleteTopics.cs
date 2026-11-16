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

// Deletes topics by name with IAdmin.DeleteTopics. A topic's future completes once the controller has
// deleted it; brokers may still report it for a short while after.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DeleteTopics orders payments

using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The DeleteTopics example.</summary>
internal static class DeleteTopics
{
    public const string Arguments = "<topic>...";

    public static void Run(IAdmin admin, Args args)
    {
        string[] topics = args.Positional();
        if (topics.Length == 0)
        {
            throw new UsageException("no topic given");
        }

        DeleteTopicsResult result = admin.DeleteTopics(topics);

        // A by-name call keys its futures by topic name.
        if (result.TopicNameValues() is not { } futures)
        {
            return;
        }

        foreach (string topic in topics)
        {
            try
            {
                futures[topic].Get();
                Console.WriteLine($"Topic {topic} deleted");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to delete topic {topic}", e);
            }
        }
    }
}
