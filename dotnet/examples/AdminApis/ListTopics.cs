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

// Lists the topics in the cluster with IAdmin.ListTopics: each topic's name and id. Internal topics such as
// __consumer_offsets are left out unless --internal is given.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListTopics --internal

using System;
using System.Linq;
using Confluent.Kafka.Admin;

/// <summary>The ListTopics example.</summary>
internal static class ListTopics
{
    public const string Arguments = "[--internal]";

    public static void Run(IAdmin admin, Args args)
    {
        bool listInternal = args.Flag("internal");
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected argument");
        }

        ListTopicsResult result = admin.ListTopics(new ListTopicsOptions().ListInternal(listInternal));

        // One future for the whole call: a failure is thrown by Get(), and reported by Program.cs.
        foreach (TopicListing listing in result.Listings().Get().OrderBy(l => l.Name(), StringComparer.Ordinal))
        {
            Console.WriteLine($"Topic {listing.Name()} (id {listing.TopicId()}){(listing.IsInternal() ? " internal" : "")}");
        }
    }
}
