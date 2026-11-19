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

// Describes the cluster's features with IAdmin.DescribeFeatures: the finalized version levels, which the
// cluster currently runs, and the version ranges the answering node supports. With --node-id that node
// answers, otherwise any broker does.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeFeatures

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;

/// <summary>The DescribeFeatures example.</summary>
internal static class DescribeFeatures
{
    public const string Arguments = "[--node-id=<id>]";

    public static void Run(IAdmin admin, Args args)
    {
        int? nodeId = args.Value("node-id") is { } text ? Cli.Parse<int>("node id", text) : null;
        if (args.Positional().Length > 0)
        {
            throw new UsageException("unexpected positional argument");
        }

        DescribeFeaturesOptions options = new();
        if (nodeId is { } id)
        {
            options.NodeId(id);
        }

        // The future's value is null when the client delivers no metadata.
        if (admin.DescribeFeatures(options).FeatureMetadata().Get() is not { } metadata)
        {
            Console.WriteLine("Feature metadata: none");
            return;
        }

        Console.WriteLine($"Finalized features epoch: {Cli.OrNone(metadata.FinalizedFeaturesEpoch())}");
        foreach ((string name, FinalizedVersionRange range) in ByName(metadata.FinalizedFeatures()))
        {
            Console.WriteLine(
                $"Finalized feature {name}: min level {range.MinVersionLevel()}, max level {range.MaxVersionLevel()}");
        }

        foreach ((string name, SupportedVersionRange range) in ByName(metadata.SupportedFeatures()))
        {
            Console.WriteLine(
                $"Supported feature {name}: min version {range.MinVersion()}, max version {range.MaxVersion()}");
        }
    }

    private static IEnumerable<KeyValuePair<string, T>> ByName<T>(IReadOnlyDictionary<string, T> features)
    {
        return features.OrderBy(feature => feature.Key, StringComparer.Ordinal);
    }
}
