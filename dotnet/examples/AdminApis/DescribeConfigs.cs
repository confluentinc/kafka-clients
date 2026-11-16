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

// Describes the configuration of resources with IAdmin.DescribeConfigs. Each resource is a type (topic,
// broker, broker_logger, group or client_metrics) followed by its name; a broker is named by its id. Every
// entry shows its value, where the value comes from and whether it is the default, sensitive or read-only.
// With --synonyms the broker also returns the other configurations the value could come from, in order of
// precedence, and with --documentation the description of each entry.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeConfigs --synonyms \
//         topic orders broker 0

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Config;

/// <summary>The DescribeConfigs example.</summary>
internal static class DescribeConfigs
{
    public const string Arguments =
        "[--synonyms] [--documentation] <type> <name>...\n" +
        "types: topic, broker, broker_logger, group, client_metrics";

    public static void Run(IAdmin admin, Args args)
    {
        bool includeSynonyms = args.Flag("synonyms");
        bool includeDocumentation = args.Flag("documentation");
        List<ConfigResource> resources = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 2, required: true))
        {
            ConfigResource resource = new(ConfigArgs.ParseResourceType(group[0]), group[1]);
            if (!resources.Contains(resource))
            {
                resources.Add(resource);
            }
        }

        DescribeConfigsResult result = admin.DescribeConfigs(
            resources,
            new DescribeConfigsOptions()
                .IncludeSynonyms(includeSynonyms)
                .IncludeDocumentation(includeDocumentation));
        foreach (ConfigResource resource in resources)
        {
            try
            {
                Config config = result.Values()[resource].Get();
                Console.WriteLine($"Configs of {ConfigArgs.Label(resource)}:");
                foreach (ConfigEntry entry in config.Entries().OrderBy(entry => entry.Name(), StringComparer.Ordinal))
                {
                    Console.WriteLine($"  {EntryLine(entry)}");
                    foreach (ConfigEntry.ConfigSynonym synonym in entry.Synonyms())
                    {
                        string value = entry.IsSensitive() ? "******" : synonym.Value() ?? "null";
                        Console.WriteLine($"    synonym {synonym.Name()} = {value} [{synonym.Source()}]");
                    }

                    // Some brokers send an empty description even when none was asked for.
                    if (entry.Documentation() is { Length: > 0 } documentation)
                    {
                        Console.WriteLine($"    documentation: {documentation}");
                    }
                }
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to describe configs of {ConfigArgs.Label(resource)}", e);
            }
        }
    }

    private static string EntryLine(ConfigEntry entry)
    {
        // A sensitive entry's value is never returned.
        string value = entry.IsSensitive() ? "******" : entry.Value() ?? "null";
        List<string> flags = [];
        if (entry.IsDefault())
        {
            flags.Add("default");
        }

        if (entry.IsSensitive())
        {
            flags.Add("sensitive");
        }

        if (entry.IsReadOnly())
        {
            flags.Add("read-only");
        }

        string flagsText = flags.Count == 0 ? "" : $" ({string.Join(", ", flags)})";
        return $"{entry.Name()} = {value} [{entry.Source()}]{flagsText}";
    }
}
