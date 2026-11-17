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

// Changes the configuration of resources with IAdmin.IncrementalAlterConfigs. Each resource is a type
// (topic, broker, broker_logger, group or client_metrics), a name and a ;-separated list of operations:
// key=SET:value sets a value, key=APPEND:value and key=SUBTRACT:value add items to or remove items from a
// list value, and key=DELETE reverts the key to its default. Keys not named keep their current value.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 IncrementalAlterConfigs \
//         topic orders 'retention.ms=SET:86400000;cleanup.policy=APPEND:compact'

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Config;

/// <summary>The IncrementalAlterConfigs example.</summary>
internal static class IncrementalAlterConfigs
{
    public const string Arguments =
        "<type> <name> <key=OP:value;key=DELETE;...>...\n" +
        "types: topic, broker, broker_logger, group, client_metrics; OP: SET, APPEND, SUBTRACT";

    public static void Run(IAdmin admin, Args args)
    {
        List<ConfigResource> resources = [];
        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 3, required: true))
        {
            ConfigResource resource = new(ConfigArgs.ParseResourceType(group[0]), group[1]);
            List<AlterConfigOp> operations = ParseOperations(group[2]);
            if (configs.TryGetValue(resource, out IReadOnlyCollection<AlterConfigOp>? earlier))
            {
                // A resource given more than once gets the operations of every mention.
                configs[resource] = [.. earlier, .. operations];
            }
            else
            {
                resources.Add(resource);
                configs[resource] = operations;
            }
        }

        AlterConfigsResult result = admin.IncrementalAlterConfigs(configs);
        foreach (ConfigResource resource in resources)
        {
            try
            {
                result.Values()[resource].Get();
                Console.WriteLine($"Altered configs of {ConfigArgs.Label(resource)}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to alter configs of {ConfigArgs.Label(resource)}", e);
            }
        }
    }

    private static List<AlterConfigOp> ParseOperations(string text)
    {
        List<AlterConfigOp> operations = [];
        foreach (string spec in text.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            int equals = spec.IndexOf('=');
            if (equals < 0)
            {
                throw new UsageException($"invalid operation \"{spec}\": expected key=OP:value or key=DELETE");
            }

            string key = spec[..equals];
            string operation = spec[(equals + 1)..];
            OpType opType;
            string? value;
            if (operation.Equals("delete", StringComparison.OrdinalIgnoreCase))
            {
                opType = OpType.DELETE;
                value = null;
            }
            else
            {
                int colon = operation.IndexOf(':');
                if (colon < 0)
                {
                    throw new UsageException($"invalid operation \"{spec}\": expected key=OP:value or key=DELETE");
                }

                opType = Cli.Choose(
                    "operation",
                    operation[..colon],
                    ("set", OpType.SET),
                    ("append", OpType.APPEND),
                    ("subtract", OpType.SUBTRACT));
                value = operation[(colon + 1)..];
            }

            operations.Add(new AlterConfigOp(new ConfigEntry(key, value), opType));
        }

        if (operations.Count == 0)
        {
            throw new UsageException($"no operation in \"{text}\"");
        }

        return operations;
    }
}
