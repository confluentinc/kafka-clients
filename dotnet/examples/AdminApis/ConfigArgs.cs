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

// What the config examples (DescribeConfigs, IncrementalAlterConfigs, ListConfigResources) share: reading a
// config resource type from the command line and printing a config resource.

using Confluent.Kafka.Common.Config;

/// <summary>The command-line form of a config resource: its type and how it is printed.</summary>
internal static class ConfigArgs
{
    /// <summary>Parses a resource type: topic, broker, broker_logger, group or client_metrics.</summary>
    public static ConfigResourceType ParseResourceType(string text)
    {
        return Cli.Choose(
            "resource type",
            text,
            ("topic", ConfigResourceType.TOPIC),
            ("broker", ConfigResourceType.BROKER),
            ("broker_logger", ConfigResourceType.BROKER_LOGGER),
            ("group", ConfigResourceType.GROUP),
            ("client_metrics", ConfigResourceType.CLIENT_METRICS));
    }

    /// <summary>The resource's type as it is written on the command line.</summary>
    public static string TypeName(ConfigResource resource)
    {
        return resource.Type().ToString().ToLowerInvariant();
    }

    /// <summary>The resource as its type and name: <c>topic orders</c>.</summary>
    public static string Label(ConfigResource resource)
    {
        return $"{TypeName(resource)} {resource.Name()}";
    }
}
