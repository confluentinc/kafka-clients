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

// Lists the resources that have a configuration with IAdmin.ListConfigResources. Without arguments every
// resource type is listed; otherwise only the given types (topic, broker, broker_logger, group or
// client_metrics).
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 ListConfigResources topic group

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Config;

/// <summary>The ListConfigResources example.</summary>
internal static class ListConfigResources
{
    public const string Arguments = "[<type>...]\ntypes: topic, broker, broker_logger, group, client_metrics";

    public static void Run(IAdmin admin, Args args)
    {
        HashSet<ConfigResourceType> types = [.. args.Positional().Select(ConfigArgs.ParseResourceType)];

        // No types, as when the argument is left out, lists every type.
        ListConfigResourcesResult result = admin.ListConfigResources(types);
        IEnumerable<ConfigResource> resources = result.All().Get()
            .OrderBy(ConfigArgs.TypeName, StringComparer.Ordinal)
            .ThenBy(resource => resource.Name(), StringComparer.Ordinal);
        foreach (ConfigResource resource in resources)
        {
            Console.WriteLine(ConfigArgs.Label(resource));
        }
    }
}
