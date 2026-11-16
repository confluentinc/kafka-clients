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

// Describes client quotas with IAdmin.DescribeClientQuotas.
//
// The filter is a sequence of <entity_type> <name> pairs, the entity type being user, client-id or ip; the
// name DEFAULT matches the default entity of that type and ANY every entity of it, the default one
// included. Without pairs every configured entity is described. With --strict only entities made of exactly the given types match;
// otherwise entities with further types do too.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeClientQuotas user alice client-id ANY

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Quota;

/// <summary>The DescribeClientQuotas example.</summary>
internal static class DescribeClientQuotas
{
    public const string Arguments = "[--strict] [<entity_type> <name|DEFAULT|ANY>]...";

    public static void Run(IAdmin admin, Args args)
    {
        bool strict = args.Flag("strict");
        List<ClientQuotaFilterComponent> components = [];
        foreach (string[] pair in Cli.Groups(args.Positional(), 2, required: false))
        {
            components.Add(pair[1] switch
            {
                "DEFAULT" => ClientQuotaFilterComponent.OfDefaultEntity(pair[0]),
                "ANY" => ClientQuotaFilterComponent.OfEntityType(pair[0]),
                _ => ClientQuotaFilterComponent.OfEntity(pair[0], pair[1]),
            });
        }

        ClientQuotaFilter filter;
        if (strict)
        {
            filter = ClientQuotaFilter.ContainsOnly(components);
        }
        else if (components.Count == 0)
        {
            filter = ClientQuotaFilter.All();
        }
        else
        {
            filter = ClientQuotaFilter.Contains(components);
        }

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            admin.DescribeClientQuotas(filter).Entities().Get();
        List<string> lines = [];
        foreach ((ClientQuotaEntity entity, IReadOnlyDictionary<string, double> quotas) in entities)
        {
            IEnumerable<string> values = quotas
                .Select(quota => $"{quota.Key}={quota.Value.ToString(CultureInfo.InvariantCulture)}")
                .Order(StringComparer.Ordinal);
            lines.Add($"{EntityText(entity)}: {string.Join(", ", values)}");
        }

        lines.Sort(StringComparer.Ordinal);
        foreach (string line in lines)
        {
            Console.WriteLine(line);
        }

        Console.WriteLine($"{lines.Count} entities match");
    }

    /// <summary>
    /// The entity as <c>type=name[,type=name]</c>, sorted by type; <c>&lt;default&gt;</c> names the default
    /// entity of a type.
    /// </summary>
    private static string EntityText(ClientQuotaEntity entity)
    {
        return string.Join(
            ",",
            entity.Entries().Select(entry => $"{entry.Key}={entry.Value ?? "<default>"}").Order(StringComparer.Ordinal));
    }
}
