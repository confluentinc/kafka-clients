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

// Alters the client quotas of one entity with IAdmin.AlterClientQuotas.
//
// The entity is type=name[,type=name], the entity type being user, client-id or ip and the name DEFAULT
// naming the default entity of that type. Each following key=value sets a quota (for example
// producer_byte_rate=1048576) and key=REMOVE removes it. With --validate-only the broker checks the
// alteration without applying it.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 AlterClientQuotas user=alice,client-id=app \
//         producer_byte_rate=1048576 request_percentage=REMOVE

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Quota;

/// <summary>The AlterClientQuotas example.</summary>
internal static class AlterClientQuotas
{
    public const string Arguments =
        "[--validate-only] <entity_type>=<name|DEFAULT>[,<entity_type>=<name|DEFAULT>]... <key>=<value|REMOVE>...";

    public static void Run(IAdmin admin, Args args)
    {
        bool validateOnly = args.Flag("validate-only");
        string[] positional = args.Positional();
        if (positional.Length == 0)
        {
            throw new UsageException("no entity given");
        }

        if (positional.Length == 1)
        {
            throw new UsageException("no quota given");
        }

        Dictionary<string, string?> entries = [];
        foreach (string entry in Cli.CommaList(positional[0]))
        {
            int separator = entry.IndexOf('=');
            if (separator < 0)
            {
                throw new UsageException($"invalid entity \"{entry}\": expected type=name");
            }

            // A null name is the default entity of the type.
            string name = entry[(separator + 1)..];
            entries[entry[..separator]] = name == "DEFAULT" ? null : name;
        }

        ClientQuotaEntity entity = new(entries);
        List<ClientQuotaAlteration.Op> ops = [];
        foreach (string spec in positional[1..])
        {
            int separator = spec.IndexOf('=');
            if (separator < 0)
            {
                throw new UsageException($"invalid quota \"{spec}\": expected key=value");
            }

            // A missing value removes the quota.
            string value = spec[(separator + 1)..];
            ops.Add(new ClientQuotaAlteration.Op(
                spec[..separator],
                value == "REMOVE" ? null : Cli.Parse<double>("quota value", value)));
        }

        List<ClientQuotaAlteration> alterations = [new(entity, ops)];
        AlterClientQuotasResult result = admin.AlterClientQuotas(
            alterations, new AlterClientQuotasOptions().ValidateOnly(validateOnly));
        try
        {
            result.Values()[entity].Get();
            Console.WriteLine($"Client quotas of {EntityText(entity)} {(validateOnly ? "validated" : "altered")}");
        }
        catch (ExecutionException e)
        {
            Cli.Fail($"Failed to alter client quotas of {EntityText(entity)}", e);
        }
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
