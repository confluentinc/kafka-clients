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

// Updates finalized feature version levels with IAdmin.UpdateFeatures. Each update names a feature, its new
// max version level and how it may change: upgrade, safe_downgrade (refused when it could lose metadata) or
// unsafe_downgrade. A max level of 0 with a downgrade disables the feature. With --validate-only the
// controller checks the updates without applying them.
//
// The controller applies the updates in one request; each feature still reports its own outcome. The
// DescribeFeatures example prints the current levels and the supported ranges.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 UpdateFeatures --validate-only \
//         metadata.version 27 upgrade

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

/// <summary>The UpdateFeatures example.</summary>
internal static class UpdateFeatures
{
    public const string Arguments =
        "[--validate-only] (<feature> <max_level> <upgrade|safe_downgrade|unsafe_downgrade>)...";

    public static void Run(IAdmin admin, Args args)
    {
        bool validateOnly = args.Flag("validate-only");

        List<string> names = [];
        Dictionary<string, FeatureUpdate> updates = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 3, required: true))
        {
            string name = group[0];
            short maxLevel = Cli.Parse<short>("max version level", group[1]);
            UpgradeType upgradeType = Cli.Choose(
                "upgrade type",
                group[2],
                ("upgrade", UpgradeType.UPGRADE),
                ("safe_downgrade", UpgradeType.SAFE_DOWNGRADE),
                ("unsafe_downgrade", UpgradeType.UNSAFE_DOWNGRADE));

            // The constructor refuses a negative level, and level 0 with an upgrade.
            FeatureUpdate update;
            try
            {
                update = new FeatureUpdate(maxLevel, upgradeType);
            }
            catch (ArgumentException e)
            {
                throw new UsageException(Cli.Describe(e));
            }

            if (!updates.TryAdd(name, update))
            {
                throw new UsageException($"feature {name} given twice");
            }

            names.Add(name);
        }

        // Unlike most calls this one checks its arguments before sending: it throws for a blank feature name.
        UpdateFeaturesResult result = admin.UpdateFeatures(
            updates, new UpdateFeaturesOptions().ValidateOnly(validateOnly));
        string verb = validateOnly ? "validated" : "updated";
        foreach (string name in names)
        {
            try
            {
                result.Values()[name].Get();
                Console.WriteLine($"Feature {name} {verb}: max level {updates[name].MaxVersionLevel()}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to update feature {name}", e);
            }
        }
    }
}
