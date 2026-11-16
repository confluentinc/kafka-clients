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

// Deletes the ACL bindings that match the given filters with IAdmin.DeleteAcls.
//
// Each filter is seven arguments: resource type, resource name, pattern type, principal, host, operation and
// permission type. Enum values are matched by name, ignoring case, and accept any; the pattern type also
// accepts match. ANY or None as the name, principal or host matches any value. Every binding a filter
// deletes is printed. The broker must have an authorizer configured (authorizer.class.name), otherwise every
// filter fails with a security-disabled error.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DeleteAcls \
//         topic orders literal User:alice ANY any any

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Errors;

/// <summary>The DeleteAcls example.</summary>
internal static class DeleteAcls
{
    public const string Arguments =
        "(<resource_type> <resource_name> <pattern_type> <principal> <host> <operation> <permission_type>)...";

    public static void Run(IAdmin admin, Args args)
    {
        List<AclBindingFilter> filters = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 7, required: true))
        {
            filters.Add(AclArgs.ParseFilter(group));
        }

        DeleteAclsResult result = admin.DeleteAcls(filters);
        foreach (AclBindingFilter filter in filters)
        {
            string filterText = AclArgs.FilterText(filter);
            IReadOnlyList<DeleteAclsResult.FilterResult> matches;
            try
            {
                matches = result.Values()[filter].Get().Values();
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to delete ACLs matching {filterText}", e);
                continue;
            }

            Console.WriteLine($"Filter {filterText}: {matches.Count} matching ACLs");

            // Each match is deleted or fails on its own.
            foreach (DeleteAclsResult.FilterResult match in matches)
            {
                AclBinding? binding = match.Binding();
                ApiException? error = match.Exception();
                if (error is not null)
                {
                    Cli.Fail(
                        binding is not null
                            ? $"Failed to delete ACL {AclArgs.BindingText(binding)}"
                            : $"Failed to delete an ACL matching {filterText}",
                        error);
                }
                else if (binding is not null)
                {
                    Console.WriteLine($"  deleted ACL {AclArgs.BindingText(binding)}");
                }
            }
        }
    }
}
