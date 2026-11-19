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

// Creates ACL bindings with IAdmin.CreateAcls.
//
// Each binding is seven arguments: the resource type, resource name and pattern type of the resource pattern,
// then the principal, host, operation and permission type of the access control entry. Enum values are
// matched by name, ignoring case. The broker must have an authorizer configured (authorizer.class.name),
// otherwise every binding fails with a security-disabled error.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 CreateAcls \
//         topic orders literal User:alice '*' read allow

using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Acl;

/// <summary>The CreateAcls example.</summary>
internal static class CreateAcls
{
    public const string Arguments =
        "(<resource_type> <resource_name> <pattern_type> <principal> <host> <operation> <permission_type>)...";

    public static void Run(IAdmin admin, Args args)
    {
        List<AclBinding> bindings = [];
        foreach (string[] group in Cli.Groups(args.Positional(), 7, required: true))
        {
            bindings.Add(AclArgs.ParseBinding(group));
        }

        CreateAclsResult result = admin.CreateAcls(bindings);
        foreach (AclBinding binding in bindings)
        {
            try
            {
                result.Values()[binding].Get();
                Console.WriteLine($"Created ACL {AclArgs.BindingText(binding)}");
            }
            catch (ExecutionException e)
            {
                Cli.Fail($"Failed to create ACL {AclArgs.BindingText(binding)}", e);
            }
        }
    }
}
