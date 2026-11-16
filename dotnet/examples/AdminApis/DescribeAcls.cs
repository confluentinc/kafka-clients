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

// Describes the ACL bindings that match one filter with IAdmin.DescribeAcls.
//
// The filter is seven arguments: resource type, resource name, pattern type, principal, host, operation and
// permission type. Enum values are matched by name, ignoring case, and accept any; the pattern type also
// accepts match, which matches every binding that applies to the named resource (literal, wildcard and
// prefixed). ANY or None as the name, principal or host matches any value. The broker must have an
// authorizer configured (authorizer.class.name), otherwise the call fails with a security-disabled error.
//
//     dotnet run -c Release --project AdminApis -- localhost:9092 DescribeAcls topic orders match ANY ANY any any

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Acl;

/// <summary>The DescribeAcls example.</summary>
internal static class DescribeAcls
{
    public const string Arguments =
        "<resource_type> <resource_name> <pattern_type> <principal> <host> <operation> <permission_type>";

    public static void Run(IAdmin admin, Args args)
    {
        string[] positional = args.Positional();
        if (positional.Length != 7)
        {
            throw new UsageException("expected exactly one filter of 7 arguments");
        }

        AclBindingFilter filter = AclArgs.ParseFilter(positional);

        IReadOnlyCollection<AclBinding> bindings = admin.DescribeAcls(filter).Values().Get();
        foreach (string text in bindings.Select(AclArgs.BindingText).Order(StringComparer.Ordinal))
        {
            Console.WriteLine($"ACL {text}");
        }

        Console.WriteLine($"{bindings.Count} ACLs match");
    }
}
