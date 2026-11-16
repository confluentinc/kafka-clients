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

// What the ACL examples (CreateAcls, DescribeAcls, DeleteAcls) share: reading an ACL binding or an ACL binding
// filter from seven arguments (resource type, resource name, pattern type, principal, host, operation,
// permission type) and printing one in that same form.

using System;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;

/// <summary>The command-line form of an ACL binding and of an ACL binding filter.</summary>
internal static class AclArgs
{
    /// <summary>Parses the seven arguments of a binding. Enum values are matched by name, ignoring case.</summary>
    public static AclBinding ParseBinding(string[] group)
    {
        ResourceType resourceType = ParseResourceType(group[0]);
        PatternType patternType = ParsePatternType(group[2]);
        AclOperation operation = ParseOperation(group[5]);
        AclPermissionType permissionType = ParsePermissionType(group[6]);

        // A binding names one specific resource and entry: any and match are only valid in filters, and the
        // constructors refuse them.
        try
        {
            ResourcePattern pattern = new(resourceType, group[1], patternType);
            AccessControlEntry entry = new(group[3], group[4], operation, permissionType);
            return new AclBinding(pattern, entry);
        }
        catch (ArgumentException e)
        {
            throw new UsageException($"invalid ACL binding: {Cli.Describe(e)}");
        }
    }

    /// <summary>
    /// Parses the seven arguments of a filter. Enum values are matched by name, ignoring case; ANY or None as
    /// the name, principal or host matches any value.
    /// </summary>
    public static AclBindingFilter ParseFilter(string[] group)
    {
        ResourceType resourceType = ParseResourceType(group[0]);
        PatternType patternType = ParsePatternType(group[2]);
        AclOperation operation = ParseOperation(group[5]);
        AclPermissionType permissionType = ParsePermissionType(group[6]);
        return new AclBindingFilter(
            new ResourcePatternFilter(resourceType, AnyOr(group[1]), patternType),
            new AccessControlEntryFilter(AnyOr(group[3]), AnyOr(group[4]), operation, permissionType));
    }

    /// <summary>The binding in the command-line format.</summary>
    public static string BindingText(AclBinding binding)
    {
        ResourcePattern pattern = binding.Pattern();
        AccessControlEntry entry = binding.Entry();
        return $"{pattern.ResourceType()} {pattern.Name()} {pattern.PatternType()} " +
            $"{entry.Principal()} {entry.Host()} {entry.Operation()} {entry.PermissionType()}";
    }

    /// <summary>The filter in the command-line format, ANY standing for a missing field.</summary>
    public static string FilterText(AclBindingFilter filter)
    {
        ResourcePatternFilter pattern = filter.PatternFilter();
        AccessControlEntryFilter entry = filter.EntryFilter();
        return $"{pattern.ResourceType()} {pattern.Name() ?? "ANY"} {pattern.PatternType()} " +
            $"{entry.Principal() ?? "ANY"} {entry.Host() ?? "ANY"} {entry.Operation()} {entry.PermissionType()}";
    }

    // Each FromString returns UNKNOWN for a name it does not know.
    private static ResourceType ParseResourceType(string text)
    {
        ResourceType parsed = ResourceTypeMethods.FromString(text);
        return parsed != ResourceType.UNKNOWN
            ? parsed
            : throw new UsageException($"invalid resource type: \"{text}\"");
    }

    private static PatternType ParsePatternType(string text)
    {
        // Unlike the other three, this lookup is case-sensitive.
        PatternType parsed = PatternTypeMethods.FromString(text.ToUpperInvariant());
        return parsed != PatternType.UNKNOWN
            ? parsed
            : throw new UsageException($"invalid pattern type: \"{text}\"");
    }

    private static AclOperation ParseOperation(string text)
    {
        AclOperation parsed = AclOperationMethods.FromString(text);
        return parsed != AclOperation.UNKNOWN
            ? parsed
            : throw new UsageException($"invalid operation: \"{text}\"");
    }

    private static AclPermissionType ParsePermissionType(string text)
    {
        AclPermissionType parsed = AclPermissionTypeMethods.FromString(text);
        return parsed != AclPermissionType.UNKNOWN
            ? parsed
            : throw new UsageException($"invalid permission type: \"{text}\"");
    }

    /// <summary>ANY or None matches any name, principal or host.</summary>
    private static string? AnyOr(string text)
    {
        return text is "ANY" or "None" ? null : text;
    }
}
