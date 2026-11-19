// Copyright 2025 Confluent Inc.
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

using System;
using System.Collections.Generic;
using System.Reflection;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Decodes the compiler's nullable-annotation metadata for a member or parameter, which the CLR type erases, so a shape
/// test can assert <c>T</c> vs <c>T?</c>.
/// </summary>
/// <remarks>
/// Decoded by hand, since <c>NullabilityInfoContext</c> is not on net462; a member without its own attribute takes the
/// nearest enclosing context, and with none in scope reads <see cref="Oblivious"/>, so an assertion on it fails instead
/// of passing.
/// </remarks>
internal static class NullableAnnotation
{
    internal const byte Oblivious = 0;

    internal const byte NotAnnotated = 1;

    internal const byte Annotated = 2;

    private const string NullableAttributeName = "System.Runtime.CompilerServices.NullableAttribute";

    private const string NullableContextAttributeName =
        "System.Runtime.CompilerServices.NullableContextAttribute";

    internal static byte Flag(MemberInfo member) => member switch
    {
        MethodInfo method => Flag(method.ReturnParameter),
        Type => throw new ArgumentException(
            "Pass a member, not a type: a type's NullableAttribute describes its base type, "
                + "not a member's own annotation.",
            nameof(member)),
        _ => Own(member.GetCustomAttributesData()) ?? Context(member.DeclaringType),
    };

    internal static byte Flag(ParameterInfo parameter) => Flag(parameter, 0);

    internal static byte Flag(ParameterInfo parameter, int position) =>
        Own(parameter.GetCustomAttributesData(), position) ?? Context(parameter.Member);

    private static byte Context(MemberInfo? scope)
    {
        for (; scope is not null; scope = scope.DeclaringType)
        {
            byte? context = Read(scope.GetCustomAttributesData(), NullableContextAttributeName);
            if (context is not null)
            {
                return context.Value;
            }
        }

        return Oblivious;
    }

    private static byte? Own(IList<CustomAttributeData> attributes, int position = 0) =>
        Read(attributes, NullableAttributeName, position);

    private static byte? Read(IList<CustomAttributeData> attributes, string attributeFullName, int position = 0)
    {
        foreach (CustomAttributeData attribute in attributes)
        {
            if (attribute.AttributeType.FullName != attributeFullName)
            {
                continue;
            }

            object? argument = attribute.ConstructorArguments[0].Value;
            if (argument is byte flag)
            {
                return flag;
            }

            if (argument is IReadOnlyList<CustomAttributeTypedArgument> flags && flags.Count > position)
            {
                return (byte)flags[position].Value!;
            }
        }

        return null;
    }
}
