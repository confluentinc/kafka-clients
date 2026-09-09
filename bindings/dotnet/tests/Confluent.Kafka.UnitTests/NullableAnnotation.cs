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
/// Decodes the C# compiler's nullable-annotation metadata for a member or parameter, so a
/// shape test can assert <c>T</c> vs <c>T?</c> — a distinction that is <b>erased from the
/// CLR type</b>, so <see cref="PropertyInfo.PropertyType"/> /
/// <see cref="ParameterInfo.ParameterType"/> cannot see it at all.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="System.Reflection.NullabilityInfoContext"/> would do this for us, but it
/// post-dates the net462 leg of the TFM matrix, so the flags are decoded by hand.
/// </para>
/// <para>
/// ⚠ <b>The enclosing-context walk is the load-bearing part, and omitting it leaves an
/// assertion's falsifiability up to a compiler heuristic the test does not control.</b> The
/// compiler does not annotate every member: it emits <c>NullableContextAttribute</c>
/// <em>per declaration</em>, picking whichever value lets it omit the most per-member
/// <c>NullableAttribute</c>s, and members agreeing with that context carry <b>no attribute
/// of their own</b>. So when nullable positions come to dominate a type the compiler picks
/// context <c>2</c>, and it is then the <em>nullable</em> members that are bare. A decoder
/// reading only a member's own attribute and defaulting to <see cref="NotAnnotated"/>
/// reports those as non-nullable, so the <c>Assert.Equal(1, …)</c> written against it passes
/// however the member is widened.
/// </para>
/// <para>
/// M15/P2a and P2b shipped exactly that decoder (Critic 68, finding 1). Measured against
/// their <b>10</b> non-nullable assertions: in the baseline build <b>none</b> of the 10
/// asserted members carries a <c>NullableAttribute</c>, so all 10 resolved through the
/// fallback without reading any metadata. Widening each one in turn then showed the split
/// the heuristic produces — <b>9</b> acquire an own <c>NullableAttribute([2,…])</c> and so
/// would have been caught anyway, while <b>1</b> (<c>TopicListing.Name</c>) does not,
/// because its declaring type flips to context <c>2</c> instead: that widening built clean
/// and left the suite fully green. So the member-only form was <em>accidentally</em>
/// sensitive 9 times and unfalsifiable once — and which bucket a member lands in is decided
/// by attribute-minimisation, not by the test. With the context walk all 10 fail when
/// widened.
/// </para>
/// <para>
/// Hence the fallback here is <see cref="Oblivious"/>, matching the compiler's own default
/// for "no context in scope", rather than <see cref="NotAnnotated"/>. An assertion against
/// a member carrying no nullability metadata at all then <b>fails loudly</b> instead of
/// quietly agreeing with whatever it was asked.
/// </para>
/// <para>
/// This is the single decoder for the whole test project — the three copies that preceded
/// it are what let one unsound fallback spread across two files.
/// </para>
/// </remarks>
internal static class NullableAnnotation
{
    /// <summary>No nullability metadata in scope — the annotation is unknown.</summary>
    internal const byte Oblivious = 0;

    /// <summary>Not annotated nullable, i.e. <c>T</c> under <c>#nullable enable</c>.</summary>
    internal const byte NotAnnotated = 1;

    /// <summary>Annotated nullable, i.e. <c>T?</c>.</summary>
    internal const byte Annotated = 2;

    private const string NullableAttributeName = "System.Runtime.CompilerServices.NullableAttribute";

    private const string NullableContextAttributeName =
        "System.Runtime.CompilerServices.NullableContextAttribute";

    /// <summary>
    /// The flag for a property's or method's own type, i.e. its return type for a method.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Both special cases below are handled here rather than documented, because a
    /// <see cref="MethodInfo"/> and a <see cref="System.Type"/> each bind to this overload
    /// silently.</b> A wrong-overload warning nobody sees is exactly the unfalsifiable-assertion
    /// class the rest of this file exists to close, so the guards are code.
    /// </para>
    /// <para>
    /// A <see cref="MethodInfo"/> is routed to its <see cref="MethodInfo.ReturnParameter"/>,
    /// which is where the compiler puts a return's flag. Reading the method's own attributes
    /// instead would find none (so <see cref="Own"/> is always null) and fall through to
    /// <see cref="MemberInfo.DeclaringType"/>, <b>skipping the method's own
    /// <c>NullableContextAttribute</c> scope</b> — measured on
    /// <c>ListTopicsResult.NamesToListings()</c>: widening its return to nullable moves
    /// <c>Flag(ReturnParameter)</c> to <see cref="Annotated"/> while the unrouted
    /// <c>MemberInfo</c> form stayed at <see cref="NotAnnotated"/>, i.e. green however the
    /// return is widened. (The sibling <see cref="Flag(ParameterInfo)"/> has always been
    /// correct here — it starts its walk at <see cref="ParameterInfo.Member"/>, not at the
    /// declaring type.)
    /// </para>
    /// <para>
    /// A <see cref="System.Type"/> is <b>rejected</b>: a type's own <c>NullableAttribute</c>
    /// describes its base type, not a member, and would be misread as the member flag. There
    /// is no correct answer to route it to, so the only way it cannot be asked silently is
    /// for asking to throw.
    /// </para>
    /// </remarks>
    internal static byte Flag(MemberInfo member) => member switch
    {
        MethodInfo method => Flag(method.ReturnParameter),
        Type => throw new ArgumentException(
            "Pass a member, not a type: a type's NullableAttribute describes its base type, "
                + "not a member's own annotation.",
            nameof(member)),
        _ => Own(member.GetCustomAttributesData()) ?? Context(member.DeclaringType),
    };

    /// <summary>The flag for a parameter, including a <c>ReturnParameter</c>.</summary>
    internal static byte Flag(ParameterInfo parameter) =>
        Own(parameter.GetCustomAttributesData()) ?? Context(parameter.Member);

    /// <summary>
    /// Walks outward — declaring method, then each enclosing type — for the nearest
    /// <c>NullableContextAttribute</c>, which is what a member with no attribute of its own
    /// inherits.
    /// </summary>
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

    private static byte? Own(IList<CustomAttributeData> attributes) =>
        Read(attributes, NullableAttributeName);

    /// <summary>
    /// Reads the first flag byte out of a compiler-emitted nullability attribute, which
    /// carries either a single <see cref="byte"/> (every position agrees) or a
    /// <see cref="byte"/> array whose first element is the outermost type's flag — the one
    /// being asserted.
    /// </summary>
    private static byte? Read(IList<CustomAttributeData> attributes, string attributeFullName)
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

            if (argument is IReadOnlyList<CustomAttributeTypedArgument> flags && flags.Count > 0)
            {
                return (byte)flags[0].Value!;
            }
        }

        return null;
    }
}
