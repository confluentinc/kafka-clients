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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Finds the setters a type declares: its public instance methods that take one argument and return the type itself
/// or nothing, so a shape test can compare them with Java's setters.
/// </summary>
internal static class SetterMethods
{
    internal static IEnumerable<MethodInfo> Of(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName
                && method.GetParameters().Length == 1
                && (method.ReturnType == type || method.ReturnType == typeof(void))
                && method.GetBaseDefinition().DeclaringType == type);

    internal static IEnumerable<string> NamesOf(Type type) =>
        Of(type).Select(method => method.Name).OrderBy(name => name, StringComparer.Ordinal);

    internal static MethodInfo Get(Type type, string name) => Of(type).Single(method => method.Name == name);

    internal static Type ParameterTypeOf(Type type, string name) => Get(type, name).GetParameters()[0].ParameterType;

    internal static void AssertGettersAndSetters(Type type, params string[] names)
    {
        IEnumerable<string> expected = names.OrderBy(name => name, StringComparer.Ordinal);
        Assert.Empty(type.GetProperties());
        Assert.Equal(expected, Getters.NamesOf(type));
        Assert.Equal(expected, NamesOf(type));
    }

    // For a class where Java names a getter differently from its setter (shouldValidateOnly() and validateOnly(x)).
    internal static void AssertGettersAndSetters(Type type, string[] getters, string[] setters)
    {
        Assert.Empty(type.GetProperties());
        Assert.Equal(getters.OrderBy(name => name, StringComparer.Ordinal), Getters.NamesOf(type));
        Assert.Equal(setters.OrderBy(name => name, StringComparer.Ordinal), NamesOf(type));
    }
}
