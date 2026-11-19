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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Finds the getters a type declares: its public, parameterless instance methods that return a value, leaving out
/// accessors and the overrides of <see cref="object"/>, so a shape test can compare them with Java's accessors.
/// </summary>
internal static class Getters
{
    internal static IEnumerable<MethodInfo> Of(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName
                && method.GetParameters().Length == 0
                && method.ReturnType != typeof(void)
                && method.GetBaseDefinition().DeclaringType == type);

    internal static IEnumerable<string> NamesOf(Type type) =>
        Of(type).Select(method => method.Name).OrderBy(name => name, StringComparer.Ordinal);

    internal static MethodInfo Get(Type type, string name) => type.GetMethod(name, Type.EmptyTypes)!;
}
