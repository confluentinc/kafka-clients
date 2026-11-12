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
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Finds, in an object a client is given, a public method of the name and parameters of one of Java's default
/// methods, and builds the <see cref="IllegalArgumentException"/> a client throws when the object does not implement
/// the interface through which the client would call that method.
/// </summary>
/// <remarks>
/// Java's default methods are extension methods here, so a method of their name written on a class that does not
/// implement the interface replacing them compiles and is never called; Java would have called it. The check reads
/// metadata only, never calls a method by reflection, and is best effort under trimming, which can remove the method.
/// </remarks>
internal static class DefaultMethodGuard
{
    internal const string Producer = "producer";

    internal const string Consumer = "consumer";

    private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

    internal static IllegalArgumentException NeverCalled(object instance, string method, string contract, string client) =>
        new IllegalArgumentException(
            TypeText(instance.GetType()) + " declares " + method + " but does not implement " + contract + ", so the "
                + client + " would never call it.");

    // A public instance method of exactly these parameter types, inherited ones included. Only metadata is read.
    [UnconditionalSuppressMessage(
        "ReflectionAnalysis",
        "IL2075",
        Justification = "A best-effort check: a method the trimmer removed is one nothing could call.")]
    internal static bool Declares(object instance, string name, params Type[] parameters)
    {
        foreach (MethodInfo method in instance.GetType().GetMethods(PublicInstance))
        {
            if (method.Name == name && !method.IsGenericMethodDefinition && SameTypes(method.GetParameters(), parameters))
            {
                return true;
            }
        }

        return false;
    }

    // A type as C# writes it: a keyword for a built-in type, T? for a nullable value type, and a top-level generic
    // type's arguments in angle brackets; any other type as Type.ToString writes it.
    internal static string TypeText(Type type)
    {
        if (Nullable.GetUnderlyingType(type) is Type underlying)
        {
            return TypeText(underlying) + "?";
        }

        if (type.IsArray)
        {
            return TypeText(type.GetElementType()!) + "[" + new string(',', type.GetArrayRank() - 1) + "]";
        }

        if (!type.IsEnum && Keyword(type) is string keyword)
        {
            return keyword;
        }

        string name = string.IsNullOrEmpty(type.Namespace) ? type.Name : type.Namespace + "." + type.Name;
        int tick = name.IndexOf('`');
        if (!type.IsGenericType || type.IsNested || tick < 0)
        {
            return type.ToString();
        }

        return name.Substring(0, tick) + "<" + string.Join(", ", Array.ConvertAll(type.GetGenericArguments(), TypeText)) + ">";
    }

    private static bool SameTypes(ParameterInfo[] actual, Type[] expected)
    {
        if (actual.Length != expected.Length)
        {
            return false;
        }

        for (int i = 0; i < actual.Length; i++)
        {
            if (actual[i].ParameterType != expected[i])
            {
                return false;
            }
        }

        return true;
    }

    private static string? Keyword(Type type) => Type.GetTypeCode(type) switch
    {
        TypeCode.Boolean => "bool",
        TypeCode.Byte => "byte",
        TypeCode.SByte => "sbyte",
        TypeCode.Int16 => "short",
        TypeCode.UInt16 => "ushort",
        TypeCode.Int32 => "int",
        TypeCode.UInt32 => "uint",
        TypeCode.Int64 => "long",
        TypeCode.UInt64 => "ulong",
        TypeCode.Single => "float",
        TypeCode.Double => "double",
        TypeCode.Decimal => "decimal",
        TypeCode.Char => "char",
        TypeCode.String => "string",
        _ => type == typeof(object) ? "object" : null,
    };
}
