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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Refuses, when a client is built, a serializer or deserializer with a public method the client would never call: a
/// headers form without the interface that makes the client call it, or a <c>Close()</c> without
/// <see cref="IDisposable"/>.
/// </summary>
/// <remarks>
/// Java's default methods are extension methods here, so a method of their name written on a class that does not
/// implement the interface replacing them compiles and is never called; Java would have called it. The check reads
/// metadata only, never calls a method by reflection, and is best effort under trimming, which can remove the method.
/// </remarks>
internal static class SerdeGuard
{
    internal const string Producer = "producer";

    internal const string Consumer = "consumer";

    private const BindingFlags PublicInstance = BindingFlags.Public | BindingFlags.Instance;

    internal static void Check<T>(ISerializer<T> serializer, string client, bool disposes)
    {
        if (serializer is not ISerializerWithHeaders<T>
            && Declares(serializer, "Serialize", typeof(string), typeof(Headers), typeof(T)))
        {
            string type = TypeText(typeof(T));
            throw NeverCalled(serializer, "Serialize(string, Headers, " + type + ")", "ISerializerWithHeaders<" + type + ">", client);
        }

        CheckClose(serializer, client, disposes);
    }

    internal static void Check<T>(IDeserializer<T> deserializer, string client, bool disposes)
    {
        if (deserializer is not IDeserializerWithHeaders<T>
            && Declares(deserializer, "Deserialize", typeof(string), typeof(Headers), typeof(ReadOnlySpan<byte>)))
        {
            string type = TypeText(typeof(T));
            throw NeverCalled(
                deserializer, "Deserialize(string, Headers, ReadOnlySpan<byte>)", "IDeserializerWithHeaders<" + type + ">", client);
        }

        CheckClose(deserializer, client, disposes);
    }

    // The mocks never dispose a serde, so only the clients that do refuse a Close() they would not call.
    private static void CheckClose(object serde, string client, bool disposes)
    {
        if (disposes && serde is not IDisposable && Declares(serde, "Close"))
        {
            throw NeverCalled(serde, "Close()", "IDisposable", client);
        }
    }

    private static IllegalArgumentException NeverCalled(object serde, string method, string contract, string client) =>
        new IllegalArgumentException(
            TypeText(serde.GetType()) + " declares " + method + " but does not implement " + contract + ", so the " + client
                + " would never call it.");

    // A public instance method of exactly these parameter types, inherited ones included. Only metadata is read.
    [UnconditionalSuppressMessage(
        "ReflectionAnalysis",
        "IL2075",
        Justification = "A best-effort check: a method the trimmer removed is one nothing could call.")]
    private static bool Declares(object serde, string name, params Type[] parameters)
    {
        foreach (MethodInfo method in serde.GetType().GetMethods(PublicInstance))
        {
            if (method.Name == name && !method.IsGenericMethodDefinition && SameTypes(method.GetParameters(), parameters))
            {
                return true;
            }
        }

        return false;
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

    // A type as C# writes it: a keyword for a built-in type, T? for a nullable value type, and a top-level generic
    // type's arguments in angle brackets; any other type as Type.ToString writes it.
    private static string TypeText(Type type)
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
