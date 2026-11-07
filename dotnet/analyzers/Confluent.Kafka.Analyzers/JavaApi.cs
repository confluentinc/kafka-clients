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

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// The Java client's public API, read from the list <c>cargo xtask java-api</c> writes, with each type found by the
/// .NET name the namespace layout rule gives it.
/// </summary>
internal sealed class JavaApi
{
    private const string JavaPrefix = "org.apache.kafka.";
    private const string ClientsSegment = "clients.";
    private const string RootNamespace = "Confluent.Kafka";

    private readonly Dictionary<string, JavaType> _byMarker;
    private readonly Dictionary<string, JavaType> _byDotNetName = new Dictionary<string, JavaType>(StringComparer.Ordinal);
    private readonly Dictionary<string, List<JavaType>> _nested = new Dictionary<string, List<JavaType>>(StringComparer.Ordinal);

    private JavaApi(Dictionary<string, JavaType> byMarker)
    {
        _byMarker = byMarker;
        foreach (JavaType type in byMarker.Values)
        {
            _byDotNetName[type.DotNetName] = type;
            int nested = type.Marker.LastIndexOf('$');
            if (nested >= 0)
            {
                string outer = type.Marker.Substring(0, nested);
                if (!_nested.TryGetValue(outer, out List<JavaType>? inner))
                {
                    inner = new List<JavaType>();
                    _nested[outer] = inner;
                }

                inner.Add(type);
            }
        }
    }

    /// <summary>Parses the text of the list.</summary>
    internal static JavaApi Parse(string text)
    {
        Dictionary<string, JavaType> types = new Dictionary<string, JavaType>(StringComparer.Ordinal);
        foreach (string raw in text.Split('\n'))
        {
            string line = raw.TrimEnd('\r');
            if (line.Length == 0 || line[0] == '#')
            {
                continue;
            }

            string[] fields = line.Split('\t');
            switch (fields[0])
            {
                case "type":
                    string[] supertypes = fields[3] == "-" ? Array.Empty<string>() : fields[3].Split(',');
                    types[fields[1]] = new JavaType(fields[1], fields[2], supertypes);
                    break;
                case "ctor":
                    types[fields[1]].Members.Add(JavaMember.Parse(JavaMemberKind.Constructor, fields[1], false, fields[2]));
                    break;
                case "method":
                    types[fields[1]].Members.Add(JavaMember.Parse(JavaMemberKind.Method, fields[1], fields[2] == "static", fields[3]));
                    break;
                case "field":
                    types[fields[1]].Members.Add(JavaMember.Named(JavaMemberKind.Field, fields[1], fields[2]));
                    break;
                case "constant":
                    types[fields[1]].Members.Add(JavaMember.Named(JavaMemberKind.Constant, fields[1], fields[2]));
                    break;
            }
        }

        return new JavaApi(types);
    }

    /// <summary>The type whose .NET name, namespace plus <c>Outer$Inner</c>, is <paramref name="dotNetName"/>.</summary>
    internal JavaType? ByDotNetName(string dotNetName) =>
        _byDotNetName.TryGetValue(dotNetName, out JavaType? type) ? type : null;

    /// <summary>The type of a Java marker, such as <c>org.apache.kafka.clients.producer.Callback</c>.</summary>
    internal JavaType? ByMarker(string marker) => _byMarker.TryGetValue(marker, out JavaType? type) ? type : null;

    /// <summary>The types declared directly in <paramref name="type"/>.</summary>
    internal IReadOnlyList<JavaType> NestedIn(JavaType type) =>
        _nested.TryGetValue(type.Marker, out List<JavaType>? inner) ? inner : (IReadOnlyList<JavaType>)Array.Empty<JavaType>();

    /// <summary>The nested types of a .NET namespace's Java package whose simple name is <paramref name="simpleName"/>.</summary>
    internal IEnumerable<JavaType> NestedNamed(string dotNetNamespace, string simpleName) =>
        _byMarker.Values.Where(type => type.Marker.IndexOf('$') >= 0
            && type.SimpleName == simpleName
            && type.DotNetNamespace == dotNetNamespace);

    /// <summary><paramref name="type"/>, then every supertype the list holds, each once.</summary>
    internal IReadOnlyList<JavaType> Chain(JavaType type)
    {
        List<JavaType> chain = new List<JavaType>();
        Queue<JavaType> pending = new Queue<JavaType>();
        pending.Enqueue(type);
        while (pending.Count > 0)
        {
            JavaType next = pending.Dequeue();
            if (chain.Contains(next))
            {
                continue;
            }

            chain.Add(next);
            foreach (string supertype in next.Supertypes)
            {
                JavaType? found = ByMarker(supertype);
                if (found is not null)
                {
                    pending.Enqueue(found);
                }
            }
        }

        return chain;
    }

    /// <summary>The simple names of the JDK types <paramref name="type"/> extends or implements, directly or not.</summary>
    internal HashSet<string> JdkSupertypes(JavaType type)
    {
        HashSet<string> jdk = new HashSet<string>(StringComparer.Ordinal);
        foreach (JavaType link in Chain(type))
        {
            foreach (string supertype in link.Supertypes)
            {
                if (!supertype.StartsWith(JavaPrefix, StringComparison.Ordinal))
                {
                    jdk.Add(supertype.Substring(supertype.LastIndexOf('.') + 1));
                }
            }
        }

        return jdk;
    }

    /// <summary>The .NET namespace of a Java package: <c>org.apache.kafka.</c> and <c>clients.</c> dropped, each
    /// segment in PascalCase, under <c>Confluent.Kafka</c>.</summary>
    internal static string NamespaceOf(string javaPackage)
    {
        string below = javaPackage.Substring(JavaPrefix.Length);
        if (below.StartsWith(ClientsSegment, StringComparison.Ordinal))
        {
            below = below.Substring(ClientsSegment.Length);
        }

        return RootNamespace + "." + string.Join(
            ".",
            below.Split('.').Select(segment => char.ToUpperInvariant(segment[0]) + segment.Substring(1)));
    }
}

/// <summary>A type of the Java list: its marker, kind, supertypes and public members.</summary>
internal sealed class JavaType
{
    internal JavaType(string marker, string kind, string[] supertypes)
    {
        Marker = marker;
        Kind = kind;
        Supertypes = supertypes;
        string[] segments = marker.Split('.');
        string package = string.Join(".", segments.TakeWhile(segment => char.IsLower(segment[0])));
        string classPath = string.Join(".", segments.SkipWhile(segment => char.IsLower(segment[0])));
        DotNetNamespace = JavaApi.NamespaceOf(package);
        DotNetName = DotNetNamespace + "." + classPath;
        SimpleName = classPath.Substring(classPath.LastIndexOf('$') + 1);
    }

    /// <summary>The Java name, nested classes after <c>$</c>.</summary>
    internal string Marker { get; }

    /// <summary><c>class</c>, <c>interface</c>, <c>enum</c> or <c>record</c>.</summary>
    internal string Kind { get; }

    /// <summary>A Kafka supertype's marker, or a JDK supertype as Java writes it.</summary>
    internal string[] Supertypes { get; }

    internal List<JavaMember> Members { get; } = new List<JavaMember>();

    internal string DotNetNamespace { get; }

    /// <summary>The namespace the layout rule gives it, then its class path: <c>Confluent.Kafka.Admin.Outer$Inner</c>.</summary>
    internal string DotNetName { get; }

    internal string SimpleName { get; }

    internal bool IsInterface => Kind == "interface";

    internal bool IsEnum => Kind == "enum";
}

/// <summary>What a member of the Java list is.</summary>
internal enum JavaMemberKind
{
    Constructor,
    Method,
    Field,
    Constant,
}

/// <summary>A public member of a Java type, with its parameters when it has any.</summary>
internal sealed class JavaMember
{
    private JavaMember(JavaMemberKind kind, string declaringType, string name, bool isStatic, string[] parameterTypes, string[] parameterNames)
    {
        Kind = kind;
        DeclaringType = declaringType;
        Name = name;
        IsStatic = isStatic;
        ParameterTypes = parameterTypes;
        ParameterNames = parameterNames;
    }

    internal JavaMemberKind Kind { get; }

    internal string DeclaringType { get; }

    internal string Name { get; }

    internal bool IsStatic { get; }

    internal string[] ParameterTypes { get; }

    /// <summary>The parameter names; <c>?</c> for one the source does not spell.</summary>
    internal string[] ParameterNames { get; }

    /// <summary>The member's marker: <c>Type#name(T1,T2)</c> for a method or constructor, <c>Type#NAME</c> otherwise.</summary>
    internal string Marker => Kind == JavaMemberKind.Constructor || Kind == JavaMemberKind.Method
        ? DeclaringType + "#" + Name + "(" + string.Join(",", ParameterTypes) + ")"
        : DeclaringType + "#" + Name;

    internal static JavaMember Named(JavaMemberKind kind, string declaringType, string name) =>
        new JavaMember(kind, declaringType, name, true, Array.Empty<string>(), Array.Empty<string>());

    /// <summary>Parses a signature as the list writes it: <c>name(Type a, Type b)</c>.</summary>
    internal static JavaMember Parse(JavaMemberKind kind, string declaringType, bool isStatic, string signature)
    {
        int open = signature.IndexOf('(');
        string inside = signature.Substring(open + 1, signature.Length - open - 2);
        string[] parameters = inside.Length == 0 ? Array.Empty<string>() : inside.Split(new[] { ", " }, StringSplitOptions.None);
        string[] types = new string[parameters.Length];
        string[] names = new string[parameters.Length];
        for (int i = 0; i < parameters.Length; i++)
        {
            int space = parameters[i].LastIndexOf(' ');
            types[i] = parameters[i].Substring(0, space);
            names[i] = parameters[i].Substring(space + 1);
        }

        return new JavaMember(kind, declaringType, signature.Substring(0, open), isStatic, types, names);
    }
}
