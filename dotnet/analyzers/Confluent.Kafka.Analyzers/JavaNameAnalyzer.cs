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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Threading;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks that every public type and member carries the name of the Java type or member it translates (CKRDA0013),
/// against the Java list and the three lists of exclusions and pairs given as additional files, and that every line of
/// those lists still matches something (CKRDA0014). A project given no Java list is not checked.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class JavaNameAnalyzer : DiagnosticAnalyzer
{
    /// <summary>The Java list <c>cargo xtask java-api</c> writes.</summary>
    public const string JavaApiFile = "java-api.txt";

    /// <summary>The findings not fixed yet, one key per line.</summary>
    public const string BaselineFile = "java-name-baseline.txt";

    /// <summary>The findings kept on purpose, one key per line.</summary>
    public const string AllowlistFile = "java-name-allowlist.txt";

    /// <summary>The renamed types, one <c>.NET type name</c> and Java type per line.</summary>
    public const string PairsFile = "java-name-pairs.txt";

    private static readonly HashSet<string> s_enumBuiltIns =
        new HashSet<string>(StringComparer.Ordinal) { "name", "ordinal", "values", "valueOf" };

    private static readonly DiagnosticDescriptor s_nameRule = new DiagnosticDescriptor(
        DiagnosticIds.JavaName,
        title: "A public type or member has the name of the Java type or member it translates",
        messageFormat: "{0}",
        category: "Naming",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Each finding starts with its key. Fix it, or list the key in " + BaselineFile + " (to fix later) or "
            + AllowlistFile + " (kept on purpose).",
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    private static readonly DiagnosticDescriptor s_staleRule = new DiagnosticDescriptor(
        DiagnosticIds.StaleJavaNameLine,
        title: "Every line of a Java-name list matches something",
        messageFormat: "'{0}' in {1} matches nothing; delete it",
        category: "Naming",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The baseline, the allowlist and the pairs only shrink: a line that no finding or type needs goes.",
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(s_nameRule, s_staleRule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.Analyze | GeneratedCodeAnalysisFlags.ReportDiagnostics);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(Start);
    }

    private static void Start(CompilationStartAnalysisContext context)
    {
        ImmutableArray<AdditionalText> files = context.Options.AdditionalFiles;
        SourceText? api = Find(files, JavaApiFile)?.GetText(context.CancellationToken);
        if (api is null)
        {
            return;
        }

        Session session = new Session(
            JavaApi.Parse(api.ToString()),
            ListFile.Read(Find(files, BaselineFile), context.CancellationToken),
            ListFile.Read(Find(files, AllowlistFile), context.CancellationToken),
            ListFile.Read(Find(files, PairsFile), context.CancellationToken));
        context.RegisterSymbolAction(session.Collect, SymbolKind.NamedType);
        context.RegisterCompilationEndAction(session.Report);
    }

    private static AdditionalText? Find(ImmutableArray<AdditionalText> files, string name) =>
        files.FirstOrDefault(file => string.Equals(Path.GetFileName(file.Path), name, StringComparison.Ordinal));

    /// <summary>One compilation's check: collects the public types, then pairs, checks and reports them at its end.</summary>
    private sealed class Session
    {
        private readonly JavaApi _api;
        private readonly ListFile? _baseline;
        private readonly ListFile? _allowlist;
        private readonly ListFile? _pairs;
        private readonly ConcurrentBag<INamedTypeSymbol> _types = new ConcurrentBag<INamedTypeSymbol>();

        internal Session(JavaApi api, ListFile? baseline, ListFile? allowlist, ListFile? pairs)
        {
            _api = api;
            _baseline = baseline;
            _allowlist = allowlist;
            _pairs = pairs;
        }

        internal void Collect(SymbolAnalysisContext context)
        {
            INamedTypeSymbol type = (INamedTypeSymbol)context.Symbol;
            string ns = type.ContainingNamespace.ToDisplayString();
            if (IsPublic(type) && (ns == "Confluent.Kafka" || ns.StartsWith("Confluent.Kafka.", StringComparison.Ordinal)))
            {
                _types.Add(type);
            }
        }

        internal void Report(CompilationAnalysisContext context)
        {
            Dictionary<string, string> pairs = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (string entry in Entries(_pairs))
            {
                string[] parts = entry.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length == 2)
                {
                    pairs[parts[0]] = parts[1];
                }
            }

            HashSet<string> usedPairs = new HashSet<string>(StringComparer.Ordinal);
            List<(INamedTypeSymbol Type, Pairing Pairing)> pairings = _types
                .OrderBy(DocId, StringComparer.Ordinal)
                .Select(type => (type, Pair(type, pairs, usedPairs)))
                .ToList();
            HashSet<string> pairedMarkers = new HashSet<string>(
                pairings.Where(p => p.Pairing.Java is not null).Select(p => p.Pairing.Java!.Marker),
                StringComparer.Ordinal);

            List<Finding> findings = new List<Finding>();
            foreach ((INamedTypeSymbol type, Pairing pairing) in pairings)
            {
                Check(type, pairing, pairedMarkers, findings);
            }

            HashSet<string> found = new HashSet<string>(findings.Select(f => f.Key), StringComparer.Ordinal);
            HashSet<string> excluded = new HashSet<string>(Entries(_allowlist).Concat(Entries(_baseline)), StringComparer.Ordinal);
            foreach (Finding finding in findings.Where(f => !excluded.Contains(f.Key)))
            {
                context.ReportDiagnostic(
                    Diagnostic.Create(s_nameRule, finding.Location, finding.Key + ": " + finding.Explanation));
            }

            ReportStale(context, _baseline, found.Contains);
            ReportStale(context, _allowlist, found.Contains);
            ReportStale(context, _pairs, entry => usedPairs.Contains(entry.Split(' ', '\t')[0]));
        }

        private Pairing Pair(INamedTypeSymbol type, Dictionary<string, string> pairs, HashSet<string> usedPairs)
        {
            string ns = type.ContainingNamespace.ToDisplayString();
            List<INamedTypeSymbol> chain = new List<INamedTypeSymbol>();
            for (INamedTypeSymbol? t = type; t is not null; t = t.ContainingType)
            {
                chain.Insert(0, t);
            }

            string javaKey = ns + "." + string.Join("$", chain.Select(JavaNameOf));
            JavaType? exact = _api.ByDotNetName(javaKey);
            if (exact is not null)
            {
                return new Pairing(exact, nameIsJava: true);
            }

            JavaType? listed = Listed(ns + "." + string.Join(".", chain.Select(t => t.Name)), pairs, usedPairs);
            if (listed is not null)
            {
                return new Pairing(listed, nameIsJava: false);
            }

            string syncKey = ns + "." + string.Join("$", chain.Select(t => WithoutAsync(JavaNameOf(t))));
            if (syncKey != javaKey)
            {
                JavaType? peer = _api.ByDotNetName(syncKey)
                    ?? Listed(ns + "." + string.Join(".", chain.Select(t => WithoutAsync(t.Name))), pairs, usedPairs);
                if (peer is not null)
                {
                    return new Pairing(peer, nameIsJava: false);
                }
            }

            // A Java nested type C# cannot nest under its name (CS0102: the outer type has a member of that name)
            // sits beside its outer type with its bare name, as the namespace layout rule allows.
            if (type.ContainingType is null)
            {
                List<JavaType> nested = _api.NestedNamed(ns, JavaNameOf(type)).ToList();
                if (nested.Count == 1)
                {
                    string classPath = nested[0].DotNetName.Substring(ns.Length + 1);
                    string outer = classPath.Substring(0, classPath.LastIndexOf('$'));
                    bool forced = classPath.IndexOf('$') == classPath.LastIndexOf('$')
                        && type.ContainingNamespace.GetTypeMembers()
                            .Any(t => JavaNameOf(t) == outer && t.GetMembers(type.Name).Any(m => m is not INamedTypeSymbol));
                    return new Pairing(nested[0], nameIsJava: forced);
                }
            }

            return new Pairing(null, nameIsJava: false);
        }

        private JavaType? Listed(string dotNetName, Dictionary<string, string> pairs, HashSet<string> usedPairs)
        {
            if (!pairs.TryGetValue(dotNetName, out string? marker))
            {
                return null;
            }

            JavaType? type = _api.ByMarker(marker);
            if (type is not null)
            {
                usedPairs.Add(dotNetName);
            }

            return type;
        }

        private void Check(INamedTypeSymbol type, Pairing pairing, HashSet<string> pairedMarkers, List<Finding> findings)
        {
            string typeId = DocId(type);
            Location typeLocation = LocationOf(type);
            JavaType? java = pairing.Java;
            if (!pairing.NameIsJava)
            {
                findings.Add(new Finding(
                    "unmatched " + typeId,
                    java is null
                        ? "Java has no type of this name, so its members are not checked"
                        : "Java has no type of this name; its members are compared with " + java.Marker,
                    typeLocation));
            }

            if (java is null || type.TypeKind == TypeKind.Delegate)
            {
                return;
            }

            IReadOnlyList<JavaType> chain = _api.Chain(java);
            HashSet<string> jdk = _api.JdkSupertypes(java);
            List<JavaMember> visible = chain.SelectMany(link => link.Members).ToList();
            List<JavaMember> required = chain
                .Where(link => link == java || !pairedMarkers.Contains(link.Marker))
                .SelectMany(link => link.Members)
                .Where(m => m.Kind != JavaMemberKind.Constructor || m.DeclaringType == java.Marker)
                .Where(m => type.TypeKind != TypeKind.Enum || m.Kind != JavaMemberKind.Method || !s_enumBuiltIns.Contains(m.Name))
                .ToList();
            Dictionary<string, string> javaMethods = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (JavaMember method in visible.Where(m => m.Kind == JavaMemberKind.Method))
            {
                javaMethods[DotNetNameOf(method.Name)] = method.Name;
            }

            HashSet<string> javaFields = new HashSet<string>(
                visible.Where(m => m.Kind == JavaMemberKind.Field || m.Kind == JavaMemberKind.Constant).Select(m => m.Name),
                StringComparer.Ordinal);

            CheckNames(type, java, jdk, javaMethods, javaFields, findings);
            CheckOverloads(type, java, chain, required, javaMethods, findings);

            List<INamedTypeSymbol> netChain = NetChain(type).ToList();
            foreach (JavaMember field in required.Where(m => m.Kind == JavaMemberKind.Field || m.Kind == JavaMemberKind.Constant))
            {
                if (!netChain.Any(t => t.GetMembers(field.Name).Any(m => m.DeclaredAccessibility == Accessibility.Public)))
                {
                    findings.Add(Missing(typeId, field.Marker, typeLocation));
                }
            }

            foreach (JavaType nested in _api.NestedIn(java).Where(n => !pairedMarkers.Contains(n.Marker)))
            {
                findings.Add(Missing(typeId, nested.Marker, typeLocation));
            }
        }

        private static void CheckNames(
            INamedTypeSymbol type,
            JavaType java,
            HashSet<string> jdk,
            Dictionary<string, string> javaMethods,
            HashSet<string> javaFields,
            List<Finding> findings)
        {
            foreach (ISymbol member in type.GetMembers())
            {
                if (member.DeclaredAccessibility != Accessibility.Public
                    || member.IsImplicitlyDeclared
                    || member is INamedTypeSymbol
                    || IsFixed(type, member, jdk))
                {
                    continue;
                }

                switch (member)
                {
                    case IMethodSymbol method when method.MethodKind == MethodKind.Ordinary
                        || method.MethodKind == MethodKind.UserDefinedOperator
                        || method.MethodKind == MethodKind.Conversion:
                        if (!javaMethods.ContainsKey(method.Name))
                        {
                            findings.Add(new Finding(
                                "unmatched " + DocId(method),
                                "Java's " + java.SimpleName + " has no method of this name",
                                LocationOf(method)));
                        }

                        break;
                    case IPropertySymbol property:
                        if (javaMethods.TryGetValue(property.Name, out string? javaName))
                        {
                            findings.Add(new Finding(
                                "property " + DocId(property),
                                "Java's " + java.SimpleName + "." + javaName + "() is a method",
                                LocationOf(property)));
                        }
                        else if (!javaFields.Contains(property.Name))
                        {
                            findings.Add(new Finding(
                                "unmatched " + DocId(property),
                                "Java's " + java.SimpleName + " has no method or field of this name",
                                LocationOf(property)));
                        }

                        break;
                    case IFieldSymbol field when !javaFields.Contains(field.Name):
                        findings.Add(new Finding(
                            "unmatched " + DocId(field),
                            "Java's " + java.SimpleName + " has no field or constant of this name",
                            LocationOf(field)));
                        break;
                    case IEventSymbol @event:
                        findings.Add(new Finding("unmatched " + DocId(@event), "Java has no events", LocationOf(@event)));
                        break;
                }
            }
        }

        private static void CheckOverloads(
            INamedTypeSymbol type,
            JavaType java,
            IReadOnlyList<JavaType> chain,
            List<JavaMember> required,
            Dictionary<string, string> javaMethods,
            List<Finding> findings)
        {
            List<IMethodSymbol> netMethods = NetChain(type)
                .SelectMany(t => t.GetMembers().OfType<IMethodSymbol>())
                .Where(m => m.MethodKind == MethodKind.Ordinary
                    && m.DeclaredAccessibility == Accessibility.Public
                    && !m.IsImplicitlyDeclared)
                .ToList();
            HashSet<string> netProperties = new HashSet<string>(
                NetChain(type)
                    .SelectMany(t => t.GetMembers().OfType<IPropertySymbol>())
                    .Where(p => p.DeclaredAccessibility == Accessibility.Public)
                    .Select(p => p.Name),
                StringComparer.Ordinal);
            HashSet<string> names = new HashSet<string>(
                netMethods.Select(m => m.Name).Where(javaMethods.ContainsKey),
                StringComparer.Ordinal);
            names.UnionWith(required.Where(m => m.Kind == JavaMemberKind.Method).Select(m => DotNetNameOf(m.Name)));
            foreach (string name in names.OrderBy(n => n, StringComparer.Ordinal))
            {
                List<NetOverload> net = Overloads(type, netMethods.Where(m => m.Name == name));
                if (net.Count == 0 && netProperties.Contains(name))
                {
                    continue;
                }

                List<JavaOverload> javaOverloads = JavaOverloads(
                    chain.SelectMany(link => link.Members.Select(m => (link, m)))
                        .Where(p => p.m.Kind == JavaMemberKind.Method && DotNetNameOf(p.m.Name) == name),
                    required);
                Match(type, java.SimpleName + "." + JavaNameIn(javaOverloads, name), net, javaOverloads, findings);
            }

            if (type.TypeKind == TypeKind.Class || type.TypeKind == TypeKind.Struct)
            {
                List<NetOverload> ctors = Overloads(
                    type,
                    type.InstanceConstructors.Where(c => c.DeclaredAccessibility == Accessibility.Public
                        && !(type.IsValueType && c.IsImplicitlyDeclared)));
                List<JavaOverload> javaCtors = JavaOverloads(
                    java.Members.Where(m => m.Kind == JavaMemberKind.Constructor).Select(m => (java, m)),
                    required);
                Match(type, java.SimpleName + " constructor", ctors, javaCtors, findings);
            }
        }

        private static string JavaNameIn(List<JavaOverload> overloads, string dotNetName) =>
            overloads.Count > 0 ? overloads[0].Member.Name : char.ToLowerInvariant(dotNetName[0]) + dotNetName.Substring(1);

        private static List<NetOverload> Overloads(INamedTypeSymbol type, IEnumerable<IMethodSymbol> methods)
        {
            List<NetOverload> overloads = new List<NetOverload>();
            foreach (IMethodSymbol method in methods)
            {
                bool declaredHere = SymbolEqualityComparer.Default.Equals(method.ContainingType, type);
                NetOverload overload = new NetOverload(method, declaredHere);
                int same = overloads.FindIndex(o => o.Names.SequenceEqual(overload.Names));
                if (same < 0)
                {
                    overloads.Add(overload);
                }
                else if (declaredHere && !overloads[same].DeclaredHere)
                {
                    overloads[same] = overload;
                }
            }

            return overloads;
        }

        // A method declared on a Java interface takes its parameter names from the interface, as the Python binding
        // names them; one declared only on a class, from the class.
        private static List<JavaOverload> JavaOverloads(IEnumerable<(JavaType Link, JavaMember Member)> declarations, List<JavaMember> required)
        {
            List<JavaOverload> overloads = new List<JavaOverload>();
            foreach (IGrouping<string, (JavaType Link, JavaMember Member)> signature in declarations
                .GroupBy(d => d.Member.Name + "(" + string.Join(",", d.Member.ParameterTypes) + ")", StringComparer.Ordinal))
            {
                JavaMember chosen = signature.Where(d => d.Link.IsInterface).Select(d => d.Member).FirstOrDefault()
                    ?? signature.First().Member;
                bool isRequired = signature.Any(d => required.Contains(d.Member));
                JavaMember reported = signature.Select(d => d.Member).FirstOrDefault(required.Contains) ?? chosen;
                overloads.Add(new JavaOverload(chosen, reported, isRequired));
            }

            return overloads;
        }

        private static void Match(INamedTypeSymbol type, string javaName, List<NetOverload> net, List<JavaOverload> java, List<Finding> findings)
        {
            bool[] netDone = new bool[net.Count];
            bool[] javaDone = new bool[java.Count];
            for (int pass = 0; pass < 3; pass++)
            {
                for (int i = 0; i < net.Count; i++)
                {
                    for (int j = 0; j < java.Count && !netDone[i]; j++)
                    {
                        if (javaDone[j])
                        {
                            continue;
                        }

                        string[] javaNames = java[j].Member.ParameterNames;
                        NetOverload n = net[i];
                        bool matches = pass switch
                        {
                            0 => n.Names.Count == javaNames.Length && SameNames(n.Names, javaNames),
                            1 => javaNames.Length < n.Names.Count && n.Required <= javaNames.Length && SameNames(n.Names, javaNames),
                            _ => n.Names.Count == javaNames.Length,
                        };
                        if (!matches)
                        {
                            continue;
                        }

                        netDone[i] = javaDone[j] = true;
                        if (pass == 1 && n.DeclaredHere)
                        {
                            findings.Add(new Finding(
                                "parameter " + DocId(n.Method),
                                "adds the .NET-only parameter(s) " + string.Join(", ", n.Names.Skip(javaNames.Length))
                                    + " to Java's " + javaName + "(" + string.Join(", ", javaNames) + ")",
                                LocationOf(n.Method)));
                        }
                        else if (pass == 2 && n.DeclaredHere)
                        {
                            findings.Add(new Finding(
                                "parameter " + DocId(n.Method),
                                "names its parameters (" + string.Join(", ", n.Names) + ") where Java's " + javaName
                                    + " names them (" + string.Join(", ", javaNames) + ")",
                                LocationOf(n.Method)));
                        }
                    }
                }
            }

            for (int i = 0; i < net.Count; i++)
            {
                if (!netDone[i] && net[i].DeclaredHere)
                {
                    findings.Add(new Finding(
                        "overload " + DocId(net[i].Method),
                        "Java's " + javaName + " has no overload with " + net[i].Names.Count + " parameter(s)",
                        LocationOf(net[i].Method)));
                }
            }

            for (int j = 0; j < java.Count; j++)
            {
                if (!javaDone[j] && java[j].IsRequired)
                {
                    findings.Add(Missing(DocId(type), java[j].Reported.Marker, LocationOf(type)));
                }
            }
        }

        private static bool SameNames(IReadOnlyList<string> net, string[] java)
        {
            for (int i = 0; i < java.Length; i++)
            {
                if (java[i] != "?" && java[i] != net[i])
                {
                    return false;
                }
            }

            return true;
        }

        private static Finding Missing(string typeId, string javaMarker, Location location) =>
            new Finding("missing " + typeId + " " + javaMarker, "has no counterpart of Java's " + javaMarker, location);

        // The members .NET itself requires once a type takes the .NET form of a Java supertype: the overrides of
        // object and Exception, and the members of the .NET interfaces that stand for Iterable, Closeable or
        // AutoCloseable, and Comparable.
        private static bool IsFixed(INamedTypeSymbol type, ISymbol member, HashSet<string> jdk)
        {
            for (ISymbol? overridden = Overridden(member); overridden is not null; overridden = Overridden(overridden))
            {
                if (IsSystem(overridden.ContainingType))
                {
                    return true;
                }
            }

            foreach (INamedTypeSymbol contract in type.AllInterfaces.Where(IsSystem))
            {
                foreach (ISymbol contractMember in contract.GetMembers())
                {
                    if (!SymbolEqualityComparer.Default.Equals(type.FindImplementationForInterfaceMember(contractMember), member))
                    {
                        continue;
                    }

                    switch (contract.Name)
                    {
                        case "IEnumerable":
                            return jdk.Contains("Iterable");
                        case "IDisposable":
                        case "IAsyncDisposable":
                            return jdk.Contains("Closeable") || jdk.Contains("AutoCloseable");
                        case "IComparable":
                            return jdk.Contains("Comparable");
                        case "IEquatable":
                            return true;
                    }
                }
            }

            return false;
        }

        private static ISymbol? Overridden(ISymbol member) => member switch
        {
            IMethodSymbol method => method.OverriddenMethod,
            IPropertySymbol property => property.OverriddenProperty,
            _ => null,
        };

        private static IEnumerable<INamedTypeSymbol> NetChain(INamedTypeSymbol type)
        {
            yield return type;
            for (INamedTypeSymbol? b = type.BaseType; b is not null && !IsSystem(b); b = b.BaseType)
            {
                yield return b;
            }

            foreach (INamedTypeSymbol contract in type.AllInterfaces.Where(i => !IsSystem(i)))
            {
                yield return contract;
            }
        }

        private static void ReportStale(CompilationAnalysisContext context, ListFile? list, Func<string, bool> matches)
        {
            if (list is null)
            {
                return;
            }

            foreach ((string entry, TextLine line) in list.Lines)
            {
                if (!matches(entry))
                {
                    context.ReportDiagnostic(Diagnostic.Create(s_staleRule, list.LocationOf(line), entry, list.FileName));
                }
            }
        }

        private static IEnumerable<string> Entries(ListFile? list) =>
            list is null ? Enumerable.Empty<string>() : list.Lines.Select(l => l.Entry);
    }

    private static bool IsPublic(INamedTypeSymbol type)
    {
        for (INamedTypeSymbol? t = type; t is not null; t = t.ContainingType)
        {
            if (t.DeclaredAccessibility != Accessibility.Public)
            {
                return false;
            }
        }

        return !type.IsImplicitlyDeclared;
    }

    private static bool IsSystem(INamedTypeSymbol type)
    {
        string ns = type.ContainingNamespace.ToDisplayString();
        return ns == "System" || ns.StartsWith("System.", StringComparison.Ordinal);
    }

    private static string JavaNameOf(INamedTypeSymbol type) =>
        type.TypeKind == TypeKind.Interface && type.Name.Length > 1 && type.Name[0] == 'I' && char.IsUpper(type.Name[1])
            ? type.Name.Substring(1)
            : type.Name;

    private static string WithoutAsync(string name)
    {
        if (name.Length > 6 && name.StartsWith("IAsync", StringComparison.Ordinal) && char.IsUpper(name[6]))
        {
            return "I" + name.Substring(6);
        }

        return name.Length > 5 && name.StartsWith("Async", StringComparison.Ordinal) && char.IsUpper(name[5])
            ? name.Substring(5)
            : name;
    }

    // The .NET name of a Java method: PascalCase, except the JDK members .NET names its own way.
    private static string DotNetNameOf(string javaMethod) => javaMethod switch
    {
        "hashCode" => "GetHashCode",
        "iterator" => "GetEnumerator",
        "getMessage" => "Message",
        "getCause" => "InnerException",
        _ => char.ToUpperInvariant(javaMethod[0]) + javaMethod.Substring(1),
    };

    private static string DocId(ISymbol symbol) => symbol.GetDocumentationCommentId() ?? symbol.ToDisplayString();

    private static Location LocationOf(ISymbol symbol) =>
        symbol.Locations.FirstOrDefault(l => l.IsInSource)
        ?? (symbol.ContainingType is not null ? LocationOf(symbol.ContainingType) : Location.None);

    /// <summary>How a .NET type pairs with a Java type: the Java type, if any, and whether the .NET name is Java's.</summary>
    private readonly struct Pairing
    {
        internal Pairing(JavaType? java, bool nameIsJava)
        {
            Java = java;
            NameIsJava = nameIsJava;
        }

        internal JavaType? Java { get; }

        internal bool NameIsJava { get; }
    }

    /// <summary>One finding: its key, what it means, and where it is reported.</summary>
    private sealed class Finding
    {
        internal Finding(string key, string explanation, Location location)
        {
            Key = key;
            Explanation = explanation;
            Location = location;
        }

        internal string Key { get; }

        internal string Explanation { get; }

        internal Location Location { get; }
    }

    /// <summary>A .NET method or constructor with its parameter names and how many of them are required.</summary>
    private sealed class NetOverload
    {
        internal NetOverload(IMethodSymbol method, bool declaredHere)
        {
            Method = method;
            DeclaredHere = declaredHere;
            Names = method.Parameters.Select(p => p.Name).ToList();
            Required = method.Parameters.Count(p => !p.IsOptional);
        }

        internal IMethodSymbol Method { get; }

        internal bool DeclaredHere { get; }

        internal IReadOnlyList<string> Names { get; }

        internal int Required { get; }
    }

    /// <summary>A Java overload: the declaration that names its parameters, the one a finding names, and whether the
    /// .NET type must have it.</summary>
    private sealed class JavaOverload
    {
        internal JavaOverload(JavaMember member, JavaMember reported, bool isRequired)
        {
            Member = member;
            Reported = reported;
            IsRequired = isRequired;
        }

        internal JavaMember Member { get; }

        internal JavaMember Reported { get; }

        internal bool IsRequired { get; }
    }

    /// <summary>One of the Java-name lists: its lines other than blanks and comments, each with where it is in the file.</summary>
    private sealed class ListFile
    {
        private readonly string _path;
        private readonly SourceText _text;

        private ListFile(string path, SourceText text)
        {
            _path = path;
            _text = text;
            Lines = text.Lines
                .Select(line => (Entry: line.ToString().Trim(), Line: line))
                .Where(l => l.Entry.Length > 0 && l.Entry[0] != '#')
                .ToList();
        }

        internal IReadOnlyList<(string Entry, TextLine Line)> Lines { get; }

        internal string FileName => Path.GetFileName(_path);

        internal static ListFile? Read(AdditionalText? file, CancellationToken cancellationToken)
        {
            SourceText? text = file?.GetText(cancellationToken);
            return file is null || text is null ? null : new ListFile(file.Path, text);
        }

        internal Location LocationOf(TextLine line) =>
            Location.Create(_path, line.Span, _text.Lines.GetLinePositionSpan(line.Span));
    }
}
