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

using System.Collections.Immutable;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks that no public type has an <c>operator ==</c> or <c>operator !=</c> (CKRDA0018).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EqualityOperatorAnalyzer : DiagnosticAnalyzer
{
    private static readonly SymbolDisplayFormat s_typeName = new SymbolDisplayFormat(
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypes,
        genericsOptions: SymbolDisplayGenericsOptions.IncludeTypeParameters);

    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.EqualityOperator,
        title: "A public type has no operator == or !=",
        messageFormat: "{0}",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Java's == and != compare references, and no Java class can change that; Java's equals compares "
            + "values. So .NET's == and != compare references too, as they do on a class that declares neither, and "
            + "Equals stands for Java's equals (dotnet/CLAUDE.md §3, Java names rule 7). An operator a user compiles "
            + "against cannot be added or removed later without changing what the user's == means, so a public type "
            + "declares none. A record is reported too: the compiler gives it a == that compares values. Only a type "
            + "visible outside its assembly is checked: a public or protected type whose containing types are all "
            + "public or protected.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeType, SymbolKind.NamedType);
    }

    private static void AnalyzeType(SymbolAnalysisContext context)
    {
        INamedTypeSymbol type = (INamedTypeSymbol)context.Symbol;
        if (!IsVisible(type))
        {
            return;
        }

        string name = type.ToDisplayString(s_typeName);
        foreach (ISymbol member in type.GetMembers())
        {
            if (member is not IMethodSymbol { MethodKind: MethodKind.UserDefinedOperator } method
                || OperatorOf(method.Name) is not string op)
            {
                continue;
            }

            string advice = $"so {op} compares references as Java's does, and compare values with Equals, Java's equals";
            string message = method.IsImplicitlyDeclared
                ? $"{name} is a record, whose compiler-made operator {op} compares values; make it a class, {advice}"
                : $"{name} declares operator {op}; remove it, {advice}";
            context.ReportDiagnostic(Diagnostic.Create(s_rule, LocationOf(method, type), message));
        }
    }

    private static string? OperatorOf(string metadataName) => metadataName switch
    {
        WellKnownMemberNames.EqualityOperatorName => "==",
        WellKnownMemberNames.InequalityOperatorName => "!=",
        _ => null,
    };

    // A record's operators have no source of their own, so they are reported on the record's name.
    private static Location LocationOf(IMethodSymbol method, INamedTypeSymbol type) =>
        method.Locations.FirstOrDefault(location => location.IsInSource)
        ?? type.Locations.FirstOrDefault(location => location.IsInSource)
        ?? Location.None;

    private static bool IsVisible(INamedTypeSymbol type)
    {
        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            if (current.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Protected
                or Accessibility.ProtectedOrInternal))
            {
                return false;
            }
        }

        return true;
    }
}
