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
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks that every member of a public enum has its number written out (CKRDA0017).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EnumMemberValueAnalyzer : DiagnosticAnalyzer
{
    private static readonly SymbolDisplayFormat s_enumName =
        new SymbolDisplayFormat(typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypes);

    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.EnumMemberValue,
        title: "A public enum's member has its number written out",
        messageFormat: "{0}",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "C# compiles an enum member into each caller as its number, so a released member keeps its "
            + "number (dotnet/CLAUDE.md §3, Java names rule 7). A member with no written number takes the one after "
            + "the member before it, so a member added before it renumbers it. The message names the number to "
            + "write: the member's own, or, when another member already has that one, one above the enum's highest "
            + "number. Only an enum visible outside its assembly is checked: a public or protected enum whose "
            + "containing types are all public.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeMember, SyntaxKind.EnumMemberDeclaration);
    }

    private static void AnalyzeMember(SyntaxNodeAnalysisContext context)
    {
        EnumMemberDeclarationSyntax member = (EnumMemberDeclarationSyntax)context.Node;
        if (member.EqualsValue is not null
            || member.Parent is not EnumDeclarationSyntax declaration
            || context.SemanticModel.GetDeclaredSymbol(member, context.CancellationToken) is not IFieldSymbol field
            || !IsPublic(field.ContainingType))
        {
            return;
        }

        string name = field.ContainingType.ToDisplayString(s_enumName) + "." + field.Name;
        List<(EnumMemberDeclarationSyntax Syntax, string Name, decimal Number)> members =
            Members(context, declaration);
        decimal own = members.First(other => other.Syntax == member).Number;
        string? owner = Owner(members, member, own);
        if (owner is null)
        {
            Report(context, member, $"{name} has no written number; write it out ({field.Name} = {Text(own)})");
            return;
        }

        decimal next = members.Max(other => other.Number)
            + members.TakeWhile(other => other.Syntax != member)
                .Count(other => other.Syntax.EqualsValue is null && Owner(members, other.Syntax, other.Number) != null)
            + 1;
        string taken = $"{name} has no written number, and {Text(own)} is {owner}'s; ";
        Report(
            context,
            member,
            next <= MaxOf(field.ContainingType.EnumUnderlyingType)
                ? taken + $"write it out ({field.Name} = {Text(next)})"
                : taken + "write it out with a number no other member has");
    }

    private static List<(EnumMemberDeclarationSyntax Syntax, string Name, decimal Number)> Members(
        SyntaxNodeAnalysisContext context,
        EnumDeclarationSyntax declaration)
    {
        List<(EnumMemberDeclarationSyntax, string, decimal)> members =
            new List<(EnumMemberDeclarationSyntax, string, decimal)>();
        foreach (EnumMemberDeclarationSyntax member in declaration.Members)
        {
            if (context.SemanticModel.GetDeclaredSymbol(member, context.CancellationToken) is IFieldSymbol
                {
                    HasConstantValue: true,
                } field)
            {
                members.Add((member, field.Name, Convert.ToDecimal(field.ConstantValue, CultureInfo.InvariantCulture)));
            }
        }

        return members;
    }

    // The first other member, in declaration order, that has the number too.
    private static string? Owner(
        List<(EnumMemberDeclarationSyntax Syntax, string Name, decimal Number)> members,
        EnumMemberDeclarationSyntax member,
        decimal number) =>
        members.FirstOrDefault(other => other.Syntax != member && other.Number == number).Name;

    private static decimal MaxOf(INamedTypeSymbol? underlying) => underlying?.SpecialType switch
    {
        SpecialType.System_SByte => sbyte.MaxValue,
        SpecialType.System_Byte => byte.MaxValue,
        SpecialType.System_Int16 => short.MaxValue,
        SpecialType.System_UInt16 => ushort.MaxValue,
        SpecialType.System_UInt32 => uint.MaxValue,
        SpecialType.System_Int64 => long.MaxValue,
        SpecialType.System_UInt64 => ulong.MaxValue,
        _ => int.MaxValue,
    };

    private static string Text(decimal number) => number.ToString(CultureInfo.InvariantCulture);

    private static void Report(SyntaxNodeAnalysisContext context, EnumMemberDeclarationSyntax member, string message) =>
        context.ReportDiagnostic(Diagnostic.Create(
            s_rule,
            member.Identifier.GetLocation(),
            message + ", so a member added before it cannot renumber it"));

    private static bool IsPublic(INamedTypeSymbol enumType)
    {
        if (enumType.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Protected
            or Accessibility.ProtectedOrInternal))
        {
            return false;
        }

        for (INamedTypeSymbol? type = enumType.ContainingType; type is not null; type = type.ContainingType)
        {
            if (type.DeclaredAccessibility != Accessibility.Public)
            {
                return false;
            }
        }

        return true;
    }
}
