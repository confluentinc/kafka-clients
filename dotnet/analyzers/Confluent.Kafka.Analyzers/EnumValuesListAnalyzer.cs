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

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks that the <c>s_values</c> or <c>VALUES</c> field of an <c>&lt;Enum&gt;Methods</c> class, Java's
/// <c>values()</c> or <c>VALUES</c> written out, lists every member of its enum once, as <c>Enum.MEMBER</c>, in
/// declaration order (CKRDA0015).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EnumValuesListAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.EnumValuesList,
        title: "An enum's values list holds every member once, in declaration order",
        messageFormat: "{0}",
        category: "Correctness",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Enum.GetValues(Type) is not Native AOT safe, so an <Enum>Methods class lists its enum's members "
            + "by hand. A member added to the enum and not to the list is missed by every lookup built from it. "
            + "Only a field named s_values or VALUES, of type Enum[] or IReadOnlyList<Enum>, in a class named "
            + "<Enum>Methods is checked.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeField, SyntaxKind.FieldDeclaration);
    }

    private static void AnalyzeField(SyntaxNodeAnalysisContext context)
    {
        foreach (VariableDeclaratorSyntax declarator in ((FieldDeclarationSyntax)context.Node).Declaration.Variables)
        {
            if (context.SemanticModel.GetDeclaredSymbol(declarator, context.CancellationToken) is IFieldSymbol field
                && ListedEnum(field) is INamedTypeSymbol enumType)
            {
                AnalyzeList(context, declarator, field, enumType);
            }
        }
    }

    private static INamedTypeSymbol? ListedEnum(IFieldSymbol field)
    {
        if (!field.IsStatic || (field.Name != "s_values" && field.Name != "VALUES"))
        {
            return null;
        }

        ITypeSymbol? element = field.Type switch
        {
            IArrayTypeSymbol { Rank: 1 } array => array.ElementType,
            INamedTypeSymbol { IsGenericType: true } named
                when named.ConstructedFrom.SpecialType == SpecialType.System_Collections_Generic_IReadOnlyList_T =>
                named.TypeArguments[0],
            _ => null,
        };
        return element is INamedTypeSymbol { TypeKind: TypeKind.Enum } enumType
            && field.ContainingType.Name == enumType.Name + "Methods"
                ? enumType
                : null;
    }

    private static void AnalyzeList(
        SyntaxNodeAnalysisContext context,
        VariableDeclaratorSyntax declarator,
        IFieldSymbol field,
        INamedTypeSymbol enumType)
    {
        string list = field.ContainingType.Name + "." + field.Name;
        IOperation? value = declarator.Initializer is null
            ? null
            : (context.SemanticModel.GetOperation(declarator.Initializer, context.CancellationToken)
                as IFieldInitializerOperation)?.Value;
        ImmutableArray<IOperation>? elements = value is null ? null : Elements(value);
        if (elements is null)
        {
            Report(
                context,
                declarator.Identifier.GetLocation(),
                $"{list} must list the members of {enumType.Name} written out, as {enumType.Name}.<NAME>");
            return;
        }

        List<IFieldSymbol> members = enumType.GetMembers().OfType<IFieldSymbol>()
            .Where(member => member.HasConstantValue)
            .ToList();
        var position = new Dictionary<IFieldSymbol, int>(SymbolEqualityComparer.Default);
        for (int i = 0; i < members.Count; i++)
        {
            position[members[i]] = i;
        }

        var listed = new HashSet<IFieldSymbol>(SymbolEqualityComparer.Default);
        IFieldSymbol? previous = null;
        foreach (IOperation element in elements.Value)
        {
            if (element is not IFieldReferenceOperation { Field: { HasConstantValue: true } member }
                || !SymbolEqualityComparer.Default.Equals(member.ContainingType, enumType))
            {
                Report(
                    context,
                    element.Syntax.GetLocation(),
                    $"write {enumType.Name}.<NAME> in {list}, not '{element.Syntax}'");
                continue;
            }

            if (!listed.Add(member))
            {
                Report(context, element.Syntax.GetLocation(), $"{list} lists {enumType.Name}.{member.Name} twice");
                continue;
            }

            if (previous is not null && position[member] < position[previous])
            {
                Report(
                    context,
                    element.Syntax.GetLocation(),
                    $"{list} lists {enumType.Name}.{previous.Name} before {enumType.Name}.{member.Name}; list the "
                        + "members in declaration order");
            }

            previous = member;
        }

        foreach (IFieldSymbol member in members.Where(member => !listed.Contains(member)))
        {
            Report(context, declarator.Identifier.GetLocation(), $"{list} does not list {enumType.Name}.{member.Name}");
        }
    }

    private static ImmutableArray<IOperation>? Elements(IOperation value)
    {
        value = WithoutImplicitConversions(value);
        if (value is IObjectCreationOperation { Arguments.Length: 1 } creation
            && creation.Type is INamedTypeSymbol { Name: "ReadOnlyCollection" } collection
            && collection.ContainingNamespace.ToDisplayString() == "System.Collections.ObjectModel")
        {
            value = WithoutImplicitConversions(creation.Arguments[0].Value);
        }

        return value switch
        {
            IArrayCreationOperation { Initializer: { } initializer } => initializer.ElementValues,
            ICollectionExpressionOperation expression => expression.Elements,
            _ => null,
        };
    }

    private static IOperation WithoutImplicitConversions(IOperation operation)
    {
        while (operation is IConversionOperation { IsImplicit: true } conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }

    private static void Report(SyntaxNodeAnalysisContext context, Location location, string message) =>
        context.ReportDiagnostic(Diagnostic.Create(s_rule, location, message));
}
