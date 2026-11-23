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
/// declaration order, and that each switch expression of the class that names a member of its enum, a lookup such
/// as <c>FromString</c> or <c>ForId</c>, gives every member an arm, or its default arm's answer, and no member two
/// arms (CKRDA0015).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EnumValuesListAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.EnumValuesList,
        title: "An enum's values list and lookups name every member once",
        messageFormat: "{0}",
        category: "Correctness",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Enum.GetValues(Type) is not Native AOT safe, so an <Enum>Methods class lists its enum's members "
            + "by hand. A member added to the enum and not to the list is missed by every lookup built from it. "
            + "Only a field named s_values or VALUES, of type Enum[] or IReadOnlyList<Enum>, in a class named "
            + "<Enum>Methods is checked. A switch expression of that class that names a member of the enum, a lookup "
            + "built by hand for the same reason, gives every member an arm (or names it as its default arm's answer, "
            + "as a FromString that answers UNKNOWN) and no member two arms; the order of its arms is free. A lookup "
            + "written another way (an if, a ternary, a range cast) is not checked.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeField, SyntaxKind.FieldDeclaration);
        context.RegisterSyntaxNodeAction(AnalyzeSwitch, SyntaxKind.SwitchExpression);
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

    // A switch expression of an <Enum>Methods class that names a member of the class's enum: each member has an arm, in
    // its pattern or its answer (UpgradeType.FromCode's 1 => UPGRADE, SslClientAuth.ToString's REQUIRED => "required"),
    // or is the default arm's answer (_ => UNKNOWN); no member has two arms.
    private static void AnalyzeSwitch(SyntaxNodeAnalysisContext context)
    {
        var expression = (SwitchExpressionSyntax)context.Node;
        if (expression.FirstAncestorOrSelf<ClassDeclarationSyntax>() is not { } declaration
            || context.SemanticModel.GetDeclaredSymbol(declaration, context.CancellationToken) is not { } methods
            || !methods.Name.EndsWith("Methods", System.StringComparison.Ordinal))
        {
            return;
        }

        var arms = new List<(SwitchExpressionArmSyntax Arm, HashSet<IFieldSymbol> Members)>();
        var byDefault = new HashSet<IFieldSymbol>(SymbolEqualityComparer.Default);
        INamedTypeSymbol? enumType = null;
        foreach (SwitchExpressionArmSyntax arm in expression.Arms)
        {
            bool isDefault = arm.Pattern is DiscardPatternSyntax && arm.WhenClause is null;
            var named = new HashSet<IFieldSymbol>(SymbolEqualityComparer.Default);
            SyntaxNode[] parts = isDefault
                ? new SyntaxNode[] { arm.Expression }
                : new SyntaxNode[] { arm.Pattern, arm.Expression };
            foreach (SyntaxNode part in parts)
            {
                foreach (IdentifierNameSyntax name in part.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>())
                {
                    if (context.SemanticModel.GetSymbolInfo(name, context.CancellationToken).Symbol is IFieldSymbol
                        {
                            HasConstantValue: true,
                            ContainingType: { TypeKind: TypeKind.Enum } owner,
                        } member
                        && methods.Name == owner.Name + "Methods")
                    {
                        enumType = owner;
                        named.Add(member);
                    }
                }
            }

            if (isDefault)
            {
                byDefault.UnionWith(named);
            }
            else
            {
                arms.Add((arm, named));
            }
        }

        if (enumType is null)
        {
            return;
        }

        string lookup = methods.Name + "." + (context.ContainingSymbol?.Name ?? "?");
        var armed = new HashSet<IFieldSymbol>(SymbolEqualityComparer.Default);
        foreach ((SwitchExpressionArmSyntax arm, HashSet<IFieldSymbol> named) in arms)
        {
            foreach (IFieldSymbol member in named.OrderBy(member => member.Name, System.StringComparer.Ordinal))
            {
                if (!armed.Add(member))
                {
                    Report(context, arm.GetLocation(), $"{lookup} gives {enumType.Name}.{member.Name} two arms");
                }
            }
        }

        IEnumerable<IFieldSymbol> members = enumType.GetMembers().OfType<IFieldSymbol>()
            .Where(member => member.HasConstantValue);
        foreach (IFieldSymbol member in members)
        {
            if (!armed.Contains(member) && !byDefault.Contains(member))
            {
                Report(
                    context,
                    expression.SwitchKeyword.GetLocation(),
                    $"{lookup} has no arm for {enumType.Name}.{member.Name}");
            }
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
