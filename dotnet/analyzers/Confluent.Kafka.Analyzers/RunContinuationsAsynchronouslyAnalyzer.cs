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
using System.Collections.Immutable;
using System.Globalization;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;
using Microsoft.CodeAnalysis.Text;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Reports a <c>TaskCompletionSource</c> or <c>TaskCompletionSource&lt;T&gt;</c> created without
/// <c>TaskCreationOptions.RunContinuationsAsynchronously</c> as a compile-time constant argument, by a <c>new</c> or by
/// the base call of a class derived from one (CKRDA0001).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class RunContinuationsAsynchronouslyAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.RunContinuationsAsynchronously,
        title: "Create a TaskCompletionSource with TaskCreationOptions.RunContinuationsAsynchronously",
        messageFormat: "'{0}' is created without TaskCreationOptions.RunContinuationsAsynchronously, so a continuation can run inline on the thread that completes it",
        category: "Reliability",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The binding completes its TaskCompletionSources on a client's drain thread. Without "
            + "RunContinuationsAsynchronously an awaiter's continuation runs on that thread and stalls every later "
            + "completion. The option must be a compile-time constant argument, so the rule can see it. A class derived "
            + "from one passes it in each constructor's base call; a constructor without one, a primary constructor "
            + "included, calls base() without it.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(OnCompilationStart);
    }

    private static void OnCompilationStart(CompilationStartAnalysisContext context)
    {
        INamedTypeSymbol? options = context.Compilation.GetTypeByMetadataName("System.Threading.Tasks.TaskCreationOptions");
        IFieldSymbol? flag = options?.GetMembers("RunContinuationsAsynchronously").OfType<IFieldSymbol>().FirstOrDefault();
        if (options is null || flag is null || !flag.HasConstantValue || flag.ConstantValue is null)
        {
            return;
        }

        long required = Convert.ToInt64(flag.ConstantValue, CultureInfo.InvariantCulture);
        ImmutableArray<INamedTypeSymbol> sources = new[]
            {
                "System.Threading.Tasks.TaskCompletionSource`1",
                "System.Threading.Tasks.TaskCompletionSource",
            }
            .Select(context.Compilation.GetTypeByMetadataName)
            .Where(type => type is not null)
            .Select(type => type!)
            .ToImmutableArray();
        if (sources.IsEmpty)
        {
            return;
        }

        context.RegisterOperationAction(
            operationContext => AnalyzeCreation(operationContext, sources, options, required),
            OperationKind.ObjectCreation);
        context.RegisterOperationAction(
            operationContext => AnalyzeBaseCall(operationContext, sources, options, required),
            OperationKind.Invocation);
        context.RegisterSymbolAction(symbolContext => AnalyzeDerivedSource(symbolContext, sources), SymbolKind.NamedType);
    }

    private static void AnalyzeCreation(
        OperationAnalysisContext context,
        ImmutableArray<INamedTypeSymbol> sources,
        INamedTypeSymbol options,
        long required)
    {
        IObjectCreationOperation creation = (IObjectCreationOperation)context.Operation;
        if (creation.Type is not INamedTypeSymbol type || !IsSource(type, sources) || HasOption(creation.Arguments, options, required))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            s_rule,
            creation.Syntax.GetLocation(),
            type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    // A derived class's ': base(...)' call. An implicit base() call is left to AnalyzeDerivedSource.
    private static void AnalyzeBaseCall(
        OperationAnalysisContext context,
        ImmutableArray<INamedTypeSymbol> sources,
        INamedTypeSymbol options,
        long required)
    {
        IInvocationOperation call = (IInvocationOperation)context.Operation;
        if (call.IsImplicit
            || call.TargetMethod.MethodKind != MethodKind.Constructor
            || call.Instance is not IInstanceReferenceOperation
            || !IsSource(call.TargetMethod.ContainingType, sources)
            || HasOption(call.Arguments, options, required))
        {
            return;
        }

        Location location = call.Syntax is ConstructorInitializerSyntax initializer
            ? Location.Create(initializer.SyntaxTree, TextSpan.FromBounds(initializer.ThisOrBaseKeyword.SpanStart, initializer.Span.End))
            : call.Syntax.GetLocation();
        context.ReportDiagnostic(Diagnostic.Create(
            s_rule,
            location,
            call.TargetMethod.ContainingType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    // A class derived from a TaskCompletionSource whose constructor calls base() without the option: one with no base
    // or this call, a primary constructor whose base type is written without arguments, or the default constructor of
    // a class that declares none. A primary constructor's written base arguments are AnalyzeBaseCall's.
    private static void AnalyzeDerivedSource(SymbolAnalysisContext context, ImmutableArray<INamedTypeSymbol> sources)
    {
        INamedTypeSymbol type = (INamedTypeSymbol)context.Symbol;
        if (type.BaseType is not { } baseType || !IsSource(baseType, sources))
        {
            return;
        }

        string display = baseType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);
        foreach (IMethodSymbol constructor in type.InstanceConstructors)
        {
            if (constructor.IsImplicitlyDeclared)
            {
                context.ReportDiagnostic(Diagnostic.Create(s_rule, type.Locations.FirstOrDefault() ?? Location.None, display));
                continue;
            }

            foreach (SyntaxReference reference in constructor.DeclaringSyntaxReferences)
            {
                SyntaxToken? implicitBaseCall = reference.GetSyntax(context.CancellationToken) switch
                {
                    ConstructorDeclarationSyntax { Initializer: null } declaration => declaration.Identifier,
                    TypeDeclarationSyntax { ParameterList: not null } primary
                        when primary.BaseList?.Types.FirstOrDefault() is not PrimaryConstructorBaseTypeSyntax => primary.Identifier,
                    _ => null,
                };
                if (implicitBaseCall is { } identifier)
                {
                    context.ReportDiagnostic(Diagnostic.Create(s_rule, identifier.GetLocation(), display));
                }
            }
        }
    }

    private static bool IsSource(INamedTypeSymbol type, ImmutableArray<INamedTypeSymbol> sources) =>
        sources.Any(source => SymbolEqualityComparer.Default.Equals(source, type.OriginalDefinition));

    private static bool HasOption(ImmutableArray<IArgumentOperation> arguments, INamedTypeSymbol options, long required) =>
        arguments.Any(argument =>
            SymbolEqualityComparer.Default.Equals(argument.Parameter?.Type, options)
            && argument.Value.ConstantValue is { HasValue: true, Value: { } value }
            && (Convert.ToInt64(value, CultureInfo.InvariantCulture) & required) != 0);
}
