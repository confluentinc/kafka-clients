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
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Reports a <c>TaskCompletionSource</c> or <c>TaskCompletionSource&lt;T&gt;</c> created without
/// <c>TaskCreationOptions.RunContinuationsAsynchronously</c> as a compile-time constant argument (CKRDA0001).
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
            + "completion. The option must be a compile-time constant argument, so the rule can see it.");

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
    }

    private static void AnalyzeCreation(
        OperationAnalysisContext context,
        ImmutableArray<INamedTypeSymbol> sources,
        INamedTypeSymbol options,
        long required)
    {
        IObjectCreationOperation creation = (IObjectCreationOperation)context.Operation;
        if (creation.Type is not INamedTypeSymbol type
            || !sources.Any(source => SymbolEqualityComparer.Default.Equals(source, type.OriginalDefinition)))
        {
            return;
        }

        foreach (IArgumentOperation argument in creation.Arguments)
        {
            if (SymbolEqualityComparer.Default.Equals(argument.Parameter?.Type, options)
                && argument.Value.ConstantValue is { HasValue: true, Value: { } value }
                && (Convert.ToInt64(value, CultureInfo.InvariantCulture) & required) != 0)
            {
                return;
            }
        }

        context.ReportDiagnostic(Diagnostic.Create(
            s_rule,
            creation.Syntax.GetLocation(),
            type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }
}
