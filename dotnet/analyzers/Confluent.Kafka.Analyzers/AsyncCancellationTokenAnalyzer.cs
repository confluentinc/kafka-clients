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
/// Checks that a public method returning a <c>Task</c>, <c>Task&lt;T&gt;</c>, <c>ValueTask</c> or
/// <c>ValueTask&lt;T&gt;</c> ends with <c>CancellationToken cancellationToken = default</c> (CKRDA0016).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class AsyncCancellationTokenAnalyzer : DiagnosticAnalyzer
{
    private const string CancellationTokenType = "System.Threading.CancellationToken";

    private const string ParameterName = "cancellationToken";

    private static readonly DiagnosticDescriptor s_rule = new DiagnosticDescriptor(
        DiagnosticIds.AsyncCancellationToken,
        title: "An async method ends with CancellationToken cancellationToken = default",
        messageFormat: "{0}",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Every async method takes a CancellationToken (dotnet/CLAUDE.md §4 Cancellation); adding one "
            + "later changes the signature, which breaks every compiled caller. Not checked: a callback the client "
            + "calls (a method named On…), the futures KafkaFuture and AsyncKafkaFuture, a member that implements "
            + "or overrides one declared outside this assembly (DisposeAsync), and a member that is not public.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(s_rule);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeMethod, SymbolKind.Method);
    }

    private static void AnalyzeMethod(SymbolAnalysisContext context)
    {
        IMethodSymbol method = (IMethodSymbol)context.Symbol;
        if (method.MethodKind != MethodKind.Ordinary
            || method.IsImplicitlyDeclared
            || !IsPublic(method)
            || TaskKind(method.ReturnType) is not string task
            || IsCallback(method)
            || IsFuture(method.ContainingType)
            || ComesFromOutside(method))
        {
            return;
        }

        string name = method.ContainingType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat) + "."
            + method.Name;
        ImmutableArray<IParameterSymbol> parameters = method.Parameters;
        IParameterSymbol? last = parameters.Length == 0 ? null : parameters[parameters.Length - 1];
        if (last is null || !IsCancellationToken(last.Type))
        {
            Report(
                context,
                method,
                parameters.Any(parameter => IsCancellationToken(parameter.Type))
                    ? $"{name}: the CancellationToken must be the last parameter"
                    : $"{name} returns a {task}; end it with CancellationToken cancellationToken = default");
            return;
        }

        if (last.Name != ParameterName)
        {
            Report(context, method, $"{name}: name the CancellationToken cancellationToken");
        }

        if (!last.IsOptional)
        {
            Report(
                context,
                method,
                $"{name}: the CancellationToken must be optional: CancellationToken cancellationToken = default");
        }
    }

    private static bool IsPublic(IMethodSymbol method)
    {
        if (method.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Protected
            or Accessibility.ProtectedOrInternal))
        {
            return false;
        }

        for (INamedTypeSymbol? type = method.ContainingType; type is not null; type = type.ContainingType)
        {
            if (!type.IsExtension && type.DeclaredAccessibility != Accessibility.Public)
            {
                return false;
            }
        }

        return true;
    }

    private static string? TaskKind(ITypeSymbol type) =>
        type is INamedTypeSymbol { Name: "Task" or "ValueTask", Arity: 0 or 1 } named
            && named.ContainingNamespace.ToDisplayString() == "System.Threading.Tasks"
                ? named.Name
                : null;

    // Java's callbacks are all on…: onCompletion, onComplete, onPartitionsRevoked. The user implements them and the
    // client calls them, so a default value there means nothing.
    private static bool IsCallback(IMethodSymbol method) =>
        method.Name.Length > 2 && method.Name.StartsWith("On", System.StringComparison.Ordinal)
            && char.IsUpper(method.Name[2]);

    // Java's get() and get(timeout), with the binding's Get(CancellationToken) beside them, and ToCompletionStage(),
    // which starts nothing (dotnet/CLAUDE.md §4 Futures).
    private static bool IsFuture(INamedTypeSymbol type) =>
        type.Name is "KafkaFuture" or "AsyncKafkaFuture"
            && type.ContainingNamespace.ToDisplayString() == "Confluent.Kafka.Common";

    private static bool ComesFromOutside(IMethodSymbol method)
    {
        for (IMethodSymbol? overridden = method.OverriddenMethod; overridden is not null;
            overridden = overridden.OverriddenMethod)
        {
            if (!SymbolEqualityComparer.Default.Equals(overridden.ContainingAssembly, method.ContainingAssembly))
            {
                return true;
            }
        }

        return method.ContainingType.AllInterfaces
            .Where(contract => !SymbolEqualityComparer.Default.Equals(
                contract.ContainingAssembly, method.ContainingAssembly))
            .SelectMany(contract => contract.GetMembers().OfType<IMethodSymbol>())
            .Any(member => SymbolEqualityComparer.Default.Equals(
                method.ContainingType.FindImplementationForInterfaceMember(member), method));
    }

    private static bool IsCancellationToken(ITypeSymbol type) => type.ToDisplayString() == CancellationTokenType;

    private static void Report(SymbolAnalysisContext context, IMethodSymbol method, string message)
    {
        Location? location = method.Locations.FirstOrDefault(candidate => candidate.IsInSource);
        if (location is not null)
        {
            context.ReportDiagnostic(Diagnostic.Create(s_rule, location, message));
        }
    }
}
