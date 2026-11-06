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
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks how the binding uses its <c>SafeHandle</c>s: a client handle reaches a P/Invoke as the <c>SafeHandle</c>, not
/// its raw pointer (CKRDA0005), and every <c>DangerousAddRef</c> is taken inside a <c>try</c> whose <c>finally</c>
/// block releases it (CKRDA0006).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SafeHandleUseAnalyzer : DiagnosticAnalyzer
{
    private const string ClientHandle = "Confluent.Kafka.Internal.Interop.SafeClientHandle";
    private const string ConsumerHandleHandle = "Confluent.Kafka.Internal.Interop.SafeConsumerReentrancyHandle";

    private static readonly DiagnosticDescriptor s_rawClientHandle = new DiagnosticDescriptor(
        DiagnosticIds.RawClientHandle,
        title: "A client handle is passed to a P/Invoke as its SafeHandle",
        messageFormat: "'{0}' is passed '{1}.DangerousGetHandle()' instead of the SafeHandle, so nothing keeps the handle alive during the call",
        category: "Reliability",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "An operation P/Invoke takes a client's SafeHandle, so the marshaller holds a reference for the "
            + "call and a closed handle throws ObjectDisposedException. ReleaseHandle is exempt.");

    private static readonly DiagnosticDescriptor s_addRefReleased = new DiagnosticDescriptor(
        DiagnosticIds.AddRefReleasedInFinally,
        title: "DangerousAddRef is taken inside a try whose finally block releases it",
        messageFormat: "'{0}.{1}' is not inside a try whose finally block calls '{0}.DangerousRelease()'",
        category: "Reliability",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A reference is call-scoped: it is taken inside a try statement, or one of its catch blocks, and "
            + "released in that try's finally block on the same thread, so no exception can leak it. A method named "
            + "like AddRef that takes a ref bool hands its reference to its caller, whose own release this rule checks.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(s_rawClientHandle, s_addRefReleased);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(OnCompilationStart);
    }

    private static void OnCompilationStart(CompilationStartAnalysisContext context)
    {
        INamedTypeSymbol? safeHandle = context.Compilation.GetTypeByMetadataName("System.Runtime.InteropServices.SafeHandle");
        if (safeHandle is null)
        {
            return;
        }

        ImmutableArray<INamedTypeSymbol> clientHandles = new[] { ClientHandle, ConsumerHandleHandle }
            .Select(context.Compilation.GetTypeByMetadataName)
            .Where(type => type is not null)
            .Select(type => type!)
            .ToImmutableArray();

        context.RegisterOperationAction(
            operationContext => AnalyzeInvocation(operationContext, safeHandle, clientHandles),
            OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(
        OperationAnalysisContext context,
        INamedTypeSymbol safeHandle,
        ImmutableArray<INamedTypeSymbol> clientHandles)
    {
        IInvocationOperation invocation = (IInvocationOperation)context.Operation;
        if (invocation.TargetMethod.GetDllImportData() is not null)
        {
            CheckArguments(context, invocation, clientHandles);
            return;
        }

        if (!IsAddRef(invocation.TargetMethod.Name)
            || IsAddRefWrapper(context.ContainingSymbol)
            || invocation.Syntax is not InvocationExpressionSyntax syntax)
        {
            return;
        }

        // On a SafeHandle the release names the same receiver. A wrapper of another type (TryAddRef) takes a
        // reference on a handle it holds, so any DangerousRelease in a finally block answers it.
        bool onSafeHandle = DerivesFrom(invocation.TargetMethod.ContainingType, safeHandle);
        if (!onSafeHandle && invocation.TargetMethod.Parameters.All(p => p.RefKind != RefKind.Ref || p.Type.SpecialType != SpecialType.System_Boolean))
        {
            return;
        }

        string receiver = Receiver(syntax.Expression);
        if (!IsReleasedInAFinally(syntax, onSafeHandle ? receiver : null))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                s_addRefReleased, syntax.GetLocation(), receiver.Length == 0 ? "this" : receiver, invocation.TargetMethod.Name));
        }
    }

    // DangerousAddRef, DangerousAddRefClass, TryAddRef: a method of this name hands the reference to its caller.
    private static bool IsAddRef(string name) => name.IndexOf("AddRef", StringComparison.Ordinal) >= 0;

    // A wrapper has the shape of DangerousAddRef: its name and a ref bool that tells its caller to release. A method
    // whose name only mentions AddRef, a test's, is checked like any other.
    private static bool IsAddRefWrapper(ISymbol symbol) =>
        symbol is IMethodSymbol method
        && IsAddRef(method.Name)
        && method.Parameters.Any(p => p.RefKind == RefKind.Ref && p.Type.SpecialType == SpecialType.System_Boolean);

    private static void CheckArguments(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        ImmutableArray<INamedTypeSymbol> clientHandles)
    {
        if (clientHandles.IsEmpty || context.ContainingSymbol.Name == "ReleaseHandle")
        {
            return;
        }

        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            IOperation value = argument.Value;
            while (value is IConversionOperation conversion)
            {
                value = conversion.Operand;
            }

            if (value is IInvocationOperation { TargetMethod.Name: "DangerousGetHandle", Instance: { } instance }
                && clientHandles.Any(handle => DerivesFrom(instance.Type, handle)))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    s_rawClientHandle, argument.Syntax.GetLocation(), invocation.TargetMethod.Name, instance.Syntax.ToString()));
            }
        }
    }

    // The AddRef sits in a try, or one of its catch blocks, whose finally block releases it, with no lambda or local
    // function between them: then the release runs whatever happens after the reference is taken.
    private static bool IsReleasedInAFinally(InvocationExpressionSyntax addRef, string? receiver)
    {
        foreach (SyntaxNode ancestor in addRef.Ancestors())
        {
            if (IsFunction(ancestor))
            {
                return false;
            }

            if (ancestor is TryStatementSyntax { Finally: { } @finally }
                && !@finally.Span.Contains(addRef.Span)
                && @finally.Block.DescendantNodes(node => !IsFunction(node))
                    .OfType<InvocationExpressionSyntax>()
                    .Any(release => Name(release.Expression) == "DangerousRelease"
                        && (receiver is null || Receiver(release.Expression) == receiver)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsFunction(SyntaxNode node) =>
        node is BaseMethodDeclarationSyntax
            or LocalFunctionStatementSyntax
            or AnonymousFunctionExpressionSyntax
            or AccessorDeclarationSyntax;

    private static string Name(ExpressionSyntax expression) => expression switch
    {
        MemberAccessExpressionSyntax member => member.Name.Identifier.ValueText,
        IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
        _ => string.Empty,
    };

    // The receiver as written, without a leading "this.": empty for an implicit this.
    private static string Receiver(ExpressionSyntax expression)
    {
        if (expression is not MemberAccessExpressionSyntax member || member.Expression is ThisExpressionSyntax)
        {
            return string.Empty;
        }

        string receiver = member.Expression.ToString();
        return receiver.StartsWith("this.", StringComparison.Ordinal) ? receiver.Substring("this.".Length) : receiver;
    }

    private static bool DerivesFrom(ITypeSymbol? type, INamedTypeSymbol baseType)
    {
        for (ITypeSymbol? current = type; current is not null; current = current.BaseType)
        {
            if (SymbolEqualityComparer.Default.Equals(current.OriginalDefinition, baseType))
            {
                return true;
            }
        }

        return false;
    }
}
