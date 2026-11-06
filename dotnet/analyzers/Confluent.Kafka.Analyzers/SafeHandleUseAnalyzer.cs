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
using System.Threading;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks how the binding uses its <c>SafeHandle</c>s: a client handle's raw pointer is taken only in
/// <c>ReleaseHandle</c> (CKRDA0005), and every <c>DangerousAddRef</c> is taken inside a <c>try</c> whose <c>finally</c>
/// block releases it when it succeeded (CKRDA0006).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SafeHandleUseAnalyzer : DiagnosticAnalyzer
{
    private const string ClientHandle = "Confluent.Kafka.Internal.Interop.SafeClientHandle";
    private const string ConsumerHandleHandle = "Confluent.Kafka.Internal.Interop.SafeConsumerReentrancyHandle";

    private static readonly DiagnosticDescriptor s_rawClientHandle = new DiagnosticDescriptor(
        DiagnosticIds.RawClientHandle,
        title: "A client handle's raw pointer is taken only in ReleaseHandle",
        messageFormat: "'{0}.DangerousGetHandle()' takes a client handle's raw pointer; pass the SafeHandle to a P/Invoke, so the marshaller keeps the handle alive during the call",
        category: "Reliability",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "An operation P/Invoke takes a client's SafeHandle, so the marshaller holds a reference for the "
            + "call and a closed handle throws ObjectDisposedException. The raw pointer is the only way around that, so "
            + "each DangerousGetHandle() of a client handle outside ReleaseHandle is reported, wherever its value goes; "
            + "a deliberate one carries a #pragma with its reason. Does not cover: the handle field inside a handle "
            + "class, and DangerousAddRefClass, which returns the class pointer with a reference held.");

    private static readonly DiagnosticDescriptor s_addRefReleased = new DiagnosticDescriptor(
        DiagnosticIds.AddRefReleasedInFinally,
        title: "DangerousAddRef is taken inside a try whose finally block releases it when it succeeded",
        messageFormat: "'{0}' is not inside a try whose finally block calls '{1}.DangerousRelease()' under 'if ({2})'",
        category: "Reliability",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A reference is call-scoped: it is taken inside a try statement, or one of its catch blocks, and "
            + "released in that try's finally block under 'if' on the flag the AddRef sets, on the same thread, with "
            + "nothing but releases of plain variables running before it. One flag and one release answer one AddRef, "
            + "outside any loop in the try. So no exception can leak it, and a failed AddRef is not released. A wrapper "
            + "(a method named like AddRef that takes a ref bool) hands the reference it takes with that flag to its "
            + "caller, whose release of the same handle, or of the argument passed for it, this rule checks. Does not "
            + "cover: a wrapper whose body is not in source, after which any release in the finally block counts. It "
            + "reports, although they are safe: a flag tested other than as 'if (flag)', a balanced AddRef in a loop, "
            + "and a release on another thread.");

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
        if (invocation.TargetMethod.Name == "DangerousGetHandle")
        {
            CheckRawPointer(context, invocation, clientHandles);
            return;
        }

        if (!IsAddRef(invocation.TargetMethod.Name)
            || ForwardsTheWrappersFlag(invocation, context.ContainingSymbol)
            || invocation.Syntax is not InvocationExpressionSyntax syntax)
        {
            return;
        }

        // On a SafeHandle the release names the same receiver. A wrapper (TryAddRef) takes its reference on a handle
        // its own body names.
        bool onSafeHandle = DerivesFrom(invocation.TargetMethod.ContainingType, safeHandle);
        if (!onSafeHandle && !IsAddRefWrapper(invocation.TargetMethod))
        {
            return;
        }

        string receiver = Receiver(syntax.Expression);
        string? released = onSafeHandle ? receiver : AcquiredBy(invocation, receiver, context.CancellationToken);
        string? flag = SuccessFlag(syntax);
        if (flag is null || !IsReleasedInAFinally(syntax, released, flag, invocation.SemanticModel))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                s_addRefReleased,
                syntax.GetLocation(),
                syntax.Expression.WithoutTrivia().ToString(),
                released is null ? "its handle" : released.Length == 0 ? "this" : released,
                flag ?? "success"));
        }
    }

    private static void CheckRawPointer(
        OperationAnalysisContext context,
        IInvocationOperation invocation,
        ImmutableArray<INamedTypeSymbol> clientHandles)
    {
        if (clientHandles.IsEmpty || context.ContainingSymbol.Name == "ReleaseHandle")
        {
            return;
        }

        IOperation? instance = invocation.Instance;
        if (instance?.Type is { } type && clientHandles.Any(handle => DerivesFrom(type, handle)))
        {
            context.ReportDiagnostic(Diagnostic.Create(
                s_rawClientHandle,
                invocation.Syntax.GetLocation(),
                instance.IsImplicit ? "this" : instance.Syntax.ToString()));
        }
    }

    // DangerousAddRef, DangerousAddRefClass, TryAddRef: a method of this name hands the reference to its caller.
    private static bool IsAddRef(string name) => name.IndexOf("AddRef", StringComparison.Ordinal) >= 0;

    // A wrapper has the shape of DangerousAddRef: its name and a ref bool that tells its caller to release. A method
    // whose name only mentions AddRef, a test's, is checked like any other.
    private static bool IsAddRefWrapper(ISymbol symbol) =>
        symbol is IMethodSymbol method
        && IsAddRef(method.Name)
        && method.Parameters.Any(IsRefBool);

    private static bool IsRefBool(IParameterSymbol parameter) =>
        parameter.RefKind == RefKind.Ref && parameter.Type.SpecialType == SpecialType.System_Boolean;

    // Inside a wrapper, an AddRef that sets the wrapper's own ref bool hands its reference to the caller, whose release
    // is checked there; any other AddRef in it, one that sets a local flag, is checked here.
    private static bool ForwardsTheWrappersFlag(IInvocationOperation addRef, ISymbol containing) =>
        IsAddRefWrapper(containing)
        && addRef.Arguments.Any(argument =>
            argument.Parameter is { RefKind: RefKind.Ref }
            && argument.Value is IParameterReferenceOperation flag
            && SymbolEqualityComparer.Default.Equals(flag.Parameter.ContainingSymbol, containing)
            && IsRefBool(flag.Parameter));

    // The handle a wrapper takes its reference on, as its body names it: the receiver of the AddRef that sets the
    // wrapper's own ref bool, after the receiver the wrapper is called on; a parameter of the wrapper is the argument
    // the call passes it. Null when the body is not in source.
    private static string? AcquiredBy(IInvocationOperation call, string callReceiver, CancellationToken cancellationToken)
    {
        IMethodSymbol wrapper = call.TargetMethod;
        IParameterSymbol? flag = wrapper.Parameters.FirstOrDefault(IsRefBool);
        if (flag is null)
        {
            return null;
        }

        foreach (SyntaxReference reference in wrapper.DeclaringSyntaxReferences)
        {
            foreach (InvocationExpressionSyntax addRef in reference.GetSyntax(cancellationToken).DescendantNodes().OfType<InvocationExpressionSyntax>())
            {
                if (IsAddRef(Name(addRef.Expression)) && SuccessFlag(addRef) == flag.Name)
                {
                    string acquired = Receiver(addRef.Expression);
                    if (acquired.Length > 0
                        && call.Arguments.FirstOrDefault(argument => argument.Parameter?.Name == acquired) is { } passed)
                    {
                        return WithoutThis(passed.Value.Syntax.WithoutTrivia().ToString());
                    }

                    return callReceiver.Length == 0 ? acquired
                        : acquired.Length == 0 ? callReceiver
                        : callReceiver + "." + acquired;
                }
            }
        }

        return null;
    }

    // The ref bool the AddRef sets when it took a reference, as written.
    private static string? SuccessFlag(InvocationExpressionSyntax addRef) =>
        addRef.ArgumentList.Arguments
            .FirstOrDefault(argument => argument.RefKindKeyword.IsKind(SyntaxKind.RefKeyword))
            ?.Expression is { } flag
            ? WithoutThis(flag.WithoutTrivia().ToString())
            : null;

    // The AddRef sits in a try, or one of its catch blocks, whose finally block releases it under 'if (flag)', with no
    // lambda or local function between them: then the release runs whatever happens after the reference is taken,
    // and only when it was taken.
    private static bool IsReleasedInAFinally(InvocationExpressionSyntax addRef, string? receiver, string flag, SemanticModel? model)
    {
        foreach (SyntaxNode ancestor in addRef.Ancestors())
        {
            if (IsFunction(ancestor))
            {
                return false;
            }

            if (ancestor is TryStatementSyntax { Finally: { } @finally } tryStatement
                && !@finally.Span.Contains(addRef.Span)
                && @finally.Block.DescendantNodes(node => !IsFunction(node))
                    .OfType<InvocationExpressionSyntax>()
                    .Any(release => Name(release.Expression) == "DangerousRelease"
                        && (receiver is null || Receiver(release.Expression) == receiver)
                        && IsGuardedBy(release, @finally.Block, flag, model)))
            {
                return IsTheOnlyAcquisition(addRef, tryStatement, flag);
            }
        }

        return false;
    }

    // One guarded release answers one acquisition: the AddRef is not in a loop inside the try, and no other AddRef in
    // the try sets the same flag.
    private static bool IsTheOnlyAcquisition(InvocationExpressionSyntax addRef, TryStatementSyntax tryStatement, string flag) =>
        !addRef.Ancestors().TakeWhile(node => node != tryStatement).Any(node =>
            node is ForStatementSyntax or ForEachStatementSyntax or ForEachVariableStatementSyntax
                or WhileStatementSyntax or DoStatementSyntax)
        && !tryStatement.Block.DescendantNodes(node => !IsFunction(node))
            .Concat(tryStatement.Catches.SelectMany(clause => clause.DescendantNodes(node => !IsFunction(node))))
            .OfType<InvocationExpressionSyntax>()
            .Any(other => other != addRef && IsAddRef(Name(other.Expression)) && SuccessFlag(other) == flag);

    // The release is a statement of its own, reached from the finally block only through blocks and 'if (flag)'
    // branches, at least one of them, with only other releases ahead of it in each block, so nothing that can throw
    // runs first. An else branch, another condition, a loop or a switch does not count.
    private static bool IsGuardedBy(InvocationExpressionSyntax release, BlockSyntax finallyBlock, string flag, SemanticModel? model)
    {
        if (release.Parent is not ExpressionStatementSyntax statement)
        {
            return false;
        }

        bool guarded = false;
        SyntaxNode child = statement;
        for (SyntaxNode? node = statement.Parent; node is not null; child = node, node = node.Parent)
        {
            switch (node)
            {
                case BlockSyntax block when block.Statements.TakeWhile(ahead => ahead != child).All(ahead => IsOnlyReleases(ahead, model)):
                    break;
                case IfStatementSyntax branch
                    when branch.Statement == child && WithoutThis(branch.Condition.WithoutTrivia().ToString()) == flag:
                    guarded = true;
                    break;
                default:
                    return false;
            }

            if (node == finallyBlock)
            {
                return guarded;
            }
        }

        return false;
    }

    // Releases whose receivers and conditions are plain variables, so evaluating them cannot throw.
    private static bool IsOnlyReleases(StatementSyntax statement, SemanticModel? model) => statement switch
    {
        ExpressionStatementSyntax { Expression: InvocationExpressionSyntax call } =>
            Name(call.Expression) == "DangerousRelease"
            && (call.Expression is not MemberAccessExpressionSyntax member || IsVariable(member.Expression, model)),
        IfStatementSyntax { Else: null } branch => IsVariable(branch.Condition, model) && IsOnlyReleases(branch.Statement, model),
        BlockSyntax block => block.Statements.All(inner => IsOnlyReleases(inner, model)),
        _ => false,
    };

    // A local, a parameter, a field or this: reading it runs no code that can throw. A property or a call does.
    private static bool IsVariable(ExpressionSyntax expression, SemanticModel? model) =>
        expression is ThisExpressionSyntax
        || (expression is IdentifierNameSyntax or MemberAccessExpressionSyntax { Expression: ThisExpressionSyntax }
            && model?.GetSymbolInfo(expression).Symbol is ILocalSymbol or IParameterSymbol or IFieldSymbol);

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

        return WithoutThis(member.Expression.ToString());
    }

    private static string WithoutThis(string expression) =>
        expression.StartsWith("this.", StringComparison.Ordinal) ? expression.Substring("this.".Length) : expression;

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
