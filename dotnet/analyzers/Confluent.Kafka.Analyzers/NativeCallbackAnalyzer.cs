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

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks the delegates native code calls back: each one reaches native code from a <c>static readonly</c> field
/// (CKRDA0003), and the method a native callback field points to cannot throw into native code (CKRDA0004).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NativeCallbackAnalyzer : DiagnosticAnalyzer
{
    private const int MaxForwardingDepth = 3;

    private static readonly DiagnosticDescriptor s_rootedDelegate = new DiagnosticDescriptor(
        DiagnosticIds.RootedCallbackDelegate,
        title: "A delegate passed to native code is a static readonly field",
        messageFormat: "The delegate passed to '{0}' for '{1}' is not a static readonly field, so the GC can collect it while native code still holds it",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The GC sees only managed references, not the native thunk of a delegate. A delegate handed to a "
            + "P/Invoke, or to any parameter of a native callback type, is a static readonly field, null (a Java "
            + "default) or a parameter of the same native callback type forwarded as it is, whose callers this rule checks.");

    private static readonly DiagnosticDescriptor s_noThrowCallback = new DiagnosticDescriptor(
        DiagnosticIds.NoThrowNativeCallback,
        title: "A native callback does not throw into native code",
        messageFormat: "'{0}' is called from native code through '{1}' and must wrap its whole body in a try with a catch-all clause",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The frame below a native callback is Rust, so a managed exception that leaves the callback is "
            + "undefined behaviour. The body is one try statement with a catch clause for every exception, or one call "
            + "that passes parameters, constants or casts of them to a method of the same type whose body is.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(s_rootedDelegate, s_noThrowCallback);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(OnCompilationStart);
    }

    private static void OnCompilationStart(CompilationStartAnalysisContext context)
    {
        INamedTypeSymbol? unmanagedFunctionPointer =
            context.Compilation.GetTypeByMetadataName("System.Runtime.InteropServices.UnmanagedFunctionPointerAttribute");
        ConcurrentDictionary<ISymbol, bool> checkedMethods = new ConcurrentDictionary<ISymbol, bool>(SymbolEqualityComparer.Default);

        context.RegisterOperationAction(
            operationContext => AnalyzeArguments(operationContext, unmanagedFunctionPointer),
            OperationKind.Invocation,
            OperationKind.ObjectCreation);
        if (unmanagedFunctionPointer is not null)
        {
            context.RegisterOperationAction(
                operationContext => AnalyzeFieldInitializer(operationContext, unmanagedFunctionPointer, checkedMethods),
                OperationKind.FieldInitializer);
        }
    }

    private static void AnalyzeArguments(OperationAnalysisContext context, INamedTypeSymbol? unmanagedFunctionPointer)
    {
        (IMethodSymbol? target, ImmutableArray<IArgumentOperation> arguments) = context.Operation switch
        {
            IInvocationOperation invocation => (invocation.TargetMethod, invocation.Arguments),
            IObjectCreationOperation creation => (creation.Constructor, creation.Arguments),
            _ => (null, ImmutableArray<IArgumentOperation>.Empty),
        };
        if (target is null)
        {
            return;
        }

        bool toNative = target.GetDllImportData() is not null;
        foreach (IArgumentOperation argument in arguments)
        {
            if (argument.Parameter is null
                || argument.Parameter.Type.TypeKind != TypeKind.Delegate
                || !(toNative || IsNativeCallbackType(argument.Parameter.Type, unmanagedFunctionPointer)))
            {
                continue;
            }

            IOperation value = WithoutConversions(argument.Value);
            if ((value.ConstantValue.HasValue && value.ConstantValue.Value is null)
                || value is IFieldReferenceOperation { Field: { IsStatic: true, IsReadOnly: true } }
                || (value is IParameterReferenceOperation forwarded
                    && IsNativeCallbackType(forwarded.Parameter.Type, unmanagedFunctionPointer)))
            {
                continue;
            }

            context.ReportDiagnostic(Diagnostic.Create(
                s_rootedDelegate, argument.Syntax.GetLocation(), target.Name, argument.Parameter.Name));
        }
    }

    private static void AnalyzeFieldInitializer(
        OperationAnalysisContext context,
        INamedTypeSymbol unmanagedFunctionPointer,
        ConcurrentDictionary<ISymbol, bool> checkedMethods)
    {
        IFieldInitializerOperation initializer = (IFieldInitializerOperation)context.Operation;
        foreach (IFieldSymbol field in initializer.InitializedFields)
        {
            if (!IsNativeCallbackType(field.Type, unmanagedFunctionPointer)
                || WithoutConversions(initializer.Value) is not IDelegateCreationOperation creation)
            {
                continue;
            }

            switch (creation.Target)
            {
                case IMethodReferenceOperation reference when checkedMethods.TryAdd(reference.Method, true):
                    foreach (SyntaxReference declaration in reference.Method.DeclaringSyntaxReferences)
                    {
                        if (Declaration(declaration.GetSyntax(context.CancellationToken)) is { } method
                            && !CannotThrow(method.Body, method.Expression, reference.Method.ContainingType, MaxForwardingDepth, context))
                        {
                            context.ReportDiagnostic(Diagnostic.Create(
                                s_noThrowCallback, method.Name.GetLocation(), reference.Method.Name, field.Name));
                        }
                    }

                    break;
                case IAnonymousFunctionOperation lambda when lambda.Syntax is AnonymousFunctionExpressionSyntax syntax:
                    if (!CannotThrow(syntax.Block, syntax.ExpressionBody, field.ContainingType, MaxForwardingDepth, context))
                    {
                        context.ReportDiagnostic(Diagnostic.Create(s_noThrowCallback, syntax.GetLocation(), "The lambda", field.Name));
                    }

                    break;
            }
        }
    }

    private static (BlockSyntax? Body, ExpressionSyntax? Expression, SyntaxToken Name)? Declaration(SyntaxNode node) => node switch
    {
        MethodDeclarationSyntax method => (method.Body, method.ExpressionBody?.Expression, method.Identifier),
        LocalFunctionStatementSyntax function => (function.Body, function.ExpressionBody?.Expression, function.Identifier),
        _ => null,
    };

    // The body is a catch-all try, or one call that passes only parameters, constants and casts of them to a method
    // of the same type whose every overload of that name cannot throw either.
    private static bool CannotThrow(
        BlockSyntax? body,
        ExpressionSyntax? expressionBody,
        INamedTypeSymbol? containingType,
        int depth,
        OperationAnalysisContext context)
    {
        if (body is not null && body.Statements.Count == 1 && body.Statements[0] is TryStatementSyntax tryStatement)
        {
            return tryStatement.Catches.Any(IsCatchAll);
        }

        ExpressionSyntax? forwarded = expressionBody ?? (body is { Statements.Count: 1 } ? body.Statements[0] switch
        {
            ExpressionStatementSyntax statement => statement.Expression,
            ReturnStatementSyntax returned => returned.Expression,
            _ => null,
        } : null);

        if (depth == 0
            || containingType is null
            || forwarded is not InvocationExpressionSyntax call
            || call.Expression is not IdentifierNameSyntax callee
            || !call.ArgumentList.Arguments.All(argument => IsPlain(argument.Expression)))
        {
            return false;
        }

        IMethodSymbol[] helpers = containingType.GetMembers(callee.Identifier.ValueText).OfType<IMethodSymbol>().ToArray();
        return helpers.Length > 0 && helpers.All(helper => helper.DeclaringSyntaxReferences.All(reference =>
            Declaration(reference.GetSyntax(context.CancellationToken)) is { } helperDeclaration
            && CannotThrow(helperDeclaration.Body, helperDeclaration.Expression, containingType, depth - 1, context)));
    }

    private static bool IsPlain(ExpressionSyntax expression) => expression switch
    {
        IdentifierNameSyntax or LiteralExpressionSyntax or DefaultExpressionSyntax => true,
        MemberAccessExpressionSyntax member => IsPlain(member.Expression),
        CastExpressionSyntax cast => IsPlain(cast.Expression),
        ParenthesizedExpressionSyntax parenthesized => IsPlain(parenthesized.Expression),
        _ => false,
    };

    private static bool IsCatchAll(CatchClauseSyntax clause)
    {
        if (clause.Filter is not null)
        {
            return false;
        }

        if (clause.Declaration is null)
        {
            return true;
        }

        string type = clause.Declaration.Type.ToString();
        return type is "Exception" or "System.Exception" or "global::System.Exception";
    }

    private static bool IsNativeCallbackType(ITypeSymbol type, INamedTypeSymbol? unmanagedFunctionPointer) =>
        unmanagedFunctionPointer is not null
        && type.TypeKind == TypeKind.Delegate
        && type.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, unmanagedFunctionPointer));

    private static IOperation WithoutConversions(IOperation operation)
    {
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }
}
