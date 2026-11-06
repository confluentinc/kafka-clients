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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks the delegates native code calls back: each one reaches native code from a <c>static readonly</c> field
/// (CKRDA0003), and every method or lambda converted to a native callback type cannot throw into native code
/// (CKRDA0004).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NativeCallbackAnalyzer : DiagnosticAnalyzer
{
    private const int MaxForwardingDepth = 3;

    private const string MustWrap = "must wrap its whole body in a try with a catch-all clause";

    private const string Overridable = "is overridable, so native code can reach an override this rule cannot see";

    private static readonly DiagnosticDescriptor s_rootedDelegate = new DiagnosticDescriptor(
        DiagnosticIds.RootedCallbackDelegate,
        title: "A delegate passed to native code is a static readonly field",
        messageFormat: "The delegate passed to '{0}' for '{1}' is not a static readonly field, so the GC can collect it while native code still holds it",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The GC sees only managed references, not the native thunk of a delegate. A delegate handed to a "
            + "P/Invoke, or to any parameter of a native callback type, is a static readonly field, null (a Java "
            + "default) or a parameter of the same native callback type that the method never writes, whose callers "
            + "this rule checks. Does not cover: a delegate that reaches native code another way "
            + "(Marshal.GetFunctionPointerForDelegate, a struct field), and a parameter written through a ref alias.");

    // Reported when the compilation ends: a callback's body is judged from the bodies of the methods it calls, which
    // can sit in other files of a partial type.
    private static readonly DiagnosticDescriptor s_noThrowCallback = new DiagnosticDescriptor(
        DiagnosticIds.NoThrowNativeCallback,
        title: "A native callback does not throw into native code",
        messageFormat: "'{0}' is called from native code through '{1}' and {2}",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The frame below a native callback is Rust, so a managed exception that leaves the callback is "
            + "undefined behaviour. The body is empty; or one try statement with a catch clause for every exception, "
            + "whose catch and finally blocks only make calls; or one call. Each call is a non-virtual call of a "
            + "method of the same type whose body is one of these, and passes only parameters, locals, constants, "
            + "static fields of the BCL's primitive types and conversions of them that cannot throw. A callback is "
            + "not an overridable method. Does not cover: a delegate created by reflection or from a function pointer, "
            + "and what the runtime itself can raise anywhere (a stack overflow, an out-of-memory). It reports, "
            + "although they may be safe: forwarding more than three calls deep or to another type, and any other "
            + "argument or body shape.",
        customTags: WellKnownDiagnosticTags.CompilationEnd);

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

        context.RegisterOperationAction(
            operationContext => AnalyzeArguments(operationContext, unmanagedFunctionPointer),
            OperationKind.Invocation,
            OperationKind.ObjectCreation);
        if (unmanagedFunctionPointer is null)
        {
            return;
        }

        INamedTypeSymbol? exception = context.Compilation.GetTypeByMetadataName("System.Exception");
        ConcurrentDictionary<ISymbol, Body> bodies = new ConcurrentDictionary<ISymbol, Body>(SymbolEqualityComparer.Default);
        ConcurrentBag<Callback> callbacks = new ConcurrentBag<Callback>();

        context.RegisterOperationBlockAction(blockContext => RecordMethodBody(blockContext, exception, bodies));
        context.RegisterOperationAction(
            operationContext => RecordLocalFunctionBody(operationContext, exception, bodies),
            OperationKind.LocalFunction);
        context.RegisterOperationAction(
            operationContext => RecordCallback(operationContext, unmanagedFunctionPointer, exception, callbacks),
            OperationKind.DelegateCreation);
        context.RegisterCompilationEndAction(endContext => ReportThrowingCallbacks(endContext, bodies, callbacks));
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

            IOperation value = WithoutBuiltInConversions(argument.Value);
            if ((value.ConstantValue.HasValue && value.ConstantValue.Value is null)
                || value is IFieldReferenceOperation { Field: { IsStatic: true, IsReadOnly: true } }
                || (value is IParameterReferenceOperation forwarded
                    && IsNativeCallbackType(forwarded.Parameter.Type, unmanagedFunctionPointer)
                    && !IsWritten(forwarded.Parameter, context.Operation)))
            {
                continue;
            }

            context.ReportDiagnostic(Diagnostic.Create(
                s_rootedDelegate, argument.Syntax.GetLocation(), target.Name, argument.Parameter.Name));
        }
    }

    // A parameter is forwarded as its caller passed it only when nothing in the method writes it: an assignment, a
    // deconstruction, or a ref or out argument.
    private static bool IsWritten(IParameterSymbol parameter, IOperation use)
    {
        IOperation root = use;
        while (root.Parent is not null)
        {
            root = root.Parent;
        }

        return root.Descendants().OfType<IParameterReferenceOperation>().Any(reference =>
            SymbolEqualityComparer.Default.Equals(reference.Parameter, parameter)
            && (reference.Parent is IAssignmentOperation assignment && assignment.Target == reference
                || reference.Parent is ITupleOperation { Parent: IDeconstructionAssignmentOperation }
                || reference.Parent is IArgumentOperation { Parameter.RefKind: RefKind.Ref or RefKind.Out }));
    }

    private static void RecordMethodBody(
        OperationBlockAnalysisContext context,
        INamedTypeSymbol? exception,
        ConcurrentDictionary<ISymbol, Body> bodies)
    {
        if (context.OwningSymbol is not IMethodSymbol method)
        {
            return;
        }

        foreach (IOperation block in context.OperationBlocks)
        {
            IBlockOperation? body = block switch
            {
                IMethodBodyOperation methodBody => methodBody.BlockBody ?? methodBody.ExpressionBody,
                IBlockOperation plain => plain,
                _ => null,
            };
            if (body is not null)
            {
                bodies[method.OriginalDefinition] = Body.Of(body, method.ContainingType, exception);
            }
        }
    }

    private static void RecordLocalFunctionBody(
        OperationAnalysisContext context,
        INamedTypeSymbol? exception,
        ConcurrentDictionary<ISymbol, Body> bodies)
    {
        ILocalFunctionOperation function = (ILocalFunctionOperation)context.Operation;
        if ((function.Body ?? function.IgnoredBody) is { } body)
        {
            bodies[function.Symbol.OriginalDefinition] = Body.Of(body, function.Symbol.ContainingType, exception);
        }
    }

    // Every place a method or a lambda becomes a native callback: a field or property initializer, an assignment in a
    // constructor or anywhere else, a local, or an argument.
    private static void RecordCallback(
        OperationAnalysisContext context,
        INamedTypeSymbol unmanagedFunctionPointer,
        INamedTypeSymbol? exception,
        ConcurrentBag<Callback> callbacks)
    {
        IDelegateCreationOperation creation = (IDelegateCreationOperation)context.Operation;
        if (creation.Type is null || !IsNativeCallbackType(creation.Type, unmanagedFunctionPointer))
        {
            return;
        }

        switch (creation.Target)
        {
            case IMethodReferenceOperation reference:
                callbacks.Add(new Callback(
                    reference.Method.OriginalDefinition,
                    lambda: null,
                    reference.IsVirtual,
                    Through(creation),
                    creation.Syntax.GetLocation()));
                break;
            case IAnonymousFunctionOperation lambda:
                callbacks.Add(new Callback(
                    method: null,
                    Body.Of(lambda.Body, lambda.Symbol.ContainingType, exception),
                    overridable: false,
                    Through(creation),
                    lambda.Syntax.GetLocation()));
                break;
        }
    }

    private static void ReportThrowingCallbacks(
        CompilationAnalysisContext context,
        ConcurrentDictionary<ISymbol, Body> bodies,
        ConcurrentBag<Callback> callbacks)
    {
        // A method several callbacks point to is reported once, naming the first of them in source order.
        IEnumerable<IGrouping<ISymbol, Callback>> methods = callbacks
            .Where(callback => callback.Method is not null)
            .OrderBy(callback => callback.Creation.SourceTree?.FilePath, StringComparer.Ordinal)
            .ThenBy(callback => callback.Creation.SourceSpan.Start)
            .GroupBy(callback => callback.Method!, SymbolEqualityComparer.Default);
        foreach (IGrouping<ISymbol, Callback> method in methods)
        {
            // A virtual method group can dispatch to an override, whose body this rule never sees.
            Callback first = method.FirstOrDefault(callback => callback.Overridable) ?? method.First();
            string? reason = first.Overridable ? Overridable
                : bodies.TryGetValue(method.Key, out Body? body) && body.CannotThrow(bodies, MaxForwardingDepth) ? null
                : MustWrap;
            if (reason is null)
            {
                continue;
            }

            Location[] declarations = method.Key.Locations.Where(location => location.IsInSource).ToArray();
            foreach (Location location in declarations.Length > 0 ? declarations : new[] { first.Creation })
            {
                context.ReportDiagnostic(Diagnostic.Create(s_noThrowCallback, location, method.Key.Name, first.Through, reason));
            }
        }

        foreach (Callback lambda in callbacks.Where(callback => callback.Lambda is not null))
        {
            if (!lambda.Lambda!.CannotThrow(bodies, MaxForwardingDepth))
            {
                context.ReportDiagnostic(Diagnostic.Create(s_noThrowCallback, lambda.Creation, "The lambda", lambda.Through, MustWrap));
            }
        }
    }

    // The name the callback is stored or passed under: the field, property, local or parameter; else its type.
    private static string Through(IDelegateCreationOperation creation)
    {
        IOperation? parent = creation.Parent;
        while (parent is IConversionOperation)
        {
            parent = parent.Parent;
        }

        return parent switch
        {
            IFieldInitializerOperation initializer => initializer.InitializedFields[0].Name,
            IPropertyInitializerOperation initializer => initializer.InitializedProperties[0].Name,
            IVariableInitializerOperation { Parent: IVariableDeclaratorOperation declarator } => declarator.Symbol.Name,
            ISimpleAssignmentOperation { Target: IFieldReferenceOperation field } => field.Field.Name,
            ISimpleAssignmentOperation { Target: IPropertyReferenceOperation property } => property.Property.Name,
            ISimpleAssignmentOperation { Target: ILocalReferenceOperation local } => local.Local.Name,
            IArgumentOperation { Parameter: { } parameter } => parameter.Name,
            _ => creation.Type!.Name,
        };
    }

    private static bool IsNativeCallbackType(ITypeSymbol type, INamedTypeSymbol? unmanagedFunctionPointer) =>
        unmanagedFunctionPointer is not null
        && type.TypeKind == TypeKind.Delegate
        && type.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, unmanagedFunctionPointer));

    // A user-defined conversion stays: it can build a new delegate on each call, which nothing roots.
    private static IOperation WithoutBuiltInConversions(IOperation operation)
    {
        while (operation is IConversionOperation { Conversion.IsUserDefined: false } conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }

    /// <summary>A method or lambda converted to a native callback type, and the name it reaches native code under.</summary>
    private sealed class Callback
    {
        internal Callback(ISymbol? method, Body? lambda, bool overridable, string through, Location creation)
        {
            Method = method;
            Lambda = lambda;
            Overridable = overridable;
            Through = through;
            Creation = creation;
        }

        internal ISymbol? Method { get; }

        internal Body? Lambda { get; }

        internal bool Overridable { get; }

        internal string Through { get; }

        internal Location Creation { get; }
    }

    /// <summary>
    /// What a body does where an exception would leave it: nothing, or calls to methods of its own type whose own
    /// bodies decide; or something that can throw whatever it calls.
    /// </summary>
    private sealed class Body
    {
        private static readonly Body s_canThrow = new Body(null);

        private readonly ImmutableArray<ISymbol>? _calls;

        private Body(ImmutableArray<ISymbol>? calls) => _calls = calls;

        // The body is empty; or one catch-all try whose catch and finally blocks only make calls; or one call.
        internal static Body Of(IBlockOperation body, INamedTypeSymbol? owner, INamedTypeSymbol? exception)
        {
            IOperation[] statements = Statements(body);
            if (statements.Length == 0)
            {
                return new Body(ImmutableArray<ISymbol>.Empty);
            }

            if (owner is null || statements.Length != 1)
            {
                return s_canThrow;
            }

            if (statements[0] is not ITryOperation tryOperation)
            {
                return Call(statements[0], owner) is { } forwarded ? new Body(ImmutableArray.Create(forwarded)) : s_canThrow;
            }

            if (!tryOperation.Catches.Any(clause => IsCatchAll(clause, exception)))
            {
                return s_canThrow;
            }

            ImmutableArray<ISymbol>.Builder calls = ImmutableArray.CreateBuilder<ISymbol>();
            IEnumerable<IBlockOperation?> blocks = tryOperation.Catches.Select(clause => clause.Handler).Append(tryOperation.Finally);
            foreach (IOperation statement in blocks.Where(block => block is not null).SelectMany(block => Statements(block!)))
            {
                if (Call(statement, owner) is not { } call)
                {
                    return s_canThrow;
                }

                calls.Add(call);
            }

            return new Body(calls.ToImmutable());
        }

        internal bool CannotThrow(ConcurrentDictionary<ISymbol, Body> bodies, int depth) =>
            _calls is { } calls
            && (calls.IsEmpty
                || (depth > 0 && calls.All(call => bodies.TryGetValue(call, out Body? body) && body.CannotThrow(bodies, depth - 1))));

        private static IOperation[] Statements(IBlockOperation block) =>
            block.Operations.Where(operation => operation is not IEmptyOperation and not IReturnOperation { ReturnedValue: null }).ToArray();

        private static bool IsCatchAll(ICatchClauseOperation clause, INamedTypeSymbol? exception) =>
            clause.Filter is null
            && (clause.ExceptionType.SpecialType == SpecialType.System_Object
                || SymbolEqualityComparer.Default.Equals(clause.ExceptionType, exception));

        // The method a statement calls when the statement is only that call, a non-virtual call of a method of the
        // owner type on no receiver or this, with arguments that cannot throw; null otherwise. A virtual call can
        // reach an override whose body this rule never sees. The owner is compared as constructed: C<int> and
        // C<string> run separate static constructors.
        private static ISymbol? Call(IOperation statement, INamedTypeSymbol owner)
        {
            IOperation? expression = statement switch
            {
                IExpressionStatementOperation expressionStatement => expressionStatement.Operation,
                IReturnOperation returned => returned.ReturnedValue,
                _ => null,
            };
            while (expression is IConversionOperation conversion && ConversionCannotThrow(conversion))
            {
                expression = conversion.Operand;
            }

            if (expression is not IInvocationOperation call
                || call.IsVirtual
                || !SymbolEqualityComparer.Default.Equals(call.TargetMethod.ContainingType, owner)
                || call.Instance is not (null or IInstanceReferenceOperation)
                || !call.Arguments.All(argument => ArgumentCannotThrow(argument.Value)))
            {
                return null;
            }

            return call.TargetMethod.OriginalDefinition;
        }

        // Evaluating an argument runs before the called method's try begins, so it must run no code that can throw:
        // no property getter, call, user-defined conversion, cast that can fail, or checked narrowing. A static field's
        // first read runs its type's static constructor, which can throw; the BCL's primitive types (IntPtr.Zero,
        // string.Empty) have none that does.
        private static bool ArgumentCannotThrow(IOperation value) => value switch
        {
            { ConstantValue.HasValue: true } => true,
            IParameterReferenceOperation or ILocalReferenceOperation or IInstanceReferenceOperation or IDefaultValueOperation => true,
            IFieldReferenceOperation field => field.Field.IsStatic
                ? field.Field.ContainingType.SpecialType != SpecialType.None
                : field.Instance is IInstanceReferenceOperation,
            IConversionOperation conversion => ConversionCannotThrow(conversion) && ArgumentCannotThrow(conversion.Operand),
            _ => false,
        };

        private static bool ConversionCannotThrow(IConversionOperation conversion)
        {
            // (IntPtr)x from a 32-bit or smaller integer: a user-defined IntPtr operator before C# 11, which only
            // throws for a long that does not fit a 32-bit process.
            if (IsSmallIntegerToNativeInteger(conversion))
            {
                return true;
            }

            Conversion kind = conversion.GetConversion();
            if (kind.IsUserDefined)
            {
                return false;
            }

            // decimal's conversions check their range in any context: from a float or a double, and to an integral,
            // char or enum type, they can throw OverflowException.
            SpecialType from = conversion.Operand.Type?.SpecialType ?? SpecialType.None;
            SpecialType to = conversion.Type?.SpecialType ?? SpecialType.None;
            if ((kind.IsNumeric || kind.IsEnumeration)
                && ((from == SpecialType.System_Decimal && to is not (SpecialType.System_Single or SpecialType.System_Double))
                    || (to == SpecialType.System_Decimal && from is SpecialType.System_Single or SpecialType.System_Double)))
            {
                return false;
            }

            return kind.IsImplicit || ((kind.IsNumeric || kind.IsEnumeration) && !conversion.IsChecked);
        }

        private static bool IsSmallIntegerToNativeInteger(IConversionOperation conversion) =>
            (conversion.Type?.SpecialType, conversion.Operand.Type?.SpecialType) switch
            {
                (SpecialType.System_IntPtr, SpecialType.System_SByte or SpecialType.System_Byte or SpecialType.System_Int16
                    or SpecialType.System_UInt16 or SpecialType.System_Char or SpecialType.System_Int32) => true,
                (SpecialType.System_UIntPtr, SpecialType.System_Byte or SpecialType.System_UInt16 or SpecialType.System_Char
                    or SpecialType.System_UInt32) => true,
                _ => false,
            };
    }
}
