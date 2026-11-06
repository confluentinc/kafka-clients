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
using System.Runtime.InteropServices;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// Checks each <c>[DllImport]</c> against the C ABI type map: the library and calling convention (CKRDA0007), Cdecl
/// callback delegate types (CKRDA0002), one-byte bools (CKRDA0008), no managed strings (CKRDA0009) and no
/// native-sized integers (CKRDA0010).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PInvokeSignatureAnalyzer : DiagnosticAnalyzer
{
    private const string LibraryName = "confluent_kafka";

    private static readonly DiagnosticDescriptor s_cdeclDelegate = new DiagnosticDescriptor(
        DiagnosticIds.CdeclCallbackDelegate,
        title: "A delegate type passed to native code is Cdecl",
        messageFormat: "'{0}' is passed to native code by '{1}' but has no [UnmanagedFunctionPointer(CallingConvention.Cdecl)]",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The C ABI's callbacks are Cdecl function pointers. Without the attribute a delegate is marshalled "
            + "with the platform's default convention, which is not Cdecl on x86.");

    private static readonly DiagnosticDescriptor s_libraryAndConvention = new DiagnosticDescriptor(
        DiagnosticIds.LibraryAndCallingConvention,
        title: "A P/Invoke imports confluent_kafka with CallingConvention.Cdecl",
        messageFormat: "'{0}' imports '{1}' with CallingConvention.{2}; a P/Invoke imports 'confluent_kafka' with CallingConvention.Cdecl",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The Rust core's exports are extern \"C\" functions of the confluent_kafka library. Another "
            + "calling convention works on x64 by luck and breaks on x86.");

    private static readonly DiagnosticDescriptor s_oneByteBool = new DiagnosticDescriptor(
        DiagnosticIds.OneByteBool,
        title: "A bool crossing a P/Invoke is marshalled as one byte",
        messageFormat: "{0} of '{1}' is a {2} without {3}; C's bool is one byte",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "By default a bool, and each element of a bool[], is marshalled as a four-byte Win32 BOOL, so it "
            + "reads three bytes too many.");

    private static readonly DiagnosticDescriptor s_noManagedString = new DiagnosticDescriptor(
        DiagnosticIds.NoManagedString,
        title: "A string crosses a P/Invoke as hand-marshalled UTF-8",
        messageFormat: "{0} of '{1}' is a {2}; pass an IntPtr to a NUL-terminated UTF-8 buffer (Utf8Marshal) instead",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The marshaller's string forms are ANSI or UTF-16 on netstandard2.0, and a returned string would "
            + "be freed by the marshaller although the core owns it.");

    private static readonly DiagnosticDescriptor s_noNativeSizedInteger = new DiagnosticDescriptor(
        DiagnosticIds.NoNativeSizedInteger,
        title: "A P/Invoke size is an int or a long",
        messageFormat: "{0} of '{1}' is a {2}; the C ABI has no size_t, so a size is an int or a long and a pointer an IntPtr",
        category: "Interop",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The C ABI's integers are fixed-width int32_t and int64_t.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(
        s_cdeclDelegate, s_libraryAndConvention, s_oneByteBool, s_noManagedString, s_noNativeSizedInteger);

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(OnCompilationStart);
    }

    private static void OnCompilationStart(CompilationStartAnalysisContext context)
    {
        Types types = new Types(
            context.Compilation.GetTypeByMetadataName("System.Runtime.InteropServices.MarshalAsAttribute"),
            context.Compilation.GetTypeByMetadataName("System.Runtime.InteropServices.UnmanagedFunctionPointerAttribute"),
            context.Compilation.GetTypeByMetadataName("System.Text.StringBuilder"));
        context.RegisterSymbolAction(symbolContext => AnalyzeMethod(symbolContext, types), SymbolKind.Method);
    }

    private static void AnalyzeMethod(SymbolAnalysisContext context, Types types)
    {
        IMethodSymbol method = (IMethodSymbol)context.Symbol;
        DllImportData? import = method.GetDllImportData();
        if (import is null)
        {
            return;
        }

        Location declaration = method.Locations.FirstOrDefault() ?? Location.None;
        if (import.ModuleName != LibraryName || import.CallingConvention != CallingConvention.Cdecl)
        {
            context.ReportDiagnostic(Diagnostic.Create(
                s_libraryAndConvention, declaration, method.Name, import.ModuleName ?? string.Empty, import.CallingConvention));
        }

        if (!method.ReturnsVoid)
        {
            TypeSyntax? returnType = method.DeclaringSyntaxReferences
                .Select(reference => reference.GetSyntax(context.CancellationToken))
                .OfType<MethodDeclarationSyntax>()
                .FirstOrDefault()?.ReturnType;
            CheckType(context, types, method.ReturnType, returnType, method.GetReturnTypeAttributes(), declaration, "The return value", method.Name);
        }

        foreach (IParameterSymbol parameter in method.Parameters)
        {
            Location location = parameter.Locations.FirstOrDefault() ?? declaration;
            string what = "Parameter '" + parameter.Name + "'";
            TypeSyntax? written = parameter.DeclaringSyntaxReferences
                .Select(reference => reference.GetSyntax(context.CancellationToken))
                .OfType<ParameterSyntax>()
                .FirstOrDefault()?.Type;
            CheckType(context, types, parameter.Type, written, parameter.GetAttributes(), location, what, method.Name);
            if (parameter.Type.TypeKind == TypeKind.Delegate && !IsCdecl(parameter.Type, types.UnmanagedFunctionPointer))
            {
                context.ReportDiagnostic(Diagnostic.Create(s_cdeclDelegate, location, parameter.Type.Name, method.Name));
            }
        }
    }

    private static void CheckType(
        SymbolAnalysisContext context,
        Types types,
        ITypeSymbol type,
        TypeSyntax? written,
        ImmutableArray<AttributeData> attributes,
        Location location,
        string what,
        string method)
    {
        ITypeSymbol element = type is IArrayTypeSymbol array ? array.ElementType : type;
        string display = element.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat);

        if (element.SpecialType == SpecialType.System_Boolean)
        {
            bool isArray = type is IArrayTypeSymbol;
            if (isArray ? !IsOneByteArray(attributes, types.MarshalAs) : !IsOneByte(attributes, types.MarshalAs))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    s_oneByteBool,
                    location,
                    what,
                    method,
                    isArray ? "bool[]" : "bool",
                    isArray ? "[MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)]" : "[MarshalAs(UnmanagedType.I1)]"));
            }
        }

        if (element.SpecialType == SpecialType.System_String
            || (types.StringBuilder is not null && SymbolEqualityComparer.Default.Equals(element, types.StringBuilder)))
        {
            context.ReportDiagnostic(Diagnostic.Create(s_noManagedString, location, what, method, display));
        }

        if (element.SpecialType == SpecialType.System_UIntPtr || IsWrittenAsNint(written))
        {
            // UIntPtr and nuint are one type too, so the message names the type as it is written.
            context.ReportDiagnostic(Diagnostic.Create(
                s_noNativeSizedInteger, location, what, method, written?.ToString() ?? display));
        }
    }

    // On .NET 7 and later IntPtr and nint are one type, so only the spelling tells a pointer from a native integer.
    private static bool IsWrittenAsNint(TypeSyntax? written) => written switch
    {
        ArrayTypeSyntax array => IsWrittenAsNint(array.ElementType),
        NullableTypeSyntax nullable => IsWrittenAsNint(nullable.ElementType),
        RefTypeSyntax reference => IsWrittenAsNint(reference.Type),
        IdentifierNameSyntax identifier => identifier.Identifier.ValueText == "nint",
        _ => false,
    };

    private static bool IsOneByte(ImmutableArray<AttributeData> attributes, INamedTypeSymbol? marshalAs) =>
        MarshalAs(attributes, marshalAs, UnmanagedType.I1) is not null;

    private static bool IsOneByteArray(ImmutableArray<AttributeData> attributes, INamedTypeSymbol? marshalAs) =>
        MarshalAs(attributes, marshalAs, UnmanagedType.LPArray) is { } attribute
        && attribute.NamedArguments.Any(argument =>
            argument.Key == nameof(MarshalAsAttribute.ArraySubType)
            && argument.Value.Value is { } subType
            && Convert.ToInt32(subType, CultureInfo.InvariantCulture) == (int)UnmanagedType.I1);

    private static AttributeData? MarshalAs(ImmutableArray<AttributeData> attributes, INamedTypeSymbol? marshalAs, UnmanagedType kind) =>
        attributes.FirstOrDefault(attribute =>
            SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, marshalAs)
            && attribute.ConstructorArguments.Length == 1
            && attribute.ConstructorArguments[0].Value is { } value
            && Convert.ToInt32(value, CultureInfo.InvariantCulture) == (int)kind);

    private static bool IsCdecl(ITypeSymbol delegateType, INamedTypeSymbol? unmanagedFunctionPointer) =>
        delegateType.GetAttributes().Any(attribute =>
            SymbolEqualityComparer.Default.Equals(attribute.AttributeClass, unmanagedFunctionPointer)
            && attribute.ConstructorArguments.Length == 1
            && attribute.ConstructorArguments[0].Value is { } convention
            && Convert.ToInt32(convention, CultureInfo.InvariantCulture) == (int)CallingConvention.Cdecl);

    /// <summary>The well-known types the checks compare against, resolved once per compilation.</summary>
    private sealed class Types
    {
        internal Types(INamedTypeSymbol? marshalAs, INamedTypeSymbol? unmanagedFunctionPointer, INamedTypeSymbol? stringBuilder)
        {
            MarshalAs = marshalAs;
            UnmanagedFunctionPointer = unmanagedFunctionPointer;
            StringBuilder = stringBuilder;
        }

        internal INamedTypeSymbol? MarshalAs { get; }

        internal INamedTypeSymbol? UnmanagedFunctionPointer { get; }

        internal INamedTypeSymbol? StringBuilder { get; }
    }
}
