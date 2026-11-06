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
using System.Threading.Tasks;

using Microsoft.CodeAnalysis.Testing;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.NativeCallbackAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0003 accepts only a rooted delegate where native code receives one, and CKRDA0004 only a native callback that
/// cannot throw into native code.
/// </summary>
public sealed class NativeCallbackAnalyzerTests
{
    private const string MustWrap = "must wrap its whole body in a try with a catch-all clause";

    private const string Overridable = "is overridable, so native code can reach an override this rule cannot see";

    private const string Native = """
        using System;
        using System.Runtime.InteropServices;

        internal static class NativeMethods
        {
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void Callback(IntPtr opaque);

            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void ScalarCallback(sbyte present, IntPtr opaque);

            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void NumberCallback(double number, decimal amount, int count);

            [DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]
            internal static extern void Submit(Callback cb, IntPtr opaque);
        }

        internal enum Kind
        {
            First,
        }

        """;

    // On netstandard2.0, (IntPtr)present is IntPtr's user-defined operator; from C# 11 on .NET 7+ it is built in.
    private const string ForwardedArgumentsTheBindingUses = """
        internal static class C
        {
            private static readonly NativeMethods.Callback s_zero = OnZero;

            private static readonly NativeMethods.ScalarCallback s_scalar = OnScalar;

            private static void OnZero(IntPtr opaque) => Deliver(opaque, IntPtr.Zero, 0, Kind.First);

            private static void OnScalar(sbyte present, IntPtr opaque) => Deliver(opaque, (IntPtr)present, (long)7, default);

            private static void Deliver(IntPtr opaque, IntPtr value, long scalar, Kind kind)
            {
                try
                {
                }
                catch (Exception)
                {
                }
            }
        }
        """;

    [Fact]
    public Task AStaticReadonlyField_Null_AndAForwardedParameter_AreNotReported() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly NativeMethods.Callback s_callback = OnCallback;

                internal static void Run()
                {
                    NativeMethods.Submit(s_callback, IntPtr.Zero);
                    NativeMethods.Submit(null!, IntPtr.Zero);
                    Forward(s_callback);
                }

                private static void Forward(NativeMethods.Callback cb) => NativeMethods.Submit(cb, IntPtr.Zero);

                private static void OnCallback(IntPtr opaque)
                {
                    try
                    {
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            """);

    [Theory]
    [InlineData("{|#0:OnCallback|}")]
    [InlineData("{|#0:opaque => { }|}")]
    [InlineData("{|#0:new NativeMethods.Callback(OnCallback)|}")]
    [InlineData("{|#0:s_mutable|}")]
    [InlineData("{|#0:Instance.Field|}")]
    [InlineData("{|#0:local|}")]
    public Task ADelegateThatIsNotAStaticReadonlyField_IsReported(string argument) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal sealed class C
                {
                    private static NativeMethods.Callback s_mutable = OnCallback;

                    private static readonly C Instance = new C();

                    private readonly NativeMethods.Callback Field = OnCallback;

                    internal static void Run()
                    {
                        NativeMethods.Callback local = OnCallback;
                        NativeMethods.Submit({{argument}}, IntPtr.Zero);
                    }

                    private static void OnCallback(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.RootedCallbackDelegate).WithLocation(0)
                .WithMessage("The delegate passed to 'Submit' for 'cb' is not a static readonly field, so the GC can collect it while native code still holds it"));

    [Fact]
    public Task ADelegateForwardedThroughAParameter_IsCheckedAtTheForwardingCall() =>
        Verifier.VerifyAsync(
            Native + """
                internal static class C
                {
                    internal static void Run() => Forward({|#0:opaque => { }|});

                    private static void Forward(NativeMethods.Callback cb) => NativeMethods.Submit(cb, IntPtr.Zero);
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.RootedCallbackDelegate).WithLocation(0).WithArguments("Forward", "cb"));

    [Theory]
    [InlineData("catch")]
    [InlineData("catch (Exception)")]
    [InlineData("catch (System.Exception e)")]
    [InlineData("catch (InvalidOperationException)\n{\n}\ncatch (Exception)")]
    public Task ACallbackWrappedInACatchAll_IsNotReported(string catchClause) =>
        Verifier.VerifyAsync(Native + Callback(marked: false, body: $$"""
            {
                try
                {
                    GC.KeepAlive(opaque);
                }
                {{catchClause}}
                {
                }
            }
            """));

    [Fact]
    public Task ACallbackThatForwardsPlainArgumentsToAGuardedHelper_IsNotReported() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly NativeMethods.Callback s_callback = OnCallback;

                private static void OnCallback(IntPtr opaque) => Deliver(opaque, (long)7, Kind.First, default);

                private static void Deliver(IntPtr opaque, long value, Kind kind, IntPtr error)
                {
                    try
                    {
                        GC.KeepAlive(opaque);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            """);

    [Theory]
    [InlineData("{\n    GC.KeepAlive(opaque);\n}")]
    [InlineData("=> GC.KeepAlive(opaque);")]
    [InlineData("{\n    try\n    {\n        GC.KeepAlive(opaque);\n    }\n    finally\n    {\n    }\n}")]
    [InlineData("{\n    try\n    {\n    }\n    catch (InvalidOperationException)\n    {\n    }\n}")]
    [InlineData("{\n    try\n    {\n    }\n    catch (Exception) when (opaque != IntPtr.Zero)\n    {\n    }\n}")]
    [InlineData("{\n    GC.KeepAlive(opaque);\n    try\n    {\n    }\n    catch (Exception)\n    {\n    }\n}")]
    public Task ACallbackThatCanThrow_IsReported(string body) =>
        Verifier.VerifyAsync(
            Native + Callback(body, marked: true),
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0)
                .WithMessage("'OnCallback' is called from native code through 's_callback' and must wrap its whole body in a try with a catch-all clause"));

    [Theory]
    [InlineData("Deliver(opaque, Compute())")]
    [InlineData("Unguarded(opaque)")]
    public Task ACallbackThatForwardsToSomethingThatCanThrow_IsReported(string call) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C
                {
                    private static readonly NativeMethods.Callback s_callback = OnCallback;

                    private static void {|#0:OnCallback|}(IntPtr opaque) => {{call}};

                    private static long Compute() => 1;

                    private static void Unguarded(IntPtr opaque) => GC.KeepAlive(opaque);

                    private static void Deliver(IntPtr opaque, long value)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap));

    [Fact]
    public Task ALambdaCallback_IsCheckedToo() =>
        Verifier.VerifyAsync(
            Native + """
                internal static class C
                {
                    private static readonly NativeMethods.Callback s_guarded = opaque =>
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    };

                    private static readonly NativeMethods.Callback s_unguarded = {|#0:opaque => GC.KeepAlive(opaque)|};
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("The lambda", "s_unguarded", MustWrap));

    [Fact]
    public Task AFieldOfAManagedDelegateType_IsNotACallback() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly Action<IntPtr> s_managed = OnManaged;

                private static void OnManaged(IntPtr opaque) => GC.KeepAlive(opaque);
            }
            """);

    [Theory]
    [InlineData("s_callback = OnCallback;", "s_callback")]
    [InlineData("s_callback = new NativeMethods.Callback(OnCallback);", "s_callback")]
    [InlineData("NativeMethods.Callback local = OnCallback;\nGC.KeepAlive(local);", "local")]
    public Task ACallbackCreatedOutsideAFieldInitializer_IsChecked(string creation, string through) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C
                {
                    private static readonly NativeMethods.Callback s_callback;

                    static C()
                    {
                        {{creation}}
                    }

                    private static void {|#0:OnCallback|}(IntPtr opaque) => GC.KeepAlive(opaque);
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", through, MustWrap));

    [Fact]
    public Task AGuardedCallbackSetInAStaticConstructor_IsNotReported() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly NativeMethods.Callback s_callback;

                static C()
                {
                    s_callback = OnCallback;
                }

                private static void OnCallback(IntPtr opaque)
                {
                    try
                    {
                        GC.KeepAlive(opaque);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            """);

    [Fact]
    public Task AnEmptyCallback_IsNotReported() =>
        Verifier.VerifyAsync(Native + Callback(marked: false, body: "{\n}"));

    [Theory]
    [InlineData("catch (Exception)\n{\n    throw;\n}")]
    [InlineData("catch (Exception e)\n{\n    throw new InvalidOperationException(\"failed\", e);\n}")]
    [InlineData("catch (InvalidOperationException)\n{\n    throw;\n}\ncatch (Exception)\n{\n}")]
    [InlineData("catch (Exception)\n{\n    GC.KeepAlive(opaque);\n}")]
    [InlineData("catch (Exception)\n{\n}\nfinally\n{\n    GC.KeepAlive(opaque);\n}")]
    public Task ACatchOrFinallyBlockThatCanThrow_IsReported(string clauses) =>
        Verifier.VerifyAsync(
            Native + Callback(marked: true, body: $$"""
                {
                    try
                    {
                        GC.KeepAlive(opaque);
                    }
                    {{clauses}}
                }
                """),
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap));

    [Fact]
    public Task ACatchAndFinallyThatCallGuardedHelpers_AreNotReported() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly NativeMethods.Callback s_callback = OnCallback;

                private static void OnCallback(IntPtr opaque)
                {
                    try
                    {
                        GC.KeepAlive(opaque);
                    }
                    catch (Exception e)
                    {
                        Trace(e);
                    }
                    finally
                    {
                        Flush();
                    }
                }

                private static void Trace(Exception e)
                {
                    try
                    {
                        GC.KeepAlive(e);
                    }
                    catch (Exception)
                    {
                    }
                }

                private static void Flush()
                {
                    try
                    {
                    }
                    catch
                    {
                    }
                }
            }
            """);

    [Theory]
    [InlineData("Value")]
    [InlineData("(long)s_wrapper")]
    [InlineData("checked((int)s_big)")]
    [InlineData("(string)s_object")]
    [InlineData("s_holder.Field")]
    [InlineData("Other.Value")]
    public Task AForwardedArgumentThatCanThrow_IsReported(string argument) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C
                {
                    private static readonly NativeMethods.Callback s_callback = OnCallback;

                    private static readonly Holder s_holder = new Holder();

                    private static long s_big;

                    private static object s_object = "value";

                    private static Wrapper s_wrapper;

                    private static long Value => 1;

                    private static void {|#0:OnCallback|}(IntPtr opaque) => Deliver(opaque, {{argument}});

                    private static void Deliver(IntPtr opaque, object value)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }

                    private struct Wrapper
                    {
                        public static explicit operator long(Wrapper wrapper) => 1;
                    }

                    private sealed class Holder
                    {
                        internal long Field;
                    }

                    private static class Other
                    {
                        internal static readonly long Value = Compute();

                        private static long Compute() => throw new InvalidOperationException();
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap));

    [Fact]
    public Task ADelegateBuiltByAUserDefinedConversion_IsNotRootedByTheFieldItConvertsFrom() =>
        Verifier.VerifyAsync(
            Native + """
                internal sealed class Holder
                {
                    public static implicit operator NativeMethods.Callback(Holder holder) => OnCallback;

                    private static void OnCallback(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }

                internal static class C
                {
                    private static readonly Holder s_holder = new Holder();

                    internal static void Run() => NativeMethods.Submit({|#0:s_holder|}, IntPtr.Zero);
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.RootedCallbackDelegate).WithLocation(0).WithArguments("Submit", "cb"));

    [Fact]
    public Task AnOverridableCallback_IsReported() =>
        Verifier.VerifyAsync(
            Native + """
                internal class C
                {
                    private static readonly C Instance = new C();

                    private static readonly NativeMethods.Callback s_callback = Instance.OnCallback;

                    protected virtual void {|#0:OnCallback|}(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", Overridable));

    [Theory]
    [InlineData("protected virtual", true)]
    [InlineData("private", false)]
    public Task ACallbackThatForwardsToAnOverridableHelper_IsReported(string modifiers, bool reported) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal class C
                {
                    private static readonly C Instance = new C();

                    private static readonly NativeMethods.Callback s_callback = Instance.OnCallback;

                    private void {{(reported ? "{|#0:OnCallback|}" : "OnCallback")}}(IntPtr opaque) => Deliver(opaque);

                    {{modifiers}} void Deliver(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            reported
                ? new[] { Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap) }
                : Array.Empty<DiagnosticResult>());

    [Theory]
    [InlineData("cb = OnOther;")]
    [InlineData("cb ??= OnOther;")]
    [InlineData("(cb, _) = (s_other, 0);")]
    [InlineData("((cb, _), _) = ((s_other, 0), 0);")]
    public Task AForwardedParameterTheMethodWrites_IsReported(string write) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C
                {
                    private static readonly NativeMethods.Callback s_other = OnOther;

                    internal static void Run() => Forward(s_other);

                    private static void Forward(NativeMethods.Callback cb)
                    {
                        {{write}}
                        NativeMethods.Submit({|#0:cb|}, IntPtr.Zero);
                    }

                    private static void OnOther(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.RootedCallbackDelegate).WithLocation(0).WithArguments("Submit", "cb"));

    [Fact]
    public Task AParameterReadOnADeconstructionsRightHandSide_IsStillForwardedUnchanged() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly NativeMethods.Callback s_other = OnOther;

                internal static void Run() => Forward(s_other);

                private static void Forward(NativeMethods.Callback cb)
                {
                    var (copy, count) = (cb, 0);
                    GC.KeepAlive(copy);
                    NativeMethods.Submit(cb, IntPtr.Zero);
                }

                private static void OnOther(IntPtr opaque)
                {
                    try
                    {
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            """);

    [Theory]
    [InlineData("C<string>.Deliver(opaque)", true)]
    [InlineData("Deliver(opaque)", false)]
    public Task ForwardingToAnotherInstantiationOfAGenericType_IsReported(string call, bool reported) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C<T>
                {
                    private static readonly NativeMethods.Callback s_callback = OnCallback;

                    private static void {{(reported ? "{|#0:OnCallback|}" : "OnCallback")}}(IntPtr opaque) => {{call}};

                    internal static void Deliver(IntPtr opaque)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            reported
                ? new[] { Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap) }
                : Array.Empty<DiagnosticResult>());

    [Theory]
    [InlineData("(decimal)number", true)]
    [InlineData("(int)amount", true)]
    [InlineData("(Kind)amount", true)]
    [InlineData("(double)amount", false)]
    [InlineData("(decimal)count", false)]
    public Task AForwardedDecimalConversionThatCanOverflow_IsReported(string argument, bool reported) =>
        Verifier.VerifyAsync(
            Native + $$"""
                internal static class C
                {
                    private static readonly NativeMethods.NumberCallback s_callback = OnCallback;

                    private static void {{(reported ? "{|#0:OnCallback|}" : "OnCallback")}}(double number, decimal amount, int count) =>
                        Deliver({{argument}});

                    private static void Deliver(object value)
                    {
                        try
                        {
                        }
                        catch (Exception)
                        {
                        }
                    }
                }
                """,
            reported
                ? new[] { Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback", MustWrap) }
                : Array.Empty<DiagnosticResult>());

    [Fact]
    public Task TheForwardedArgumentsTheBindingUses_AreNotReported_OnNet80() =>
        Verifier.VerifyAsync(Native + ForwardedArgumentsTheBindingUses);

    [Fact]
    public Task TheForwardedArgumentsTheBindingUses_AreNotReported_OnNetStandard20() =>
        Verifier.VerifyAsync(ReferenceAssemblies.NetStandard.NetStandard20, Native + ForwardedArgumentsTheBindingUses);


    private static string Callback(string body, bool marked) =>
        $$"""
        internal static class C
        {
            private static readonly NativeMethods.Callback s_callback = OnCallback;

            private static void {{(marked ? "{|#0:OnCallback|}" : "OnCallback")}}(IntPtr opaque)
            {{body}}
        }
        """;
}
