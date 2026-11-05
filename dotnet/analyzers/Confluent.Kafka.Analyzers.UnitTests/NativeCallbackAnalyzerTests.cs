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

using System.Threading.Tasks;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.NativeCallbackAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0003 accepts only a rooted delegate where native code receives one, and CKRDA0004 only a native callback that
/// cannot throw into native code.
/// </summary>
public sealed class NativeCallbackAnalyzerTests
{
    private const string Native = """
        using System;
        using System.Runtime.InteropServices;

        internal static class NativeMethods
        {
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void Callback(IntPtr opaque);

            [DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]
            internal static extern void Submit(Callback cb, IntPtr opaque);
        }

        internal enum Kind
        {
            First,
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
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("OnCallback", "s_callback"));

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
            Verifier.Diagnostic(DiagnosticIds.NoThrowNativeCallback).WithLocation(0).WithArguments("The lambda", "s_unguarded"));

    [Fact]
    public Task AFieldOfAManagedDelegateType_IsNotACallback() =>
        Verifier.VerifyAsync(Native + """
            internal static class C
            {
                private static readonly Action<IntPtr> s_managed = OnManaged;

                private static void OnManaged(IntPtr opaque) => GC.KeepAlive(opaque);
            }
            """);

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
