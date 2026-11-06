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

using Microsoft.CodeAnalysis.Testing;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.PInvokeSignatureAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0002 and CKRDA0007 to CKRDA0010 check each <c>[DllImport]</c> against the C ABI type map, and accept one that
/// follows it.
/// </summary>
public sealed class PInvokeSignatureAnalyzerTests
{
    private const string Import = "[DllImport(\"confluent_kafka\", CallingConvention = CallingConvention.Cdecl)]";

    [Fact]
    public Task AnImportThatFollowsTheTypeMap_IsNotReported() =>
        Verifier.VerifyAsync(Native("""
            [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
            internal delegate void Callback(IntPtr opaque);

            [DllImport("confluent_kafka", EntryPoint = "kafka_x", CallingConvention = CallingConvention.Cdecl)]
            [return: MarshalAs(UnmanagedType.I1)]
            internal static extern bool Call(
                [MarshalAs(UnmanagedType.I1)] bool flag, int count, long size, sbyte present, IntPtr handle,
                ref IntPtr outErr, Callback cb, IntPtr[] handles,
                [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)] bool[] flags);
            """));

    [Theory]
    [InlineData("\"other\", CallingConvention = CallingConvention.Cdecl", "other", "Cdecl")]
    [InlineData("\"confluent_kafka\"", "confluent_kafka", "Winapi")]
    [InlineData("\"confluent_kafka\", CallingConvention = CallingConvention.StdCall", "confluent_kafka", "StdCall")]
    public Task AnotherLibraryOrCallingConvention_IsReported(string arguments, string library, string convention) =>
        Verifier.VerifyAsync(
            Native($"[DllImport({arguments})] internal static extern void {{|#0:Call|}}();"),
            Verifier.Diagnostic(DiagnosticIds.LibraryAndCallingConvention).WithLocation(0)
                .WithMessage($"'Call' imports '{library}' with CallingConvention.{convention}; a P/Invoke imports 'confluent_kafka' with CallingConvention.Cdecl"));

    [Theory]
    [InlineData("bool {|#0:flag|}", "Parameter 'flag'")]
    [InlineData("ref bool {|#0:flag|}", "Parameter 'flag'")]
    [InlineData("[MarshalAs(UnmanagedType.U1)] bool {|#0:flag|}", "Parameter 'flag'")]
    [InlineData("[MarshalAs(UnmanagedType.Bool)] bool {|#0:flag|}", "Parameter 'flag'")]
    public Task ABoolParameterWithoutI1_IsReported(string parameter, string what) =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern void Call({parameter});"),
            Verifier.Diagnostic(DiagnosticIds.OneByteBool).WithLocation(0)
                .WithMessage($"{what} of 'Call' is a bool without [MarshalAs(UnmanagedType.I1)]; C's bool is one byte"));

    [Fact]
    public Task ABoolReturnWithoutI1_IsReported() =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern bool {{|#0:Call|}}();"),
            Verifier.Diagnostic(DiagnosticIds.OneByteBool).WithLocation(0)
                .WithArguments("The return value", "Call", "bool", "[MarshalAs(UnmanagedType.I1)]"));

    [Theory]
    [InlineData("bool[] {|#0:flags|}")]
    [InlineData("[MarshalAs(UnmanagedType.LPArray)] bool[] {|#0:flags|}")]
    [InlineData("[MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.Bool)] bool[] {|#0:flags|}")]
    [InlineData("[MarshalAs(UnmanagedType.I1)] bool[] {|#0:flags|}")]
    public Task ABoolArrayWithoutOneByteElements_IsReported(string parameter) =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern void Call({parameter});"),
            Verifier.Diagnostic(DiagnosticIds.OneByteBool).WithLocation(0)
                .WithMessage("Parameter 'flags' of 'Call' is a bool[] without [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.I1)]; C's bool is one byte"));

    [Theory]
    [InlineData("string {|#0:topic|}", "string")]
    [InlineData("[MarshalAs(UnmanagedType.LPStr)] string {|#0:topic|}", "string")]
    [InlineData("System.Text.StringBuilder {|#0:topic|}", "StringBuilder")]
    public Task AManagedStringParameter_IsReported(string parameter, string type) =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern void Call({parameter});"),
            Verifier.Diagnostic(DiagnosticIds.NoManagedString).WithLocation(0)
                .WithMessage($"Parameter 'topic' of 'Call' is a {type}; pass an IntPtr to a NUL-terminated UTF-8 buffer (Utf8Marshal) instead"));

    [Fact]
    public Task AStringReturn_IsReported() =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern string {{|#0:Call|}}();"),
            Verifier.Diagnostic(DiagnosticIds.NoManagedString).WithLocation(0).WithArguments("The return value", "Call", "string"));

    [Theory]
    [InlineData("nint {|#0:size|}", "nint")]
    [InlineData("nuint {|#0:size|}", "nuint")]
    [InlineData("UIntPtr {|#0:size|}", "UIntPtr")]
    [InlineData("ref nint {|#0:size|}", "nint")]
    public Task ANativeSizedIntegerParameter_IsReported(string parameter, string type) =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern void Call({parameter});"),
            Verifier.Diagnostic(DiagnosticIds.NoNativeSizedInteger).WithLocation(0)
                .WithMessage($"Parameter 'size' of 'Call' is a {type}; the C ABI has no size_t, so a size is an int or a long and a pointer an IntPtr"));

    [Fact]
    public Task ANativeSizedIntegerReturn_IsReported() =>
        Verifier.VerifyAsync(
            Native($"{Import} internal static extern nint {{|#0:Call|}}();"),
            Verifier.Diagnostic(DiagnosticIds.NoNativeSizedInteger).WithLocation(0).WithArguments("The return value", "Call", "nint"));

    [Theory]
    [InlineData("")]
    [InlineData("[UnmanagedFunctionPointer(CallingConvention.StdCall)]")]
    public Task ADelegateTypeThatIsNotCdecl_IsReported(string attribute) =>
        Verifier.VerifyAsync(
            Native($$"""
                {{attribute}}
                internal delegate void Callback(IntPtr opaque);

                {{Import}} internal static extern void Call(Callback {|#0:cb|});
                """),
            Verifier.Diagnostic(DiagnosticIds.CdeclCallbackDelegate).WithLocation(0)
                .WithMessage("'Callback' is passed to native code by 'Call' but has no [UnmanagedFunctionPointer(CallingConvention.Cdecl)]"));

    [Fact]
    public Task AMethodThatIsNotAnImport_IsNotReported() =>
        Verifier.VerifyAsync(Native("internal static string Call(bool flag, nint size) => string.Empty;"));

    private static string Native(string members) =>
        $$"""
        using System;
        using System.Runtime.InteropServices;

        internal static class NativeMethods
        {
            {{members}}
        }
        """;
}
