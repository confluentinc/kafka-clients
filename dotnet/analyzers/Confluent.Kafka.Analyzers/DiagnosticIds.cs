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

namespace Confluent.Kafka.Analyzers;

/// <summary>
/// The diagnostic ids of the binding's own analyzers.
/// </summary>
public static class DiagnosticIds
{
    /// <summary>
    /// A <c>TaskCompletionSource</c> created without <c>TaskCreationOptions.RunContinuationsAsynchronously</c>.
    /// </summary>
    public const string RunContinuationsAsynchronously = "CKRDA0001";

    /// <summary>
    /// A delegate type in a <c>[DllImport]</c> signature without <c>[UnmanagedFunctionPointer(CallingConvention.Cdecl)]</c>.
    /// </summary>
    public const string CdeclCallbackDelegate = "CKRDA0002";

    /// <summary>
    /// A delegate passed to a P/Invoke that is not a <c>static readonly</c> field.
    /// </summary>
    public const string RootedCallbackDelegate = "CKRDA0003";

    /// <summary>
    /// A native callback whose body is not wrapped in a <c>try</c> with a catch-all clause.
    /// </summary>
    public const string NoThrowNativeCallback = "CKRDA0004";

    /// <summary>
    /// A client handle's <c>DangerousGetHandle()</c> passed to a P/Invoke instead of the <c>SafeHandle</c>.
    /// </summary>
    public const string RawClientHandle = "CKRDA0005";

    /// <summary>
    /// A <c>DangerousAddRef</c> without a matching <c>DangerousRelease</c> in a <c>finally</c> block of the same method.
    /// </summary>
    public const string AddRefReleasedInFinally = "CKRDA0006";

    /// <summary>
    /// A <c>[DllImport]</c> of another library than <c>confluent_kafka</c>, or without <c>CallingConvention.Cdecl</c>.
    /// </summary>
    public const string LibraryAndCallingConvention = "CKRDA0007";

    /// <summary>
    /// A <c>bool</c> in a <c>[DllImport]</c> signature without <c>[MarshalAs(UnmanagedType.I1)]</c>.
    /// </summary>
    public const string OneByteBool = "CKRDA0008";

    /// <summary>
    /// A <c>string</c> or <c>StringBuilder</c> in a <c>[DllImport]</c> signature.
    /// </summary>
    public const string NoManagedString = "CKRDA0009";

    /// <summary>
    /// An <c>nint</c>, <c>nuint</c> or <c>UIntPtr</c> in a <c>[DllImport]</c> signature.
    /// </summary>
    public const string NoNativeSizedInteger = "CKRDA0010";

    /// <summary>
    /// A comment that refers to a Java or Python source line.
    /// </summary>
    public const string SourceLineReference = "CKRDA0011";

    /// <summary>
    /// A comment that holds a phase or review id.
    /// </summary>
    public const string PhaseOrReviewId = "CKRDA0012";

    /// <summary>
    /// A public type or member whose name is not the name of the Java type or member it translates.
    /// </summary>
    public const string JavaName = "CKRDA0013";

    /// <summary>
    /// A line of the Java-name baseline, allowlist or pairs that matches nothing.
    /// </summary>
    public const string StaleJavaNameLine = "CKRDA0014";
}
