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

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.SafeHandleUseAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0005 keeps a client handle's raw pointer out of a P/Invoke, and CKRDA0006 requires every
/// <c>DangerousAddRef</c> to be released in a <c>finally</c> block of the same method.
/// </summary>
public sealed class SafeHandleUseAnalyzerTests
{
    private const string Handles = """
        using System;
        using System.Runtime.InteropServices;

        namespace Confluent.Kafka.Internal.Interop
        {
            internal abstract class SafeClientHandle : SafeHandle
            {
                protected SafeClientHandle()
                    : base(IntPtr.Zero, true)
                {
                }

                public override bool IsInvalid => handle == IntPtr.Zero;

                internal IntPtr DangerousAddRefClass(ref bool success)
                {
                    DangerousAddRef(ref success);
                    return handle;
                }
            }

            internal sealed class SafeProducerHandle : SafeClientHandle
            {
                protected override bool ReleaseHandle()
                {
                    NativeMethods.Destroy(DangerousGetHandle());
                    return true;
                }
            }

            internal sealed class SafeMapHandle : SafeHandle
            {
                public SafeMapHandle()
                    : base(IntPtr.Zero, true)
                {
                }

                public override bool IsInvalid => handle == IntPtr.Zero;

                protected override bool ReleaseHandle() => true;
            }

            internal static class NativeMethods
            {
                [DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]
                internal static extern void Operation(SafeProducerHandle self);

                [DllImport("confluent_kafka", CallingConvention = CallingConvention.Cdecl)]
                internal static extern void Destroy(IntPtr self);
            }
        }

        """;

    [Fact]
    public Task TheSafeHandle_ANonClientHandle_AndReleaseHandle_AreNotReported() =>
        Verifier.VerifyAsync(Handles + """
            namespace Confluent.Kafka.Internal.Interop
            {
                internal static class C
                {
                    internal static void Run(SafeProducerHandle producer, SafeMapHandle map)
                    {
                        NativeMethods.Operation(producer);
                        NativeMethods.Destroy(map.DangerousGetHandle());
                    }
                }
            }
            """);

    [Fact]
    public Task AClientHandlesRawPointer_IsReported() =>
        Verifier.VerifyAsync(
            Handles + """
                namespace Confluent.Kafka.Internal.Interop
                {
                    internal static class C
                    {
                        internal static void Run(SafeProducerHandle producer) =>
                            NativeMethods.Destroy({|#0:producer.DangerousGetHandle()|});
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.RawClientHandle).WithLocation(0)
                .WithMessage("'Destroy' is passed 'producer.DangerousGetHandle()' instead of the SafeHandle, so nothing keeps the handle alive during the call"));

    [Theory]
    [InlineData("handle.DangerousAddRef(ref added);")]
    [InlineData("IntPtr pointer = handle.DangerousAddRefClass(ref added);")]
    public Task AnAddRefReleasedInAFinally_IsNotReported(string addRef) =>
        Verifier.VerifyAsync(Handles + $$"""
            namespace Confluent.Kafka.Internal.Interop
            {
                internal static class C
                {
                    internal static void Run(SafeProducerHandle handle)
                    {
                        bool added = false;
                        try
                        {
                            {{addRef}}
                        }
                        finally
                        {
                            if (added)
                            {
                                handle.DangerousRelease();
                            }
                        }
                    }
                }
            }
            """);

    [Theory]
    [InlineData("")]
    [InlineData("handle.DangerousRelease();")]
    [InlineData("try\n{\n}\nfinally\n{\n    other.DangerousRelease();\n}")]
    public Task AnAddRefWithoutAMatchingReleaseInAFinally_IsReported(string release) =>
        Verifier.VerifyAsync(
            Handles + $$"""
                namespace Confluent.Kafka.Internal.Interop
                {
                    internal static class C
                    {
                        internal static void Run(SafeProducerHandle handle, SafeProducerHandle other)
                        {
                            bool added = false;
                            {|#0:handle.DangerousAddRef(ref added)|};
                            {{release}}
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.AddRefReleasedInFinally).WithLocation(0)
                .WithMessage("'handle.DangerousAddRef' has no matching 'handle.DangerousRelease()' in a finally block of the same method"));

    [Fact]
    public Task AWrapperHandsItsReferenceToItsCaller_WhoseReleaseIsChecked() =>
        Verifier.VerifyAsync(
            Handles + """
                namespace Confluent.Kafka.Internal.Interop
                {
                    internal sealed class Source
                    {
                        private readonly SafeProducerHandle _handle = null!;

                        internal bool Released()
                        {
                            bool added = false;
                            try
                            {
                                return TryAddRef(ref added);
                            }
                            finally
                            {
                                if (added)
                                {
                                    _handle.DangerousRelease();
                                }
                            }
                        }

                        internal bool Leaked()
                        {
                            bool added = false;
                            return {|#0:TryAddRef(ref added)|};
                        }

                        private bool TryAddRef(ref bool added)
                        {
                            _handle.DangerousAddRef(ref added);
                            return true;
                        }
                    }
                }
                """,
            Verifier.Diagnostic(DiagnosticIds.AddRefReleasedInFinally).WithLocation(0).WithArguments("this", "TryAddRef"));
}
