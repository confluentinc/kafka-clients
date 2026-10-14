// Copyright 2025 Confluent Inc.
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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Every <c>[DllImport]</c> agrees with its prototype in the generated <c>confluent_kafka.h</c>:
/// the parameter count, each parameter's and the return's C type, the callback delegates it
/// passes, and the layout of the structs it passes.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The guard this exists for:</b> a P/Invoke whose signature no longer matches the native
/// function <b>still resolves</b>, so <see cref="NativeMethodsPrelinkTests"/> passes, and the
/// mismatch shows only as a wrong value, a crash or memory corruption at call time. Master
/// #187 made two such changes without a text conflict: <c>kafka_consumer_MockConsumer_set_poll_error</c>
/// went from 2 parameters to 4, and <c>kafka_producer_ProducerRecord_t</c> grew from 56 to 72
/// bytes, so the core read every record after the first of a <c>send_batch</c> array at the
/// wrong offset. <see cref="AParameterCountChange_IsReported"/> and
/// <see cref="AStructLayoutChange_IsReported"/> reproduce both against the checker.
/// </para>
/// <para>
/// What is compared, per <c>ffi-marshalling.md</c> §0.1's type map: <c>int32_t</c>/<c>int64_t</c>/
/// <c>int16_t</c>/<c>double</c>/<c>bool</c> scalars (a <c>bool</c> must carry
/// <c>MarshalAs(I1)</c>; a C enum is <c>int</c>); any pointer as <see cref="IntPtr"/>; a pointer
/// to an opaque struct as a <see cref="SafeHandle"/> (and one <see cref="SafeHandle"/> type
/// always stands for the same opaque struct); <c>out</c>/<c>ref</c>, arrays and C# pointers as
/// one level of C pointer over the element; a delegate as a C function pointer, its
/// <c>Invoke</c> compared the same way; a C# struct by its fields, offsets and size against the
/// C struct, laid out with natural alignment. A <see cref="string"/> anywhere is a failure:
/// strings are hand-marshalled UTF-8 (§0.1).
/// </para>
/// <para>
/// The header is copied next to the tests from <c>rust/target/include/</c>, where the same
/// <c>cargo build --features ffi</c> that builds the native writes it (<c>rust/build.rs</c>).
/// </para>
/// </remarks>
public sealed class NativeAbiConformanceTests
{
    private const string SetPollError = "kafka_consumer_MockConsumer_set_poll_error";
    private const string SendBatch = "kafka_producer_Producer_send_batch";

    private static readonly Lazy<string> s_headerText = new Lazy<string>(ReadHeader);

    /// <summary>
    /// The whole surface against the header. All mismatches are collected, so one run names
    /// every one of them rather than the first.
    /// </summary>
    [Fact]
    public void EveryDllImport_MatchesTheGeneratedHeader()
    {
        AbiCheck check = AbiCheck.Run(CHeader.Parse(s_headerText.Value), NativeMethodsPrelinkTests.LibraryImports());

        Assert.True(
            check.Failures.Count == 0,
            $"{check.Failures.Count} mismatches between the P/Invokes and confluent_kafka.h:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, check.Failures));
    }

    /// <summary>
    /// The control-positive for <see cref="EveryDllImport_MatchesTheGeneratedHeader"/>: the sweep
    /// checks every import, reaches every delegate type an import passes through its C function
    /// pointer, and reaches the producer-record struct. A sweep that silently skipped them would
    /// otherwise pass.
    /// </summary>
    [Fact]
    public void TheCheckReachesEveryImport_EveryDelegate_AndTheRecordStruct()
    {
        MethodInfo[] imports = NativeMethodsPrelinkTests.LibraryImports();
        CHeader header = CHeader.Parse(s_headerText.Value);
        AbiCheck check = AbiCheck.Run(header, imports);

        Assert.Equal(imports.Length, check.ImportCount);
        Assert.True(header.Functions.Count >= imports.Length, $"the header parsed to {header.Functions.Count} functions");

        HashSet<Type> delegateParameters = new HashSet<Type>(imports
            .SelectMany(import => import.GetParameters())
            .Select(parameter => parameter.ParameterType)
            .Where(type => typeof(Delegate).IsAssignableFrom(type)));
        Assert.NotEmpty(delegateParameters);
        Assert.True(
            delegateParameters.SetEquals(check.Delegates),
            "delegates not reached: " + string.Join(", ", delegateParameters.Except(check.Delegates).Select(AbiCheck.Name)));

        Assert.Contains(typeof(ProducerRecordNative), check.Structs);
    }

    /// <summary>
    /// #187's <c>set_poll_error</c> break: the binding's 4-parameter import against the old
    /// 2-parameter prototype.
    /// </summary>
    [Fact]
    public void AParameterCountChange_IsReported()
    {
        string header = ReplaceFunction(
            SetPollError,
            "kafka_common_Error_t *kafka_consumer_MockConsumer_set_poll_error(const kafka_consumer_Consumer_t *consumer, const char *message)");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), Imports(SetPollError));

        string failure = Assert.Single(check.Failures);
        Assert.Equal(
            "NativeMethods.MockConsumerSetPollError -> kafka_consumer_MockConsumer_set_poll_error: "
            + "4 parameters, the header declares 2: "
            + "kafka_common_Error_t * (kafka_consumer_Consumer_t * consumer, char * message)",
            failure);
    }

    /// <summary>
    /// #187's <c>ProducerRecord_t</c> break, seen from the other side: the binding's 9-field,
    /// 72-byte mirror against the 7-field, 56-byte struct.
    /// </summary>
    [Fact]
    public void AStructLayoutChange_IsReported()
    {
        string header = ReplaceOnce(
            Code(),
            @"const kafka_common_header_Header_t \*headers;\s*int32_t header_count;",
            string.Empty);

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), Imports(SendBatch));

        const string Where = "struct ProducerRecordNative (mirrors kafka_producer_ProducerRecord_t)";
        Assert.Contains(
            $"{Where}: 9 fields, the header declares 7 (topic, partition, timestamp, key, key_len, value, value_len)",
            check.Failures);
        if (IntPtr.Size == 8)
        {
            Assert.Contains($"{Where}: 72 bytes, the header lays out 56", check.Failures);
        }
    }

    /// <summary>A parameter whose C type changes width is reported against the C# type.</summary>
    [Fact]
    public void AScalarTypeChange_IsReported()
    {
        string header = ReplaceFunction(
            SetPollError,
            "kafka_common_Error_t *kafka_consumer_MockConsumer_set_poll_error(const kafka_consumer_Consumer_t *consumer, bool clear, int64_t code, const char *message)");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), Imports(SetPollError));

        string failure = Assert.Single(check.Failures);
        Assert.Equal(
            "NativeMethods.MockConsumerSetPollError -> kafka_consumer_MockConsumer_set_poll_error parameter 2 'code' (C 'code'): "
            + "C 'int64_t' vs C# 'Int32': expected Int64",
            failure);
    }

    /// <summary>
    /// A callback typedef that gains a parameter is reported against the delegate the import
    /// passes for it.
    /// </summary>
    [Fact]
    public void ACallbackTypedefChange_IsReported()
    {
        CHeader real = CHeader.Parse(s_headerText.Value);
        (MethodInfo import, string typedef) = NativeMethodsPrelinkTests.LibraryImports()
            .Where(method => method.GetParameters().Any(p => p.ParameterType == typeof(ProducerCallbacks.OperationCallback)))
            .OrderBy(EntryPointOf, StringComparer.Ordinal)
            .Select(method =>
            {
                int index = Array.FindIndex(method.GetParameters(), p => p.ParameterType == typeof(ProducerCallbacks.OperationCallback));
                return (method, real.Functions[EntryPointOf(method)].Parameters[index].Type.Base);
            })
            .First(pair => real.Callbacks.ContainsKey(pair.Base));

        string header = ReplaceOnce(
            Code(),
            @"typedef void \(\*" + Regex.Escape(typedef) + @"\)\(",
            $"typedef void (*{typedef})(int32_t, ");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), new[] { import });

        string failure = Assert.Single(check.Failures);
        Assert.StartsWith($"delegate ProducerCallbacks.OperationCallback (passed by NativeMethods.{import.Name} -> {EntryPointOf(import)} parameter ", failure);
        Assert.EndsWith("): 2 parameters, the header declares 3: void (int32_t, kafka_common_Error_t *, void *)", failure);
    }

    private static string ReadHeader()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "confluent_kafka.h");
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"confluent_kafka.h is not next to the tests ({path}). The test project copies it from "
                + "rust/target/include/, which `cargo build --features ffi` writes; build the native first.",
                path);
        }

        return File.ReadAllText(path);
    }

    /// <summary>The header without comments, the form the edits below apply to.</summary>
    private static string Code() => CHeader.StripComments(s_headerText.Value);

    /// <summary>The header with one function's prototype replaced by <paramref name="prototype"/>.</summary>
    private static string ReplaceFunction(string name, string prototype) =>
        ReplaceOnce(Code(), @"[^;{}]*\b" + Regex.Escape(name) + @"\s*\([^;]*;", Environment.NewLine + prototype + ";");

    /// <summary>Replaces the one match of <paramref name="pattern"/>; any other count fails the test.</summary>
    private static string ReplaceOnce(string text, string pattern, string replacement)
    {
        Regex regex = new Regex(pattern);
        int matches = regex.Matches(text).Count;
        Assert.True(matches == 1, $"'{pattern}' matched {matches} times in the header, expected once");
        return regex.Replace(text, replacement.Replace("$", "$$"));
    }

    private static MethodInfo[] Imports(string entryPoint)
    {
        MethodInfo[] imports = NativeMethodsPrelinkTests.LibraryImports()
            .Where(method => EntryPointOf(method) == entryPoint)
            .ToArray();
        Assert.NotEmpty(imports);
        return imports;
    }

    private static string EntryPointOf(MethodInfo method) =>
        method.GetCustomAttribute<DllImportAttribute>()!.EntryPoint ?? method.Name;

    /// <summary>
    /// One pass of the comparison over a set of imports, collecting every mismatch as a line.
    /// </summary>
    private sealed class AbiCheck
    {
        private static readonly Dictionary<string, Type> s_scalars = new Dictionary<string, Type>(StringComparer.Ordinal)
        {
            ["bool"] = typeof(bool),
            ["int8_t"] = typeof(sbyte),
            ["uint8_t"] = typeof(byte),
            ["int16_t"] = typeof(short),
            ["uint16_t"] = typeof(ushort),
            ["int32_t"] = typeof(int),
            ["uint32_t"] = typeof(uint),
            ["int64_t"] = typeof(long),
            ["uint64_t"] = typeof(ulong),
            ["float"] = typeof(float),
            ["double"] = typeof(double),
        };

        private static readonly Dictionary<string, int> s_scalarSizes = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            ["bool"] = 1,
            ["int8_t"] = 1,
            ["uint8_t"] = 1,
            ["int16_t"] = 2,
            ["uint16_t"] = 2,
            ["int32_t"] = 4,
            ["uint32_t"] = 4,
            ["float"] = 4,
            ["int64_t"] = 8,
            ["uint64_t"] = 8,
            ["double"] = 8,
        };

        private readonly CHeader _header;
        private readonly HashSet<string> _delegateChecks = new HashSet<string>(StringComparer.Ordinal);
        private readonly HashSet<Type> _delegates = new HashSet<Type>();
        private readonly Dictionary<Type, string> _structs = new Dictionary<Type, string>();
        private readonly Dictionary<Type, SortedSet<string>> _safeHandles = new Dictionary<Type, SortedSet<string>>();
        private readonly List<string> _failures = new List<string>();

        private AbiCheck(CHeader header)
        {
            _header = header;
        }

        internal IReadOnlyList<string> Failures => _failures;

        internal int ImportCount { get; private set; }

        internal IReadOnlyCollection<Type> Delegates => _delegates;

        internal IReadOnlyCollection<Type> Structs => _structs.Keys;

        internal static AbiCheck Run(CHeader header, IEnumerable<MethodInfo> imports)
        {
            AbiCheck check = new AbiCheck(header);
            foreach (MethodInfo import in imports
                .OrderBy(EntryPointOf, StringComparer.Ordinal)
                .ThenBy(method => method.Name, StringComparer.Ordinal))
            {
                check.Import(import);
            }

            foreach (KeyValuePair<Type, SortedSet<string>> handle in check._safeHandles.Where(pair => pair.Value.Count > 1))
            {
                check._failures.Add($"{Name(handle.Key)} stands for more than one opaque struct: {string.Join(", ", handle.Value)}");
            }

            return check;
        }

        /// <summary>A readable C# type name for the messages: nested types dotted, no namespace.</summary>
        internal static string Name(Type type)
        {
            if (type.IsByRef)
            {
                return Name(type.GetElementType()!) + "&";
            }

            if (type.IsArray)
            {
                return Name(type.GetElementType()!) + "[]";
            }

            if (type.IsPointer)
            {
                return Name(type.GetElementType()!) + "*";
            }

            return type.IsNested ? Name(type.DeclaringType!) + "." + type.Name : type.Name;
        }

        private void Import(MethodInfo method)
        {
            ImportCount++;
            DllImportAttribute import = method.GetCustomAttribute<DllImportAttribute>()!;
            string entryPoint = EntryPointOf(method);
            string where = $"{Name(method.DeclaringType!)}.{method.Name} -> {entryPoint}";

            if (import.CallingConvention != CallingConvention.Cdecl)
            {
                _failures.Add($"{where}: needs CallingConvention.Cdecl, has {import.CallingConvention}");
            }

            if (!_header.Functions.TryGetValue(entryPoint, out CSignature? prototype))
            {
                _failures.Add($"{where}: confluent_kafka.h declares no such function");
                return;
            }

            CheckSignature(where, prototype, method.GetParameters(), method.ReturnParameter);
        }

        private void CheckSignature(string where, CSignature expected, ParameterInfo[] parameters, ParameterInfo returnParameter)
        {
            if (parameters.Length != expected.Parameters.Count)
            {
                _failures.Add($"{where}: {parameters.Length} parameters, the header declares {expected.Parameters.Count}: {expected}");
                return;
            }

            for (int i = 0; i < parameters.Length; i++)
            {
                CParam c = expected.Parameters[i];
                string at = $"{where} parameter {i} '{parameters[i].Name}'" + (c.Name is null ? string.Empty : $" (C '{c.Name}')");
                CheckValue(at, c.Type, parameters[i].ParameterType, MarshalAsOf(parameters[i].GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false)));
            }

            CheckValue($"{where} return", expected.Return, returnParameter.ParameterType, MarshalAsOf(returnParameter.GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false)));
        }

        private void CheckValue(string where, CType c, Type cs, MarshalAsAttribute? marshalAs)
        {
            // A bool array's elements are marshalled by ArraySubType; any other bool by Value.
            UnmanagedType? boolAs = marshalAs is null ? null
                : cs.IsArray ? (marshalAs.Value == UnmanagedType.LPArray ? marshalAs.ArraySubType : null)
                : marshalAs.Value;
            string? problem = Match(c, cs, boolAs, where);
            if (problem is not null)
            {
                _failures.Add($"{where}: C '{c}' vs C# '{Name(cs)}': {problem}");
            }
        }

        /// <summary>
        /// <see langword="null"/> when <paramref name="cs"/> can carry <paramref name="c"/>, else the
        /// reason it cannot. Delegates and structs are checked in depth, reporting under their own name.
        /// </summary>
        private string? Match(CType c, Type cs, UnmanagedType? boolAs, string where)
        {
            if (cs == typeof(void))
            {
                return c.IsVoid ? null : "the C# side is void";
            }

            if (c.IsVoid)
            {
                return "the header has void";
            }

            if (cs.IsByRef || cs.IsPointer || cs.IsArray)
            {
                if (c.Depth == 0)
                {
                    return "an out/ref, a pointer or an array needs a C pointer";
                }

                // The element of a C# bool* is the 1-byte bool itself.
                return Match(c.Pointee(), cs.GetElementType()!, cs.IsPointer ? UnmanagedType.I1 : boolAs, where);
            }

            if (cs == typeof(IntPtr) || cs == typeof(UIntPtr))
            {
                return c.Depth > 0 || FunctionOf(c) is not null ? null : "an IntPtr needs a C pointer";
            }

            if (typeof(SafeHandle).IsAssignableFrom(cs))
            {
                if (c.Depth != 1 || !_header.OpaqueStructs.Contains(c.Base))
                {
                    return "a SafeHandle needs a pointer to an opaque struct";
                }

                if (!_safeHandles.TryGetValue(cs, out SortedSet<string>? targets))
                {
                    targets = new SortedSet<string>(StringComparer.Ordinal);
                    _safeHandles.Add(cs, targets);
                }

                targets.Add(c.Base);
                return null;
            }

            if (typeof(Delegate).IsAssignableFrom(cs))
            {
                CSignature? function = FunctionOf(c);
                if (function is null)
                {
                    return "a delegate needs a C function pointer";
                }

                CheckDelegate(cs, function, where);
                return null;
            }

            if (cs == typeof(string) || cs == typeof(StringBuilder) || cs == typeof(char))
            {
                return "strings cross as hand-marshalled UTF-8 behind an IntPtr (ffi-marshalling.md §0.1)";
            }

            if (Nullable.GetUnderlyingType(cs) is not null)
            {
                return "a Nullable<T> has no C layout";
            }

            if (cs.IsValueType && !cs.IsPrimitive && !cs.IsEnum)
            {
                if (c.Depth != 0 || !_header.Structs.ContainsKey(c.Base))
                {
                    return "a C# struct needs a C struct";
                }

                CheckStruct(cs, c.Base);
                return null;
            }

            if (c.Depth != 0 || FunctionOf(c) is not null)
            {
                return "a scalar needs a C scalar";
            }

            // A C enum is int-sized.
            string cBase = _header.Enums.Contains(c.Base) ? "int32_t" : c.Base;
            if (!s_scalars.TryGetValue(cBase, out Type? expected))
            {
                return $"'{c.Base}' is not a C scalar";
            }

            Type actual = cs.IsEnum ? Enum.GetUnderlyingType(cs) : cs;
            if (actual != expected)
            {
                return $"expected {expected.Name}";
            }

            if (actual == typeof(bool) && boolAs != UnmanagedType.I1)
            {
                return "a C bool is 1 byte, so it needs MarshalAs(UnmanagedType.I1)";
            }

            return null;
        }

        private CSignature? FunctionOf(CType c)
        {
            if (c.Function is not null)
            {
                return c.Function;
            }

            return c.Depth == 0 && _header.Callbacks.TryGetValue(c.Base, out CSignature? typedef) ? typedef : null;
        }

        private void CheckDelegate(Type type, CSignature function, string where)
        {
            _delegates.Add(type);
            if (!_delegateChecks.Add(type.FullName + "|" + function))
            {
                return;
            }

            string at = $"delegate {Name(type)} (passed by {where})";
            UnmanagedFunctionPointerAttribute? pointer = type.GetCustomAttribute<UnmanagedFunctionPointerAttribute>();
            if (pointer is null || pointer.CallingConvention != CallingConvention.Cdecl)
            {
                _failures.Add($"{at}: needs [UnmanagedFunctionPointer(CallingConvention.Cdecl)]");
            }

            MethodInfo invoke = type.GetMethod("Invoke")!;
            CheckSignature(at, function, invoke.GetParameters(), invoke.ReturnParameter);
        }

        private void CheckStruct(Type type, string cName)
        {
            if (_structs.TryGetValue(type, out string? seen))
            {
                if (seen != cName)
                {
                    _failures.Add($"struct {Name(type)} stands for both {seen} and {cName}");
                }

                return;
            }

            _structs.Add(type, cName);
            string where = $"struct {Name(type)} (mirrors {cName})";
            IReadOnlyList<CField> expected = _header.Structs[cName];

            if (type.StructLayoutAttribute?.Value != LayoutKind.Sequential)
            {
                _failures.Add($"{where}: needs [StructLayout(LayoutKind.Sequential)]");
            }

            FieldInfo[] fields = type
                .GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .OrderBy(field => Marshal.OffsetOf(type, field.Name).ToInt64())
                .ToArray();
            (long[] offsets, long size, _) = LayOut(expected);

            if (fields.Length != expected.Count)
            {
                _failures.Add($"{where}: {fields.Length} fields, the header declares {expected.Count} ({string.Join(", ", expected.Select(field => field.Name))})");
            }

            for (int i = 0; i < Math.Min(fields.Length, expected.Count); i++)
            {
                string at = $"{where} field {i} '{fields[i].Name}' (C '{expected[i].Name}')";
                CheckValue(at, expected[i].Type, fields[i].FieldType, MarshalAsOf(fields[i].GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false)));

                long offset = Marshal.OffsetOf(type, fields[i].Name).ToInt64();
                if (offset != offsets[i])
                {
                    _failures.Add($"{at}: at offset {offset}, the header lays it out at {offsets[i]}");
                }
            }

            int managedSize = Marshal.SizeOf(type);
            if (managedSize != size)
            {
                _failures.Add($"{where}: {managedSize} bytes, the header lays out {size}");
            }
        }

        /// <summary>
        /// The C layout of a struct's fields, with natural alignment: each field at the next
        /// multiple of its own alignment, the size rounded up to the largest one. (32-bit x86
        /// Linux aligns 8-byte fields to 4; no supported target is that.)
        /// </summary>
        private (long[] Offsets, long Size, long Align) LayOut(IReadOnlyList<CField> fields)
        {
            long[] offsets = new long[fields.Count];
            long offset = 0;
            long align = 1;
            for (int i = 0; i < fields.Count; i++)
            {
                (long fieldSize, long fieldAlign) = SizeOf(fields[i].Type);
                offset = RoundUp(offset, fieldAlign);
                offsets[i] = offset;
                offset += fieldSize;
                align = Math.Max(align, fieldAlign);
            }

            return (offsets, RoundUp(offset, align), align);
        }

        private (long Size, long Align) SizeOf(CType c)
        {
            if (c.Depth > 0 || FunctionOf(c) is not null)
            {
                return (IntPtr.Size, IntPtr.Size);
            }

            if (_header.Enums.Contains(c.Base))
            {
                return (4, 4);
            }

            if (_header.Structs.TryGetValue(c.Base, out IReadOnlyList<CField>? fields))
            {
                (_, long size, long align) = LayOut(fields);
                return (size, align);
            }

            if (s_scalarSizes.TryGetValue(c.Base, out int scalar))
            {
                return (scalar, scalar);
            }

            throw new InvalidOperationException($"No C layout for '{c}'.");
        }

        private static long RoundUp(long value, long alignment) => (value + alignment - 1) / alignment * alignment;

        private static MarshalAsAttribute? MarshalAsOf(object[] attributes) =>
            attributes.Length == 0 ? null : (MarshalAsAttribute)attributes[0];
    }
}
