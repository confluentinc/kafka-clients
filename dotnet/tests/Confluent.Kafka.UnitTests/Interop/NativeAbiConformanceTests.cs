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
/// Every <c>[DllImport]</c> agrees with its prototype in the generated
/// <c>confluent_kafka_bindings.h</c>: the parameter count, each parameter's and the return's C
/// type, the callback delegates it passes, and the layout of the structs it passes.
/// </summary>
public sealed class NativeAbiConformanceTests
{
    private const string HeaderFile = "confluent_kafka_bindings.h";
    private const string MapKey = "kafka_Map_key_err";
    private const string MapValue = "kafka_Map_value_err";
    private const string FutureGetCb = "kafka_common_KafkaFuture_get_cb";
    private const string BytesDestroy = "kafka_Bytes_destroy_err";
    private const string MapSize = "kafka_Map_size_err";
    private const string ErrorType = "kafka_common_Error_t";

    private static readonly Lazy<string> s_headerText = new Lazy<string>(ReadHeader);

    [Fact]
    public void EveryDllImport_MatchesTheGeneratedHeader()
    {
        MethodInfo[] imports = NativeMethodsPrelinkTests.LibraryImports();
        CHeader header = CHeader.Parse(s_headerText.Value);
        AbiCheck check = AbiCheck.Run(header, Checked(imports));

        List<string> failures = new List<string>(check.Failures);
        failures.AddRange(AllowlistFailures(header, NativeAbiAllowlist.Missing, NativeAbiAllowlist.Differing, imports));

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} mismatches between the P/Invokes and {HeaderFile}:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void NoDllImport_NamesAPlainFunctionThatHasAnErrTwin()
    {
        IReadOnlyList<string> failures = ErrTwinFailures(
            CHeader.Parse(s_headerText.Value),
            Checked(NativeMethodsPrelinkTests.LibraryImports()));

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} imports name a function {HeaderFile} declares only as its _err twin:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void TheCheckReachesTheBindingsHeader_EveryImport_EveryDelegate_AndEveryStruct()
    {
        MethodInfo[] all = NativeMethodsPrelinkTests.LibraryImports();
        MethodInfo[] imports = Checked(all);
        CHeader header = CHeader.Parse(s_headerText.Value);
        AbiCheck check = AbiCheck.Run(header, imports);

        Assert.Contains(header.Functions.Keys, name => name.EndsWith("_err", StringComparison.Ordinal));
        Assert.DoesNotContain(
            header.Functions.Keys,
            name => name.EndsWith("_err", StringComparison.Ordinal)
                && header.Functions.ContainsKey(name.Substring(0, name.Length - "_err".Length)));

        Assert.NotEmpty(imports);
        Assert.Equal(all.Count(import => !NativeAbiAllowlist.Contains(EntryPointOf(import))), imports.Length);
        Assert.Equal(imports.Length, check.ImportCount);
        Assert.True(header.Functions.Count >= imports.Length, $"the header parsed to {header.Functions.Count} functions");

        Type[] parameterTypes = imports
            .SelectMany(import => import.GetParameters())
            .Select(parameter => Element(parameter.ParameterType))
            .ToArray();
        HashSet<Type> delegateParameters = new HashSet<Type>(parameterTypes.Where(type => typeof(Delegate).IsAssignableFrom(type)));
        Assert.NotEmpty(delegateParameters);
        Assert.True(
            delegateParameters.SetEquals(check.Delegates),
            "delegates not reached: " + string.Join(", ", delegateParameters.Except(check.Delegates).Select(AbiCheck.Name)));

        HashSet<Type> structParameters = new HashSet<Type>(
            parameterTypes
                .Concat(imports.Select(import => import.ReturnType))
                .Where(type => type.IsValueType && !type.IsPrimitive && !type.IsEnum && type != typeof(void)));
        Assert.True(
            structParameters.SetEquals(check.Structs),
            "structs not reached: " + string.Join(", ", structParameters.Except(check.Structs).Select(AbiCheck.Name)));
    }

    [Fact]
    public void AnUnlistedMissingImport_IsReported()
    {
        MethodInfo import = Assert.Single(Imports(MapKey));
        string header = ReplaceOnce(Code(), @"\b" + Regex.Escape(MapKey) + @"\s*\(", "kafka_Map_key_renamed_err(");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), new[] { import });

        string failure = Assert.Single(check.Failures);
        Assert.Equal($"NativeMethods.MapKey -> {MapKey}: {HeaderFile} declares no such function", failure);
    }

    [Fact]
    public void AStaleMissingEntry_IsReported()
    {
        Dictionary<string, string> missing = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MapKey] = "P1.2",
            ["kafka_no_such_import"] = "P1.4",
        };

        IReadOnlyList<string> failures = AllowlistFailures(
            CHeader.Parse(s_headerText.Value),
            missing,
            new Dictionary<string, string>(StringComparer.Ordinal),
            Imports(MapKey));

        Assert.Equal(
            new[]
            {
                $"{MapKey} is listed as missing (P1.2), but {HeaderFile} declares it",
                "kafka_no_such_import is listed as missing (P1.4), but no import uses it",
            },
            failures);
    }

    [Fact]
    public void AStaleDifferingEntry_IsReported()
    {
        Dictionary<string, string> differing = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MapKey] = "P1.3",
            [MapValue] = "P1.3",
            ["kafka_no_such_import"] = "P1.2",
        };

        IReadOnlyList<string> failures = AllowlistFailures(
            CHeader.Parse(s_headerText.Value),
            new Dictionary<string, string>(StringComparer.Ordinal),
            differing,
            Imports(MapKey));

        Assert.Equal(
            new[]
            {
                $"{MapKey} is listed as differing (P1.3), but every import of it matches {HeaderFile}",
                $"{MapValue} is listed as differing (P1.3), but no import uses it",
                $"kafka_no_such_import is listed as differing (P1.2), but {HeaderFile} does not declare it",
                "kafka_no_such_import is listed as differing (P1.2), but no import uses it",
            },
            failures);
    }

    [Fact]
    public void ADifferingEntryWhoseImportStillDiffers_IsNotReported()
    {
        string header = ReplaceFunction(
            MapKey,
            "void *kafka_Map_key_err(const kafka_Map_t *self_, int64_t index, kafka_common_Error_t **out_err)");
        Dictionary<string, string> differing = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [MapKey] = "P1.3",
        };

        IReadOnlyList<string> failures = AllowlistFailures(
            CHeader.Parse(header),
            new Dictionary<string, string>(StringComparer.Ordinal),
            differing,
            Imports(MapKey));

        Assert.Empty(failures);
    }

    [Fact]
    public void ANameListedAsBothMissingAndDiffering_IsReported()
    {
        Dictionary<string, string> listed = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["kafka_no_such_import"] = "P1.3",
        };

        IReadOnlyList<string> failures = AllowlistFailures(
            CHeader.Parse(s_headerText.Value),
            listed,
            listed,
            Imports(MapKey));

        Assert.Equal(
            new[]
            {
                "kafka_no_such_import is listed as both missing and differing",
                "kafka_no_such_import is listed as missing (P1.3), but no import uses it",
                $"kafka_no_such_import is listed as differing (P1.3), but {HeaderFile} does not declare it",
                "kafka_no_such_import is listed as differing (P1.3), but no import uses it",
            },
            failures);
    }

    [Fact]
    public void AnImportOfAPlainFunctionWithAnErrTwin_IsReported()
    {
        MethodInfo import = Assert.Single(Imports(MapKey));
        string header = ReplaceOnce(Code(), @"\b" + Regex.Escape(MapKey) + @"\s*\(", "kafka_Map_key_err_err(");

        IReadOnlyList<string> failures = ErrTwinFailures(CHeader.Parse(header), new[] { import });

        string failure = Assert.Single(failures);
        Assert.Equal($"NativeMethods.MapKey -> {MapKey}: {HeaderFile} declares only {MapKey}_err", failure);
    }

    [Fact]
    public void AParameterCountChange_IsReported()
    {
        string header = ReplaceFunction(
            MapValue,
            "void *kafka_Map_value_err(const kafka_Map_t *self_, kafka_common_Error_t **out_err)");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), Imports(MapValue));

        string failure = Assert.Single(check.Failures);
        Assert.Equal(
            "NativeMethods.MapValue -> kafka_Map_value_err: 3 parameters, the header declares 2: "
            + "void * (kafka_Map_t * self_, kafka_common_Error_t ** out_err)",
            failure);
    }

    [Fact]
    public void AStructLayoutChange_IsReported()
    {
        string header = ReplaceOnce(Code(), @"int32_t len;\s*\}\s*kafka_Bytes_t;", "} kafka_Bytes_t;");

        AbiCheck real = AbiCheck.Run(CHeader.Parse(s_headerText.Value), new[] { BytesVehicle.Import });
        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), new[] { BytesVehicle.Import });

        Assert.Empty(real.Failures);
        Assert.Contains(typeof(BytesVehicle), real.Structs);
        const string Where = "struct NativeAbiConformanceTests.BytesVehicle (mirrors kafka_Bytes_t)";
        Assert.Contains($"{Where}: 2 fields, the header declares 1 (data)", check.Failures);
        if (IntPtr.Size == 8)
        {
            Assert.Contains($"{Where}: 16 bytes, the header lays out 8", check.Failures);
        }
    }

    [Fact]
    public void AScalarTypeChange_IsReported()
    {
        string header = ReplaceFunction(
            MapKey,
            "void *kafka_Map_key_err(const kafka_Map_t *self_, int64_t index, kafka_common_Error_t **out_err)");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), Imports(MapKey));

        string failure = Assert.Single(check.Failures);
        Assert.Equal(
            "NativeMethods.MapKey -> kafka_Map_key_err parameter 1 'index' (C 'index'): "
            + "C 'int64_t' vs C# 'Int32': expected Int64",
            failure);
    }

    [Fact]
    public void AnErrorSlotThatIsNotARefIntPtr_IsReported()
    {
        AbiCheck check = AbiCheck.Run(
            CHeader.Parse(s_headerText.Value),
            new[]
            {
                ErrorSlotVehicles.ByValueSlot,
                ErrorSlotVehicles.OutSlot,
                ErrorSlotVehicles.InSlot,
                ErrorSlotVehicles.InRefSlot,
                ErrorSlotVehicles.InOutRefSlot,
                ErrorSlotVehicles.RefLongSlot,
            });

        const string Where = "NativeAbiConformanceTests.ErrorSlotVehicles";
        const string Slot = $"{MapSize} parameter 1 'outErr' (C 'out_err'): C 'kafka_common_Error_t **'";
        Assert.Equal(
            new[]
            {
                $"{Where}.ByValueSlotImport -> {Slot} vs C# 'IntPtr': expected ref IntPtr",
                $"{Where}.InOutRefSlotImport -> {Slot} vs C# '[In, Out] ref IntPtr': expected ref IntPtr",
                $"{Where}.InRefSlotImport -> {Slot} vs C# '[In] ref IntPtr': expected ref IntPtr",
                $"{Where}.InSlotImport -> {Slot} vs C# 'in IntPtr': expected ref IntPtr",
                $"{Where}.OutSlotImport -> {Slot} vs C# 'out IntPtr': expected ref IntPtr",
                $"{Where}.RefLongSlotImport -> {Slot} vs C# 'ref Int64': expected ref IntPtr",
            },
            check.Failures);
    }

    [Fact]
    public void ARefIntPtrErrorSlot_Matches()
    {
        AbiCheck check = AbiCheck.Run(
            CHeader.Parse(s_headerText.Value),
            Imports(MapSize).Concat(new[] { ErrorSlotVehicles.RefSlot }));

        Assert.Empty(check.Failures);
        Assert.Equal(2, check.ImportCount);
    }

    [Fact]
    public void ACallbackTypedefChange_IsReported()
    {
        MethodInfo import = Assert.Single(Imports(FutureGetCb));
        string typedef = CHeader.Parse(s_headerText.Value).Functions[FutureGetCb].Parameters[1].Type.Base;
        Assert.Equal("kafka_common_KafkaFuture_get_cb_t", typedef);

        string header = ReplaceOnce(
            Code(),
            @"typedef void \(\*" + Regex.Escape(typedef) + @"\)\(",
            $"typedef void (*{typedef})(int32_t, ");

        AbiCheck check = AbiCheck.Run(CHeader.Parse(header), new[] { import });

        string failure = Assert.Single(check.Failures);
        Assert.Equal(
            $"delegate NativeMethods.KafkaFutureGetCallback (passed by NativeMethods.KafkaFutureGetCb -> {FutureGetCb} parameter 1 'cb' (C 'cb')): "
            + "3 parameters, the header declares 4: void (int32_t, void * value, kafka_common_Error_t * error, void * opaque)",
            failure);
    }

    private static string ReadHeader()
    {
        string path = Path.Combine(AppContext.BaseDirectory, HeaderFile);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"{HeaderFile} is not next to the tests ({path}). The test project copies it from "
                + "rust/target/include/, which `cargo build --features ffi` writes; build the native first.",
                path);
        }

        return File.ReadAllText(path);
    }

    private static MethodInfo[] Checked(IEnumerable<MethodInfo> imports) =>
        imports.Where(import => !NativeAbiAllowlist.Contains(EntryPointOf(import))).ToArray();

    private static IReadOnlyList<string> AllowlistFailures(
        CHeader header,
        IReadOnlyDictionary<string, string> missing,
        IReadOnlyDictionary<string, string> differing,
        IEnumerable<MethodInfo> imports)
    {
        ILookup<string, MethodInfo> users = imports.ToLookup(EntryPointOf, StringComparer.Ordinal);
        List<string> failures = new List<string>();
        foreach (KeyValuePair<string, string> entry in missing.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            if (differing.ContainsKey(entry.Key))
            {
                failures.Add($"{entry.Key} is listed as both missing and differing");
            }

            if (header.Functions.ContainsKey(entry.Key))
            {
                failures.Add($"{entry.Key} is listed as missing ({entry.Value}), but {HeaderFile} declares it");
            }

            if (!users.Contains(entry.Key))
            {
                failures.Add($"{entry.Key} is listed as missing ({entry.Value}), but no import uses it");
            }
        }

        foreach (KeyValuePair<string, string> entry in differing.OrderBy(pair => pair.Key, StringComparer.Ordinal))
        {
            MethodInfo[] entryUsers = users[entry.Key].ToArray();
            if (!header.Functions.ContainsKey(entry.Key))
            {
                failures.Add($"{entry.Key} is listed as differing ({entry.Value}), but {HeaderFile} does not declare it");
            }
            else if (entryUsers.Length > 0 && AbiCheck.Run(header, entryUsers).Failures.Count == 0)
            {
                failures.Add($"{entry.Key} is listed as differing ({entry.Value}), but every import of it matches {HeaderFile}");
            }

            if (entryUsers.Length == 0)
            {
                failures.Add($"{entry.Key} is listed as differing ({entry.Value}), but no import uses it");
            }
        }

        return failures;
    }

    private static IReadOnlyList<string> ErrTwinFailures(CHeader header, IEnumerable<MethodInfo> imports) =>
        imports
            .Where(import => header.Functions.ContainsKey(EntryPointOf(import) + "_err"))
            .OrderBy(EntryPointOf, StringComparer.Ordinal)
            .Select(import => $"{AbiCheck.Name(import.DeclaringType!)}.{import.Name} -> {EntryPointOf(import)}: "
                + $"{HeaderFile} declares only {EntryPointOf(import)}_err")
            .ToList();

    private static Type Element(Type type) =>
        type.IsByRef || type.IsPointer || type.IsArray ? Element(type.GetElementType()!) : type;

    private static string Code() => CHeader.StripComments(s_headerText.Value);

    private static string ReplaceFunction(string name, string prototype) =>
        ReplaceOnce(Code(), @"[^;{}]*\b" + Regex.Escape(name) + @"\s*\([^;]*;", Environment.NewLine + prototype + ";");

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

    /// <summary>A C# mirror of <c>kafka_Bytes_t</c>, for the struct-layout check.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct BytesVehicle
    {
        internal static readonly MethodInfo Import = typeof(BytesVehicle).GetMethod(
            nameof(Destroy),
            BindingFlags.NonPublic | BindingFlags.Static)!;

        public IntPtr Data;
        public int Len;

        [DllImport("confluent_kafka", EntryPoint = BytesDestroy, CallingConvention = CallingConvention.Cdecl)]
        private static extern void Destroy(ref BytesVehicle bytes, ref IntPtr outErr);
    }

    /// <summary>Imports of <c>kafka_Map_size_err</c>, one per C# shape of its error slot, for the error-slot check.</summary>
    private static class ErrorSlotVehicles
    {
        internal static readonly MethodInfo RefSlot = Of(nameof(RefSlotImport));
        internal static readonly MethodInfo ByValueSlot = Of(nameof(ByValueSlotImport));
        internal static readonly MethodInfo OutSlot = Of(nameof(OutSlotImport));
        internal static readonly MethodInfo InSlot = Of(nameof(InSlotImport));
        internal static readonly MethodInfo InRefSlot = Of(nameof(InRefSlotImport));
        internal static readonly MethodInfo InOutRefSlot = Of(nameof(InOutRefSlotImport));
        internal static readonly MethodInfo RefLongSlot = Of(nameof(RefLongSlotImport));

        private static MethodInfo Of(string import) =>
            typeof(ErrorSlotVehicles).GetMethod(import, BindingFlags.NonPublic | BindingFlags.Static)!;

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int RefSlotImport(IntPtr self, ref IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int ByValueSlotImport(IntPtr self, IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int OutSlotImport(IntPtr self, out IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int InSlotImport(IntPtr self, in IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int InRefSlotImport(IntPtr self, [In] ref IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int InOutRefSlotImport(IntPtr self, [In, Out] ref IntPtr outErr);

        [DllImport("confluent_kafka", EntryPoint = MapSize, CallingConvention = CallingConvention.Cdecl)]
        private static extern int RefLongSlotImport(IntPtr self, ref long outErr);
    }

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
                _failures.Add($"{where}: {HeaderFile} declares no such function");
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
                if (IsErrorSlot(c.Type))
                {
                    CheckErrorSlot(at, c.Type, parameters[i]);
                    continue;
                }

                CheckValue(at, c.Type, parameters[i].ParameterType, MarshalAsOf(parameters[i].GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false)));
            }

            CheckValue($"{where} return", expected.Return, returnParameter.ParameterType, MarshalAsOf(returnParameter.GetCustomAttributes(typeof(MarshalAsAttribute), inherit: false)));
        }

        private static bool IsErrorSlot(CType c) => c.Function is null && c.Depth == 2 && c.Base == ErrorType;

        // Only a plain `ref IntPtr`: `out` does not make the caller null the slot, `in` lets the compiler
        // pass a temporary the error is lost in, and a by-value IntPtr is a NULL slot.
        private void CheckErrorSlot(string where, CType c, ParameterInfo parameter)
        {
            if (parameter.ParameterType != typeof(IntPtr).MakeByRefType() || parameter.IsIn || parameter.IsOut)
            {
                _failures.Add($"{where}: C '{c}' vs C# '{Shape(parameter)}': expected ref IntPtr");
            }
        }

        private static string Shape(ParameterInfo parameter)
        {
            Type type = parameter.ParameterType;
            if (!type.IsByRef)
            {
                return Name(type);
            }

            string modifier = parameter.IsOut ? (parameter.IsIn ? "[In, Out] ref" : "out")
                : parameter.IsIn ? (IsReadOnly(parameter) ? "in" : "[In] ref")
                : "ref";
            return $"{modifier} {Name(type.GetElementType()!)}";
        }

        private static bool IsReadOnly(ParameterInfo parameter) =>
            parameter.GetCustomAttributes(inherit: false)
                .Any(attribute => attribute.GetType().FullName == "System.Runtime.CompilerServices.IsReadOnlyAttribute");

        private void CheckValue(string where, CType c, Type cs, MarshalAsAttribute? marshalAs)
        {
            UnmanagedType? boolAs = marshalAs is null ? null
                : cs.IsArray ? (marshalAs.Value == UnmanagedType.LPArray ? marshalAs.ArraySubType : null)
                : marshalAs.Value;
            string? problem = Match(c, cs, boolAs, where);
            if (problem is not null)
            {
                _failures.Add($"{where}: C '{c}' vs C# '{Name(cs)}': {problem}");
            }
        }

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
