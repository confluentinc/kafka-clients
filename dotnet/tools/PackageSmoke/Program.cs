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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

/// <summary>
/// Checks that the Confluent.Kafka package works where it is installed: its native library loads
/// from the package, every P/Invoke of the assembly binds to it, and the clients construct, send
/// and close. The first argument is the package version expected.
/// </summary>
internal static class Program
{
    // No broker listens there: the clients are only constructed and closed.
    private const string BootstrapServers = "127.0.0.1:9";

    private static int Main(string[] args)
    {
        if (args.Length != 1)
        {
            Console.Error.WriteLine("usage: PackageSmoke <expected Confluent.Kafka version>");
            return 2;
        }

        Assembly assembly = typeof(KafkaException).Assembly;
        string version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "";
#if NETFRAMEWORK
        string runtime = $".NET Framework {Environment.Version}, {Environment.OSVersion}";
#else
        string runtime = $"{RuntimeInformation.FrameworkDescription}, {RuntimeInformation.RuntimeIdentifier}, {RuntimeInformation.OSDescription}";
#endif
        // test-nupkg.ps1 reads the bitness, to check that the program ran in a process of the platform tested.
        Console.WriteLine($"Confluent.Kafka {version} on {runtime}, {(Environment.Is64BitProcess ? 64 : 32)}-bit process");
        if (version.Split('+')[0] != args[0])
        {
            throw new InvalidOperationException($"loaded Confluent.Kafka {version}, not {args[0]}");
        }

        // The package's assembly for this application's framework, not another framework's; .NET
        // Framework runs its .NET Framework 4.6.2 assembly.
#if NETFRAMEWORK
        string? expected = ".NETFramework,Version=v4.6.2";
#else
        string? expected = AppContext.TargetFrameworkName;
#endif
        string? built = assembly.GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName;
        if (built != expected)
        {
            throw new InvalidOperationException($"loaded the Confluent.Kafka assembly built for {built}, not for {expected}");
        }

        using (var mock = new MockProducer<string, string>(Serdes.String().Serializer(), Serdes.String().Serializer()))
        {
            RecordMetadata metadata = mock.Send(new ProducerRecord<string, string>("smoke", "value", "key")).Get();
            Console.WriteLine($"OK: MockProducer sent to {metadata.Topic()}-{metadata.Partition()}@{metadata.Offset()}");
            mock.Close();
        }

        // Binds every P/Invoke now, so an entry point the native library lacks fails here. They are
        // internal, and Marshal.PrelinkAll binds only public methods, so each is prelinked itself.
        int imports = PrelinkEveryImport(assembly);
        string native = LoadedNativeLibrary(assembly);
        Console.WriteLine($"OK: all {imports} P/Invokes bind to {native}");
        if (!native.StartsWith(AppContext.BaseDirectory, IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"the native library was loaded from {native}, not from the application in {AppContext.BaseDirectory}");
        }

        foreach (string codec in new[] { "gzip", "snappy", "lz4", "zstd" })
        {
            CloseProducer(new Dictionary<string, object?> { ["bootstrap.servers"] = BootstrapServers, ["compression.type"] = codec });
            Console.WriteLine($"OK: compression {codec}");
        }
        CloseProducer(new Dictionary<string, object?> { ["bootstrap.servers"] = BootstrapServers, ["security.protocol"] = "SSL" });
        Console.WriteLine("OK: security.protocol=SSL");

        var consumerConfig = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = BootstrapServers,
            ["group.id"] = "package-smoke",
            ["group.protocol"] = "consumer",
        };
        using (var consumer = new KafkaConsumer<string, string>(consumerConfig, Serdes.String().Deserializer(), Serdes.String().Deserializer()))
        {
            consumer.Close();
        }
        Console.WriteLine("OK: KafkaConsumer");
        return 0;
    }

    private static void CloseProducer(Dictionary<string, object?> config)
    {
        using var producer = new KafkaProducer<string, string>(config, Serdes.String().Serializer(), Serdes.String().Serializer());
        producer.Close(TimeSpan.FromSeconds(10));
    }

    /// <summary>Prelinks every <c>[DllImport]</c> of the assembly and returns how many there are.</summary>
    private static int PrelinkEveryImport(Assembly assembly)
    {
        Type nativeMethods = assembly.GetType("Confluent.Kafka.Internal.Interop.NativeMethods", throwOnError: true)!;
        var failures = new List<string>();
        int imports = 0;
        foreach (MethodInfo method in nativeMethods.GetMethods(
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.DeclaredOnly))
        {
            if (method.GetCustomAttribute<DllImportAttribute>() is not { } import)
            {
                continue;
            }
            imports++;
            try
            {
                Marshal.Prelink(method);
            }
            catch (Exception e)
            {
                failures.Add($"{import.EntryPoint ?? method.Name}: {e.GetType().Name}: {e.Message}");
            }
        }
        if (imports == 0)
        {
            throw new InvalidOperationException("found no [DllImport] on NativeMethods");
        }
        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                $"{failures.Count} of {imports} P/Invokes do not bind:{Environment.NewLine}"
                + string.Join(Environment.NewLine, failures));
        }
        return imports;
    }

    private static bool IsWindows()
    {
#if NETFRAMEWORK
        return true;
#else
        return OperatingSystem.IsWindows();
#endif
    }

    /// <summary>The path of the native library the assembly's P/Invokes loaded.</summary>
    private static string LoadedNativeLibrary(Assembly assembly)
    {
        if (IsWindows())
        {
            IntPtr module = GetModuleHandleW("confluent_kafka.dll");
            var path = new StringBuilder(32768);
            if (module == IntPtr.Zero || GetModuleFileNameW(module, path, path.Capacity) == 0)
            {
                throw new InvalidOperationException("confluent_kafka.dll is not loaded in this process");
            }
            return Path.GetFullPath(path.ToString());
        }
#if NETFRAMEWORK
        throw new PlatformNotSupportedException();
#else
        IntPtr library = NativeLibrary.Load("confluent_kafka", assembly, null);
        IntPtr symbol = NativeLibrary.GetExport(library, "kafka_common_Error_destroy_err");
        DlInfo info;
        int found = OperatingSystem.IsMacOS() ? DlAddrMacOS(symbol, out info) : DlAddrLinux(symbol, out info);
        if (found == 0 || info.FileName == IntPtr.Zero)
        {
            throw new InvalidOperationException("dladdr found no library for kafka_common_Error_destroy_err");
        }
        return Path.GetFullPath(Marshal.PtrToStringUTF8(info.FileName)!);
#endif
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct DlInfo
    {
        public IntPtr FileName;
        public IntPtr FileBase;
        public IntPtr SymbolName;
        public IntPtr SymbolAddress;
    }

    [DllImport("libdl.so.2", EntryPoint = "dladdr")]
    private static extern int DlAddrLinux(IntPtr address, out DlInfo info);

    [DllImport("/usr/lib/libSystem.B.dylib", EntryPoint = "dladdr")]
    private static extern int DlAddrMacOS(IntPtr address, out DlInfo info);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr GetModuleHandleW(string moduleName);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    private static extern int GetModuleFileNameW(IntPtr module, StringBuilder fileName, int size);
}
