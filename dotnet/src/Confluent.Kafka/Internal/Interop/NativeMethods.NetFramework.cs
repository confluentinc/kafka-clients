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

#if NETFRAMEWORK
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

internal static partial class NativeMethods
{
    private const uint LoadWithAlteredSearchPath = 0x00000008;

    // .NET Framework only. An AnyCPU process is 64-bit or 32-bit depending on the machine and the host,
    // so no single native beside the application fits every process. The package's build targets copy
    // both, under runtimes\win-x64\native and runtimes\win-x86\native, and this loads the one of the
    // process's bitness before the first call; a DllImport then binds to the loaded module by name. A
    // native beside the assembly is tried first, as DllImport tries it first, so only one copy loads. The
    // code base is the assembly's own folder when it runs shadow-copied. With nothing loaded, DllImport
    // probes and fails as it would without this.
    static NativeMethods()
    {
        try
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                PreloadNativeLibrary();
            }
        }
        catch (Exception)
        {
        }
    }

    private static void PreloadNativeLibrary()
    {
        Assembly assembly = typeof(NativeMethods).Assembly;
        AppDomain domain = AppDomain.CurrentDomain;
        List<string> candidates = NativeLibraryCandidates(
            assembly.Location, assembly.EscapedCodeBase, domain.BaseDirectory, domain.RelativeSearchPath, Environment.Is64BitProcess);
        foreach (string candidate in candidates)
        {
            if (File.Exists(candidate) && LoadLibrary(candidate))
            {
                return;
            }
        }
    }

    internal static List<string> NativeLibraryCandidates(
        string? assemblyLocation, string? codeBase, string? baseDirectory, string? relativeSearchPath, bool is64BitProcess)
    {
        var directories = new List<string>();
        if (!string.IsNullOrEmpty(assemblyLocation))
        {
            AddDirectoryOf(directories, assemblyLocation!);
        }
        if (!string.IsNullOrEmpty(codeBase) && Uri.TryCreate(codeBase, UriKind.Absolute, out Uri? uri) && uri.IsFile)
        {
            AddDirectoryOf(directories, uri.LocalPath);
        }
        if (!string.IsNullOrEmpty(baseDirectory))
        {
            directories.Add(baseDirectory!);
            foreach (string part in (relativeSearchPath ?? string.Empty).Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                directories.Add(Path.Combine(baseDirectory!, part));
            }
        }

        string file = DllName + ".dll";
        string rid = is64BitProcess ? "win-x64" : "win-x86";
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var candidates = new List<string>();
        foreach (string directory in directories)
        {
            string full = Path.GetFullPath(directory);
            if (seen.Add(full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)))
            {
                candidates.Add(Path.Combine(full, file));
                candidates.Add(Path.Combine(full, "runtimes", rid, "native", file));
            }
        }
        return candidates;
    }

    private static void AddDirectoryOf(List<string> directories, string path)
    {
        string? directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            directories.Add(directory!);
        }
    }

    private static bool LoadLibrary(string path)
    {
        IntPtr name = Marshal.StringToHGlobalUni(path);
        try
        {
            return LoadLibraryExW(name, IntPtr.Zero, LoadWithAlteredSearchPath) != IntPtr.Zero;
        }
        finally
        {
            Marshal.FreeHGlobal(name);
        }
    }

    // The one import outside the core: Windows' loader, for the preload above.
#pragma warning disable CKRDA0007
    [DllImport("kernel32.dll", EntryPoint = "LoadLibraryExW", ExactSpelling = true)]
    private static extern IntPtr LoadLibraryExW(IntPtr fileName, IntPtr file, uint flags);
#pragma warning restore CKRDA0007
}
#endif
