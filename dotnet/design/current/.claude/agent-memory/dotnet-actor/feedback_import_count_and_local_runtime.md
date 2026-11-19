---
name: import-count-and-local-runtime
description: Adding any library DllImport needs the NativeMethodsPrelinkTests import count bumped; this host runs only net10.0 tests
metadata:
  type: feedback
---

Every new `[DllImport]` in the library breaks `NativeMethodsPrelinkTests.TheSweepFindsEveryDllImport` until its
`ExpectedImportCount` is raised by the number added. Test-assembly DllImports (a test's nested `Native` class) are
not counted and not checked against the header.

**Why:** the sweep pins the exact count, so a forgotten bump shows up as a single failing test.

**How to apply:** when adding imports, count them (`grep -c DllImport <file>`) and bump the constant in the same
change. Run filtered tests with `-f net10.0`: only the .NET 10 runtime is installed on this macOS host, so
`-f net8.0` aborts with "You must install or update .NET".
