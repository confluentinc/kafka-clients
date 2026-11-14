#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# The "verify-dotnet (Windows ...)" jobs of semaphore.yml: builds the native library for one
# Windows platform, the platforms of the Confluent.Kafka package being win-x64 and win-x86, with
# build-dotnet-native.ps1, as the package's native blocks do, then builds the .NET solution and runs
# the unit tests, which drive the mock clients, on net462, net8.0 and net10.0, all in that
# platform's process, and then, on x64, the Native AOT check. The broker-backed tests do not run: they
# need Linux containers, which the Windows agents do not run.
#
# Usage: .\.semaphore\verify-dotnet-windows.ps1 -Platform x64|x86

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('x64', 'x86')]
    [string] $Platform
)

. (Join-Path $PSScriptRoot 'windows-tools.ps1')

$triple = $WindowsTriples[$Platform]
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

$native = Join-Path $ToolsDir "native-$Platform"
Remove-Item -Recurse -Force $native -ErrorAction SilentlyContinue
& (Join-Path $PSScriptRoot 'build-dotnet-native.ps1') -Platform $Platform -OutDir $native

Write-Output "== .NET ($Platform)"
$dotnet = Install-DotNet $Platform

# Some unit tests read the Java sources in the kafka submodule.
Invoke-Native git @('-C', $repo, 'config', 'core.longpaths', 'true')
Invoke-Retry { Invoke-Native git @('-C', $repo, 'submodule', 'update', '--init', '--depth=1', 'kafka') }

Write-Output "== .NET build and unit tests ($Platform)"
# PlatformTarget makes every assembly, the net462 test host's included, run in this platform's
# process; CargoTargetTriple points the library project at the native built above.
$properties = @('-c', 'Release', "-p:PlatformTarget=$Platform", "-p:CargoTargetTriple=$triple")
Invoke-Native $dotnet (@('build', (Join-Path $repo 'dotnet\Confluent.Kafka.sln')) + $properties)
$failed = @()
foreach ($tfm in 'net462', 'net8.0', 'net10.0') {
    Write-Output "== unit tests, $tfm, $Platform"
    try {
        Invoke-Native $dotnet (@('test', (Join-Path $repo 'dotnet\tests\Confluent.Kafka.UnitTests\Confluent.Kafka.UnitTests.csproj'), '--no-build', '-f', $tfm,
                '--blame-hang-timeout', '15m', '--blame-hang-dump-type', 'none') + $properties)
    } catch {
        Write-Output "$_"
        $failed += $tfm
    }
}
if ($failed) {
    throw "The unit tests failed on $($failed -join ', ') ($Platform)"
}
Write-Output "The unit tests passed on net462, net8.0 and net10.0 ($Platform)"

# The Native AOT check, as `make test-aot-dotnet` on Linux and macOS: publishes dotnet/tools/AotSmoke as
# a Native AOT executable for win-x64, linked by the agent's Visual Studio C++ tools, and runs it. The x86
# job skips it: .NET 10's 32-bit Native AOT runtime links only with Visual Studio 2022's C++ tools (with
# Visual Studio 2019's, which the agent has, __dtoul3_legacy, __ltof3 and __ultof3 are unresolved), and
# .NET 8 cannot AOT compile for x86 at all.
if ($Platform -eq 'x86') {
    Write-Output "The Native AOT check does not run on x86: it needs Visual Studio 2022's C++ tools on the agent"
} else {
    foreach ($tfm in 'net8.0', 'net10.0') {
        Write-Output "== Native AOT check, $tfm, $Platform"
        $aotOut = Join-Path $ToolsDir "aot-smoke-$Platform-$tfm"
        Remove-Item -Recurse -Force $aotOut -ErrorAction SilentlyContinue
        Invoke-Native $dotnet @('publish', (Join-Path $repo 'dotnet\tools\AotSmoke\AotSmoke.csproj'), '-c', 'Release', '-f', $tfm,
            '--use-current-runtime', "-p:CargoTargetTriple=$triple", '-o', $aotOut)
        Invoke-Native (Join-Path $aotOut 'AotSmoke.exe') @()
    }
    Write-Output "The Native AOT check passed on net8.0 and net10.0 ($Platform)"
}
