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
# platform's process, and then the Native AOT check. The broker-backed tests do not run: they need
# Linux containers, which the Windows agents do not run.
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
# a Native AOT executable for this platform, linked by the agent's Visual Studio C++ tools, and runs it.
# The AOT compiler runs as the machine's architecture, x64 here, and the x86 SDK does not fetch that
# compiler, so the x86 job publishes with the x64 SDK for win-x86, as on a developer's x64 machine.
# .NET cannot AOT compile net8.0 for x86, so that job checks net10.0 only.
$aotDotnet = if ($Platform -eq 'x86') { Install-DotNet 'x64' } else { $dotnet }
$aotFrameworks = if ($Platform -eq 'x86') { @('net10.0') } else { @('net8.0', 'net10.0') }
foreach ($tfm in $aotFrameworks) {
    Write-Output "== Native AOT check, $tfm, $Platform"
    $aotOut = Join-Path $ToolsDir "aot-smoke-$Platform-$tfm"
    Remove-Item -Recurse -Force $aotOut -ErrorAction SilentlyContinue
    # AotSmokeFrameworks keeps the restore to this one framework, so the x86 restore never meets net8.0.
    Invoke-Native $aotDotnet @('publish', (Join-Path $repo 'dotnet\tools\AotSmoke\AotSmoke.csproj'), '-c', 'Release', '-f', $tfm,
        "-p:AotSmokeFrameworks=$tfm", '-r', "win-$Platform", "-p:CargoTargetTriple=$triple", '-o', $aotOut)
    Invoke-Native (Join-Path $aotOut 'AotSmoke.exe') @()
}
Write-Output "The Native AOT check passed on $($aotFrameworks -join ', ') ($Platform)"
