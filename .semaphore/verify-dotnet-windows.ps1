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
# build-dotnet-native.ps1 (its debug all-features build, the one the Linux and macOS binding tests
# link; the package's native blocks build the release one), then builds the .NET solution and runs
# the unit tests, which drive the mock clients, on net462 and net10.0, and on net10.0 against the
# library's netstandard2.0 build too (Confluent.Kafka.UnitTests.NetStandard), all in that
# platform's process, and then, on x64, the Native AOT check. The broker-backed tests do not run: they
# need Linux containers, which the Windows agents do not run.
#
# On a pull request the unit tests run through the test selection, as in the Linux and macOS jobs:
# only the tests that the selection in the committed test-selection/ folder made for exactly this
# working tree selects in its dotnet.solution section (rust/xtask/src/test_selection.rs); otherwise
# every test runs.
# The selection tasks run in xtask's test-selection binary, which builds on Windows, where xtask
# itself does not.
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

# On a build of master or of a release branch the debug builds use the release build's opt-level,
# as in the Linux and macOS jobs (.semaphore/job-start.sh).
if ($env:SEMAPHORE_GIT_REF_TYPE -eq 'branch' -and
    ($env:SEMAPHORE_GIT_BRANCH -ceq 'master' -or $env:SEMAPHORE_GIT_BRANCH -cmatch '^[0-9]+\.[0-9]+\.x$')) {
    $env:CARGO_PROFILE_DEV_OPT_LEVEL = '3'
    $env:CARGO_PROFILE_TEST_OPT_LEVEL = '3'
    Write-Output "A $env:SEMAPHORE_GIT_BRANCH build: the debug builds use opt-level 3, as the release build does"
}

$native = Join-Path $ToolsDir "native-$Platform"
Remove-Item -Recurse -Force $native -ErrorAction SilentlyContinue
# The debug all-features library, as the Linux and macOS binding tests link.
& (Join-Path $PSScriptRoot 'build-dotnet-native.ps1') -Platform $Platform -OutDir $native -RustProfile debug

Write-Output "== .NET ($Platform)"
$dotnet = Install-DotNet $Platform

# Some unit tests read the Java sources in the kafka submodule.
Invoke-Native git @('-C', $repo, 'config', 'core.longpaths', 'true')
Invoke-Retry { Invoke-Native git @('-C', $repo, 'submodule', 'update', '--init', '--depth=1', 'kafka') }

Write-Output "== Test selection ($Platform)"
if ($env:SEMAPHORE_GIT_PR_NUMBER) {
    $env:TEST_SELECTION = 'test-selection'
}
$rust = Join-Path $repo 'rust'
# The selection tasks run in rust\, as `cargo xtask` does: they find the repository from there.
Push-Location $rust
try {
    Invoke-Retry { Invoke-Native cargo @('build', '--locked', '--package', 'xtask', '--bin', 'test-selection') }
    $selection = Join-Path $rust 'target\debug\test-selection.exe'
    # What the selection runs and why, at the top of the tests' output.
    Invoke-Native $selection @('show-test-selection')
} finally {
    Pop-Location
}

Write-Output "== .NET build and unit tests ($Platform)"
# PlatformTarget makes every assembly, the net462 test host's included, run in this platform's
# process; CargoTargetTriple and CargoProfileDir point the library project at the native built above.
$properties = @('-c', 'Release', "-p:PlatformTarget=$Platform", "-p:CargoTargetTriple=$triple", '-p:CargoProfileDir=debug')
Invoke-Native $dotnet (@('build', (Join-Path $repo 'dotnet\Confluent.Kafka.sln')) + $properties)
$unitTests = Join-Path $repo 'dotnet\tests\Confluent.Kafka.UnitTests\Confluent.Kafka.UnitTests.csproj'
# The same tests against the library's netstandard2.0 build, the one a .NET 8 application loads.
$netStandardUnitTests = Join-Path $repo 'dotnet\tests\Confluent.Kafka.UnitTests.NetStandard\Confluent.Kafka.UnitTests.NetStandard.csproj'
$runs = @(
    @{ Name = 'net462'; Project = $unitTests; Tfm = 'net462' },
    @{ Name = 'net10.0'; Project = $unitTests; Tfm = 'net10.0' },
    @{ Name = 'net10.0 against the netstandard2.0 build'; Project = $netStandardUnitTests; Tfm = 'net10.0' }
)
$failed = @()
foreach ($run in $runs) {
    Write-Output "== unit tests, $($run.Name), $Platform"
    # `{}` is where selected-run puts the --filter of the selected tests, or nothing when every test
    # runs; it runs no test when dotnet.solution selects none.
    Push-Location $rust
    try {
        Invoke-Native $selection (@('selected-run', '-C', $repo, '--section', 'dotnet.solution', '--dotnet', '--',
                $dotnet, 'test', $run.Project, '--no-build', '-f', $run.Tfm,
                '--blame-hang-timeout', '15m', '--blame-hang-dump-type', 'none') + $properties + @('{}'))
    } catch {
        Write-Output "$_"
        $failed += $run.Name
    } finally {
        Pop-Location
    }
}
if ($failed) {
    throw "The unit tests failed on $($failed -join ', ') ($Platform)"
}
Write-Output "The selected unit tests passed on net462, net10.0 and net10.0 against the netstandard2.0 build ($Platform)"

# The Native AOT check, as `make -C dotnet test-aot-dotnet` on Linux and macOS: publishes
# dotnet/tools/AotSmoke as a Native AOT executable for win-x64, linked by the agent's Visual Studio C++
# tools, against the library's net10.0 build and against its netstandard2.0 build, the one a .NET 8
# application compiles, and runs both, whatever the test selection selects. CargoTargetTriple and
# CargoProfileDir point the library project at the native built above. The x86 job skips it: .NET 10's
# 32-bit Native AOT runtime links only with Visual Studio 2022's C++ tools (with Visual Studio 2019's,
# which the agent has, __dtoul3_legacy, __ltof3 and __ultof3 are unresolved).
if ($Platform -eq 'x86') {
    Write-Output "The Native AOT check does not run on x86: it needs Visual Studio 2022's C++ tools on the agent"
} else {
    $builds = @(
        @{ Name = 'net10.0'; Properties = @() },
        @{ Name = 'netstandard2.0'; Properties = @('-p:ConfluentKafkaNetStandard=true') }
    )
    foreach ($build in $builds) {
        Write-Output "== Native AOT check, the library's $($build.Name) build, $Platform"
        $aotOut = Join-Path $ToolsDir "aot-smoke-$Platform-$($build.Name)"
        $publishArgs = @('publish', (Join-Path $repo 'dotnet\tools\AotSmoke\AotSmoke.csproj'), '-c', 'Release', '-f', 'net10.0',
            '--use-current-runtime', "-p:CargoTargetTriple=$triple", '-p:CargoProfileDir=debug', '-o', $aotOut) + $build.Properties
        # The publish replaces the AotSmoke.exe it has just written to the output folder with the
        # native one, and a scanner reading the first can hold it (MSB3061, MSB3021), so the publish
        # is retried, each attempt first removing what an earlier one left there, where it can. A
        # real build error also runs three times before the job fails.
        Invoke-Retry {
            Remove-Item -Recurse -Force $aotOut -ErrorAction SilentlyContinue
            Invoke-Native $dotnet $publishArgs
        }
        Invoke-Native (Join-Path $aotOut 'AotSmoke.exe') @()
    }
    Write-Output "The Native AOT check passed on the library's net10.0 and netstandard2.0 builds ($Platform)"
}
