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
# the unit tests, which drive the mock clients, on net462, net8.0 and net10.0, all in that
# platform's process. The broker-backed tests do not run: they need Linux containers, which the
# Windows agents do not run.
#
# On a pull request the unit tests run through the test selection, as in the Linux and macOS jobs:
# only the tests the committed test-selection.json selects in its dotnet.solution section, if it was
# made for exactly this working tree (rust/xtask/src/test_selection.rs); otherwise every test runs.
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
    $env:TEST_SELECTION = 'test-selection.json'
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
$failed = @()
foreach ($tfm in 'net462', 'net8.0', 'net10.0') {
    Write-Output "== unit tests, $tfm, $Platform"
    # `{}` is where selected-run puts the --filter of the selected tests, or nothing when every test
    # runs; it runs no test when dotnet.solution selects none.
    Push-Location $rust
    try {
        Invoke-Native $selection (@('selected-run', '-C', $repo, '--section', 'dotnet.solution', '--dotnet', '--',
                $dotnet, 'test', (Join-Path $repo 'dotnet\tests\Confluent.Kafka.UnitTests\Confluent.Kafka.UnitTests.csproj'), '--no-build', '-f', $tfm,
                '--blame-hang-timeout', '15m', '--blame-hang-dump-type', 'none') + $properties + @('{}'))
    } catch {
        Write-Output "$_"
        $failed += $tfm
    } finally {
        Pop-Location
    }
}
if ($failed) {
    throw "The unit tests failed on $($failed -join ', ') ($Platform)"
}
Write-Output "The selected unit tests passed on net462, net8.0 and net10.0 ($Platform)"
