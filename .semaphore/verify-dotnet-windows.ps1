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
# Windows platform, the platforms of the Confluent.Kafka package being win-x64 and win-x86, then
# builds the .NET solution and runs the unit tests, which drive the mock clients, on net462,
# net8.0 and net10.0, all in that platform's process. The broker-backed tests do not run: they
# need Linux containers, which the Windows agents do not run.
#
# It installs what it needs: NASM (aws-lc assembles its Windows sources with it, on both
# platforms), Rust, at the version of rust/rust-toolchain.toml, and the .NET 10 SDK with the .NET 8
# runtime, both for the platform tested, so that the tests run in an x86 process on x86. The
# Visual Studio C++ tools come with the agent. The library links the C runtime statically
# (rust/.cargo/config.toml), which is checked.
#
# Usage: .\.semaphore\verify-dotnet-windows.ps1 -Platform x64|x86

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('x64', 'x86')]
    [string] $Platform
)

$ErrorActionPreference = 'Stop'
# Windows PowerShell's progress bar makes Invoke-WebRequest many times slower.
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$NasmVersion = '2.16.03'
# nasm.us publishes no checksums; this is the SHA-256 of the archive it served when it was pinned.
$NasmSha256 = '3ee4782247bcb874378d02f7eab4e294a84d3d15f3f6ee2de2f47a46aa7226e6'

$triple = @{ x64 = 'x86_64-pc-windows-msvc'; x86 = 'i686-pc-windows-msvc' }[$Platform]
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$tools = Join-Path ([System.IO.Path]::GetTempPath()) 'confluent-kafka-ci'
New-Item -ItemType Directory -Force -Path $tools | Out-Null

# Runs a native command and fails on a non-zero exit code. Its standard error is not an error:
# git and cargo write their progress there.
function Invoke-Native {
    param([string] $Command, [string[]] $Arguments)
    Write-Output "> $Command $($Arguments -join ' ')"
    $saved = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Command @Arguments
    } finally {
        $ErrorActionPreference = $saved
    }
    if ($LASTEXITCODE -ne 0) {
        throw "$Command exited with $LASTEXITCODE"
    }
}

function Get-VerifiedDownload {
    param([string] $Uri, [string] $Path, [string] $Sha256)
    Invoke-WebRequest -Uri $Uri -OutFile $Path -UseBasicParsing
    $actual = (Get-FileHash -Algorithm SHA256 -Path $Path).Hash
    if ($actual -ne $Sha256.ToUpperInvariant()) {
        throw "$Uri has SHA-256 $actual, not $Sha256"
    }
}

Write-Output "== Windows $Platform ($triple) on $([Environment]::OSVersion.VersionString), $([Environment]::ProcessorCount) processors, PowerShell $($PSVersionTable.PSVersion)"
foreach ($tool in 'git', 'bash', 'cmake', 'nasm', 'rustup', 'cargo', 'dotnet', 'python', 'docker') {
    $found = Get-Command $tool -ErrorAction SilentlyContinue | Select-Object -First 1
    Write-Output ("  {0,-7} {1}" -f $tool, $(if ($found) { $found.Source } else { '(not found)' }))
}
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not (Test-Path $vswhere)) {
    throw "vswhere.exe not found at ${vswhere}: the agent has no Visual Studio installer"
}
$vs = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
if (-not $vs) {
    throw 'No Visual Studio with the C++ x86/x64 tools'
}
Write-Output "  Visual Studio with the C++ tools: $vs"

Write-Output "== NASM $NasmVersion"
$nasmZip = Join-Path $tools "nasm-$NasmVersion-win64.zip"
Get-VerifiedDownload -Uri "https://www.nasm.us/pub/nasm/releasebuilds/$NasmVersion/win64/nasm-$NasmVersion-win64.zip" -Path $nasmZip -Sha256 $NasmSha256
Expand-Archive -Path $nasmZip -DestinationPath $tools -Force
$env:PATH = (Join-Path $tools "nasm-$NasmVersion") + ";$env:PATH"
Invoke-Native nasm @('-v')

Write-Output '== Rust'
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
$env:PATH = "$cargoBin;$env:PATH"
if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    $rustupInit = Join-Path $tools 'rustup-init.exe'
    $rustupUri = 'https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe'
    # The checksum comes from the same server; it guards against a corrupt download.
    Invoke-WebRequest -Uri "$rustupUri.sha256" -OutFile "$rustupInit.sha256" -UseBasicParsing
    $rustupSha256 = ((Get-Content "$rustupInit.sha256" -Raw).Trim() -split '\s+')[0]
    Get-VerifiedDownload -Uri $rustupUri -Path $rustupInit -Sha256 $rustupSha256
    Invoke-Native $rustupInit @('-y', '--no-modify-path', '--profile', 'minimal', '--default-toolchain', 'none')
}
$toolchainFile = Get-Content (Join-Path $repo 'rust\rust-toolchain.toml') -Raw
if ($toolchainFile -notmatch '(?m)^channel\s*=\s*"([^"]+)"') {
    throw 'No channel in rust/rust-toolchain.toml'
}
$channel = $Matches[1]
Invoke-Native rustup @('toolchain', 'install', $channel, '--profile', 'minimal', '--component', 'clippy', '--component', 'rustfmt', '--target', $triple)

Write-Output "== Native library ($triple)"
# A RUSTFLAGS variable would replace the static C runtime flags of rust/.cargo/config.toml.
Remove-Item Env:RUSTFLAGS -ErrorAction SilentlyContinue
Push-Location (Join-Path $repo 'rust')
try {
    Invoke-Native cargo @('build', '--locked', '--features', 'ffi', '--release', '--target', $triple)
} finally {
    Pop-Location
}
$library = Join-Path $repo "rust\target\$triple\release\confluent_kafka.dll"
Write-Output "$library, $((Get-Item $library).Length) bytes"
$dumpbin = & $vswhere -latest -products * -find 'VC\Tools\MSVC\**\bin\Hostx64\x64\dumpbin.exe' | Select-Object -First 1
if ($dumpbin) {
    $dependents = & $dumpbin /nologo /dependents $library
    $dependents | Write-Output
    $crt = $dependents | Where-Object { $_ -match '(?i)^\s*(vcruntime|msvcp|ucrtbase|api-ms-win-crt-)' }
    if ($crt) {
        throw "confluent_kafka.dll imports the C runtime ($($crt.Trim() -join ', ')): it must link it statically"
    }
} else {
    Write-Output 'Warning: dumpbin.exe not found; the C runtime dependency is not checked'
}

Write-Output "== .NET ($Platform)"
$dotnetRoot = Join-Path $tools "dotnet-$Platform"
$dotnetInstall = Join-Path $tools 'dotnet-install.ps1'
Invoke-WebRequest -Uri 'https://dot.net/v1/dotnet-install.ps1' -OutFile $dotnetInstall -UseBasicParsing
# The .NET 10 SDK builds every TFM and runs net10.0; the .NET 8 runtime runs net8.0, which does not
# roll forward to .NET 10. net462 runs on the agent's .NET Framework.
Invoke-Native powershell @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $dotnetInstall, '-Channel', '10.0', '-Architecture', $Platform, '-InstallDir', $dotnetRoot)
Invoke-Native powershell @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $dotnetInstall, '-Channel', '8.0', '-Runtime', 'dotnet', '-Architecture', $Platform, '-InstallDir', $dotnetRoot)
$env:DOTNET_ROOT = $dotnetRoot
$env:PATH = "$dotnetRoot;$env:PATH"
$env:DOTNET_MULTILEVEL_LOOKUP = '0'
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:DOTNET_NOLOGO = '1'
$dotnet = Join-Path $dotnetRoot 'dotnet.exe'
Invoke-Native $dotnet @('--info')

# Some unit tests read the Java sources in the kafka submodule.
Invoke-Native git @('-C', $repo, 'config', 'core.longpaths', 'true')
Invoke-Native git @('-C', $repo, 'submodule', 'update', '--init', '--depth=1', 'kafka')

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
