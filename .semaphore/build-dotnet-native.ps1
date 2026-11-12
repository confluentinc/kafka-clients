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
# Builds the native library of one Windows platform for the Confluent.Kafka NuGet package: the
# Windows counterpart of build-dotnet-native.sh, for the "Release (.NET): native library
# (Windows ...)" blocks of release.yml. verify-dotnet-windows.ps1 builds the library it tests
# with it too.
#
# It installs what it needs: NASM (aws-lc assembles its Windows sources with it, on both
# platforms) and Rust, at the version of rust/rust-toolchain.toml. The Visual Studio C++ tools come
# with the agent. The library links the C runtime statically (rust/.cargo/config.toml), which is
# checked, so an application needs no Visual C++ Redistributable.
#
# Usage, from the repository root:
#   .\.semaphore\build-dotnet-native.ps1 -Platform x64|x86 -OutDir <dir>
# Writes <dir>\<rid>\confluent_kafka.dll and <dir>\<rid>\confluent_kafka_bindings.h, the header of
# that library's C ABI. <dir> must not exist or be empty. The library also stays in
# rust\target\<triple>\release, where the library project reads it with -p:CargoTargetTriple.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('x64', 'x86')]
    [string] $Platform,
    [Parameter(Mandatory = $true)]
    [string] $OutDir
)

. (Join-Path $PSScriptRoot 'windows-tools.ps1')

$NasmVersion = '2.16.03'
# nasm.us publishes no checksums; this is the SHA-256 of the archive it served when it was pinned.
$NasmSha256 = '3ee4782247bcb874378d02f7eab4e294a84d3d15f3f6ee2de2f47a46aa7226e6'

$triple = $WindowsTriples[$Platform]
$rid = $WindowsRids[$Platform]
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if ((Test-Path $OutDir) -and (Get-ChildItem -Force $OutDir | Select-Object -First 1)) {
    throw "$OutDir is not empty"
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
$nasmZip = Join-Path $ToolsDir "nasm-$NasmVersion-win64.zip"
Invoke-Retry { Get-VerifiedDownload -Uri "https://www.nasm.us/pub/nasm/releasebuilds/$NasmVersion/win64/nasm-$NasmVersion-win64.zip" -Path $nasmZip -Sha256 $NasmSha256 }
Expand-Archive -Path $nasmZip -DestinationPath $ToolsDir -Force
$env:PATH = (Join-Path $ToolsDir "nasm-$NasmVersion") + ";$env:PATH"
Invoke-Native nasm @('-v')

Write-Output '== Rust'
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
$env:PATH = "$cargoBin;$env:PATH"
if (-not (Get-Command rustup -ErrorAction SilentlyContinue)) {
    $rustupInit = Join-Path $ToolsDir 'rustup-init.exe'
    $rustupUri = 'https://static.rust-lang.org/rustup/dist/x86_64-pc-windows-msvc/rustup-init.exe'
    # The checksum comes from the same server; it guards against a corrupt download.
    Invoke-Retry {
        Invoke-WebRequest -Uri "$rustupUri.sha256" -OutFile "$rustupInit.sha256" -UseBasicParsing
        $rustupSha256 = ((Get-Content "$rustupInit.sha256" -Raw).Trim() -split '\s+')[0]
        Get-VerifiedDownload -Uri $rustupUri -Path $rustupInit -Sha256 $rustupSha256
    }
    Invoke-Native $rustupInit @('-y', '--no-modify-path', '--profile', 'minimal', '--default-toolchain', 'none')
}
$toolchainFile = Get-Content (Join-Path $repo 'rust\rust-toolchain.toml') -Raw
if ($toolchainFile -notmatch '(?m)^channel\s*=\s*"([^"]+)"') {
    throw 'No channel in rust/rust-toolchain.toml'
}
$channel = $Matches[1]
# Windows can still hold a file rustup has just extracted (os error 32, as when a scanner reads
# it), and rustup then rolls the install back; the install is retried.
Invoke-Retry { Invoke-Native rustup @('toolchain', 'install', $channel, '--profile', 'minimal', '--component', 'clippy', '--component', 'rustfmt', '--target', $triple) }

Write-Output "== Native library ($triple)"
# A RUSTFLAGS variable would replace the static C runtime flags of rust/.cargo/config.toml.
Remove-Item Env:RUSTFLAGS -ErrorAction SilentlyContinue
# The header is left in place: Cargo reruns build.rs, which writes it, whenever src\ or
# cbindgen.toml changes, and skips it on a cached build, which would then find no header.
$header = Join-Path $repo 'rust\target\include\confluent_kafka_bindings.h'
Push-Location (Join-Path $repo 'rust')
try {
    # A scanner can hold a build output for a moment (os error 32, or LNK1114 from the linker), which
    # fails the build; the build is retried, and Cargo keeps what it already built. A real build error
    # also runs three times before the job fails.
    Invoke-Retry { Invoke-Native cargo @('build', '--locked', '--features', 'ffi', '--release', '--target', $triple) }
} finally {
    Pop-Location
}
$library = Join-Path $repo "rust\target\$triple\release\confluent_kafka.dll"
if (-not (Test-Path $header)) {
    throw "the build wrote no $header"
}
Write-Output "$library, $((Get-Item $library).Length) bytes"
$dumpbin = & $vswhere -latest -products * -find 'VC\Tools\MSVC\**\bin\Hostx64\x64\dumpbin.exe' | Select-Object -First 1
if (-not $dumpbin) {
    throw 'dumpbin.exe not found: the C runtime dependency of confluent_kafka.dll cannot be checked'
}
$dependents = & $dumpbin /nologo /dependents $library
$dependents | Write-Output
if ($LASTEXITCODE -ne 0) {
    throw "dumpbin exited with $LASTEXITCODE"
}
# Every Windows DLL imports KERNEL32.dll; without it the listing was not read.
if (-not ($dependents | Where-Object { $_ -match '(?i)^\s*kernel32\.dll\s*$' })) {
    throw 'dumpbin listed no KERNEL32.dll import: the C runtime dependency of confluent_kafka.dll was not checked'
}
$crt = $dependents | Where-Object { $_ -match '(?i)^\s*(vcruntime|msvcp|ucrtbase|api-ms-win-crt-)' }
if ($crt) {
    throw "confluent_kafka.dll imports the C runtime ($($crt.Trim() -join ', ')): it must link it statically"
}

$target = Join-Path $OutDir $rid
New-Item -ItemType Directory -Force -Path $target | Out-Null
Copy-Item $library, $header -Destination $target
# The pack block checks the library again with check-nupkg.py, with the others.
Get-ChildItem $target | ForEach-Object { Write-Output ("{0}  {1}" -f (Get-FileHash -Algorithm SHA256 $_.FullName).Hash.ToLowerInvariant(), $_.FullName) }
