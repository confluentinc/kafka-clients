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
# Tests the Confluent.Kafka NuGet package as a user receives it on one Windows platform: the
# Windows counterpart of test-nupkg.sh, for the "Release (.NET): package verification
# (Windows ...)" blocks of semaphore.yml. The smoke project (dotnet/tools/PackageSmoke), a copy
# outside the repository, references the package by its exact version from that one feed
# (packageSourceMapping), with an empty NuGet cache, so nothing but the package can be used. It is
# built for the platform and run in a process of it, on net8.0, net10.0 and the agent's .NET
# Framework (net462), with no RID of its own, as users build it: on .NET Framework the SDK gives an
# application the RID of its PlatformTarget, and NuGet copies that RID's native library beside it.
# A second .NET Framework build is AnyCPU. On x64 it sets no platform, as most applications do: the
# SDK then gives it the win-x86 RID, so NuGet copies the win-x86 library beside it, yet it runs as a
# 64-bit process, the reported failure. On x86 it sets AnyCPU and Prefer32Bit, so it has no RID and
# nothing beside it, and runs as a 32-bit process. Either way it must load the library of its
# process from runtimes\<rid>\native, where the package's build targets copy both Windows libraries. Each run must load one copy of the library, from where it is expected. A
# self-contained `dotnet publish` for the platform's RID must carry the native library and run. The
# binding's unit tests then run against the package, on the same three frameworks, with no RID, as
# a test project is built; on .NET Framework they load the library the build targets copy. The
# unit-test project's
# -p:ConfluentKafkaPackageVersion swaps its project reference for the package, so the assembly and
# the native library both come from it. Some of them read the Java sources of the kafka submodule,
# which must be checked out.
#
# Usage, from the repository root:
#   .\.semaphore\test-nupkg.ps1 -Platform x64|x86 -PackageDir <dir> -Header <abi-header> [-ChecksumFile <file>]
#       Tests the package in <dir>, which must hold Confluent.Kafka.<version>.nupkg and nothing
#       else. <abi-header> is the confluent_kafka_bindings.h of the platform's native library, for
#       the unit tests' ABI conformance test. With <file>, writes `<sha256>  <package>` to it once
#       every test has passed.

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('x64', 'x86')]
    [string] $Platform,
    [Parameter(Mandatory = $true)]
    [string] $PackageDir,
    [Parameter(Mandatory = $true)]
    [string] $Header,
    [string] $ChecksumFile = ''
)

. (Join-Path $PSScriptRoot 'windows-tools.ps1')

$rid = $WindowsRids[$Platform]
$bits = @{ x64 = '64'; x86 = '32' }[$Platform]
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$tfms = 'net462', 'net8.0', 'net10.0'

$listing = @(Get-ChildItem -Force $PackageDir)
if ($listing.Count -ne 1 -or $listing[0].Name -notmatch '^Confluent\.Kafka\.(.+)\.nupkg$') {
    throw "$PackageDir must hold Confluent.Kafka.<version>.nupkg and nothing else, but holds: $($listing.Name -join ' ')"
}
$package = $listing[0].FullName
$packageName = $listing[0].Name
$version = $Matches[1]
# The release versions; see check-nupkg.py's VERSION_PATTERN.
if ($version -cnotmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((alpha|beta|rc)\.(0|[1-9][0-9]*)(\.dev\.(0|[1-9][0-9]*))?|dev\.(0|[1-9][0-9]*)))?$') {
    throw "'$version' is not a release version"
}
$Header = (Resolve-Path $Header).Path
if ($ChecksumFile -and $ChecksumFile.StartsWith((Resolve-Path $PackageDir).Path, [StringComparison]::OrdinalIgnoreCase)) {
    throw '-ChecksumFile must be outside -PackageDir'
}
if (-not (Test-Path (Join-Path $repo 'kafka\clients\src\main\java'))) {
    throw 'the unit tests read the Java sources of the kafka submodule; run git submodule update --init kafka'
}

$dotnet = Install-DotNet $Platform

# Recorded before the tests, and written only if they all pass.
$checksum = "$((Get-FileHash -Algorithm SHA256 $package).Hash.ToLowerInvariant())  $packageName"

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("test-nupkg-" + [System.IO.Path]::GetRandomFileName())
New-Item -ItemType Directory -Path $work | Out-Null
# An empty NuGet cache, so that a package of the same version cached earlier cannot be used.
$env:NUGET_PACKAGES = Join-Path $work 'packages'
$env:NUGET_HTTP_CACHE_PATH = Join-Path $work 'http-cache'
$feed = Join-Path $work 'feed'
New-Item -ItemType Directory -Path $feed | Out-Null
Copy-Item $package $feed
Write-Output "Testing Confluent.Kafka $version ($Platform) from $feed"

# The nuget.config of a project in $Directory that takes Confluent.Kafka from the feed only and
# every other package from nuget.org.
function Write-NuGetConfig {
    param([string] $Directory)
    $config = @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="confluent-kafka" value="$feed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="confluent-kafka">
      <package pattern="Confluent.Kafka" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
  <fallbackPackageFolders>
    <clear />
  </fallbackPackageFolders>
</configuration>
"@
    [System.IO.File]::WriteAllText((Join-Path $Directory 'nuget.config'), $config, (New-Object System.Text.UTF8Encoding $false))
}

# Runs the smoke program and checks that it ran in a process of this platform and, with $Native,
# that it loaded that native library, a path relative to the program's folder.
function Invoke-Smoke {
    param([string] $Command, [string[]] $Arguments, [string] $Native = '')
    Invoke-Native $Command $Arguments | Tee-Object -Variable output
    if (-not ($output | Where-Object { $_ -match "^Confluent\.Kafka .* $bits-bit process" })) {
        throw "the smoke program did not run in a $bits-bit process"
    }
    if ($Native) {
        $loaded = @($output | Where-Object { $_ -match '^OK: loaded (.+) of the application$' } | ForEach-Object { $Matches[1] })
        if ($loaded.Count -ne 1 -or $loaded[0] -ne $Native) {
            throw "the smoke program loaded $($loaded -join ', '), not $Native"
        }
    }
}

# The smoke project, outside the repository. PlatformTarget makes the net462 program a process of
# this platform, which picks the native library of this RID from the package.
$smoke = Join-Path $work 'smoke'
New-Item -ItemType Directory -Path $smoke | Out-Null
Copy-Item (Join-Path $repo 'dotnet\tools\PackageSmoke\*') $smoke -Include 'PackageSmoke.csproj', 'Program.cs', 'Directory.Build.props'
Write-NuGetConfig $smoke
# A restore takes no configuration.
$smokeProperties = @("-p:ConfluentKafkaVersion=$version", "-p:PlatformTarget=$Platform")
Push-Location $smoke
try {
    Invoke-Retry { Invoke-Native $dotnet (@('restore', '--configfile', 'nuget.config') + $smokeProperties) }
    Invoke-Native $dotnet (@('build', '--no-restore', '-c', 'Release') + $smokeProperties)
} finally {
    Pop-Location
}

# The package restored is the one under test, byte for byte.
$restored = Join-Path $env:NUGET_PACKAGES "confluent.kafka\$($version.ToLowerInvariant())\confluent.kafka.$($version.ToLowerInvariant()).nupkg"
if (-not (Test-Path $restored)) {
    throw "Confluent.Kafka $version was not restored into $env:NUGET_PACKAGES"
}
if ((Get-FileHash -Algorithm SHA256 $restored).Hash -ne (Get-FileHash -Algorithm SHA256 $package).Hash) {
    throw "the restored Confluent.Kafka $version is not the package under test"
}

foreach ($tfm in $tfms) {
    Write-Output "== smoke test, $tfm, $Platform"
    $bin = Join-Path $smoke "bin\Release\$tfm"
    if ($tfm -eq 'net462') {
        $exe = @(Get-ChildItem -Recurse -Path $bin -Filter PackageSmoke.exe)
        if ($exe.Count -ne 1) {
            throw "found $($exe.Count) PackageSmoke.exe under $bin, not 1"
        }
        if (-not (Test-Path (Join-Path $exe[0].DirectoryName 'confluent_kafka.dll'))) {
            throw "the net462 smoke program has no confluent_kafka.dll beside it: NuGet did not copy the package's $rid library"
        }
        Invoke-Smoke $exe[0].FullName @($version) 'confluent_kafka.dll'
    } else {
        Invoke-Smoke $dotnet @((Join-Path $bin 'PackageSmoke.dll'), $version)
    }
}

# .NET Framework, AnyCPU: a 64-bit process on x64, a 32-bit one with Prefer32Bit on x86. Built in
# a copy of its own, so it shares no obj folder with the build above.
$anyCpu = @()
if ($Platform -eq 'x86') {
    $anyCpu = @('-p:PlatformTarget=AnyCPU', '-p:Prefer32Bit=true')
}
Write-Output "== smoke test, net462, AnyCPU$(if ($anyCpu.Count) { ' with Prefer32Bit' }), $Platform"
$smokeAnyCpu = Join-Path $work 'smoke-anycpu'
New-Item -ItemType Directory -Path $smokeAnyCpu | Out-Null
Copy-Item (Join-Path $smoke '*') $smokeAnyCpu -Include 'PackageSmoke.csproj', 'Program.cs', 'Directory.Build.props', 'nuget.config'
Push-Location $smokeAnyCpu
try {
    Invoke-Retry { Invoke-Native $dotnet (@('restore', '--configfile', 'nuget.config', "-p:ConfluentKafkaVersion=$version") + $anyCpu) }
    Invoke-Native $dotnet (@('build', '--no-restore', '-c', 'Release', '-f', 'net462', "-p:ConfluentKafkaVersion=$version") + $anyCpu)
} finally {
    Pop-Location
}
$exe = @(Get-ChildItem -Recurse -Path (Join-Path $smokeAnyCpu 'bin\Release\net462') -Filter PackageSmoke.exe)
if ($exe.Count -ne 1) {
    throw "found $($exe.Count) AnyCPU PackageSmoke.exe, not 1"
}
foreach ($windowsRid in $WindowsRids.Values) {
    if (-not (Test-Path (Join-Path $exe[0].DirectoryName "runtimes\$windowsRid\native\confluent_kafka.dll"))) {
        throw "the AnyCPU net462 smoke program has no runtimes\$windowsRid\native\confluent_kafka.dll: the package's build targets did not copy it"
    }
}
# The layout above: on x64 a library of the wrong platform beside the program, so that the run below
# reproduces the reported failure; on x86 none.
$adjacent = Test-Path (Join-Path $exe[0].DirectoryName 'confluent_kafka.dll')
if ($Platform -eq 'x64' -and -not $adjacent) {
    throw "the AnyCPU net462 smoke program has no confluent_kafka.dll beside it: the SDK no longer gives an AnyCPU application the win-x86 RID, so this run no longer reproduces a 64-bit process beside the win-x86 library"
}
if ($Platform -eq 'x86' -and $adjacent) {
    throw "the AnyCPU net462 smoke program built with Prefer32Bit has a confluent_kafka.dll beside it, which the expected load below does not account for"
}
Invoke-Smoke $exe[0].FullName @($version) "runtimes\$rid\native\confluent_kafka.dll"

Write-Output "== self-contained publish for $rid"
$publish = Join-Path $work 'publish'
Push-Location $smoke
try {
    Invoke-Retry { Invoke-Native $dotnet @('restore', '--configfile', 'nuget.config', '-r', $rid, "-p:ConfluentKafkaVersion=$version") }
    Invoke-Native $dotnet @('publish', '--no-restore', '-c', 'Release', '-f', 'net10.0', '-r', $rid, '--self-contained',
        "-p:ConfluentKafkaVersion=$version", '-o', $publish)
} finally {
    Pop-Location
}
if (-not (Test-Path (Join-Path $publish 'confluent_kafka.dll'))) {
    throw "the self-contained publish for $rid has no confluent_kafka.dll"
}
Invoke-Smoke (Join-Path $publish 'PackageSmoke.exe') @($version)

# The unit-test project and what it builds with, without the library's sources or any build
# output, so the package is the only Confluent.Kafka it can use.
$unit = Join-Path $work 'unit'
New-Item -ItemType Directory -Path (Join-Path $unit 'tests') | Out-Null
New-Item -ItemType Directory -Path (Join-Path $unit 'analyzers') | Out-Null
Copy-Item (Join-Path $repo 'dotnet\Directory.Build.props'), (Join-Path $repo 'dotnet\Confluent.Kafka.snk'), (Join-Path $repo 'dotnet\.editorconfig') $unit
Copy-Item -Recurse (Join-Path $repo 'dotnet\analyzers\Confluent.Kafka.Analyzers') (Join-Path $unit 'analyzers')
Copy-Item -Recurse (Join-Path $repo 'dotnet\tests\Confluent.Kafka.UnitTests') (Join-Path $unit 'tests')
Get-ChildItem -Recurse -Directory (Join-Path $unit 'tests'), (Join-Path $unit 'analyzers') | Where-Object { $_.Name -in 'bin', 'obj' } | Remove-Item -Recurse -Force
# Where the project looks for the kafka submodule, ..\..\..\kafka from its directory. A junction
# needs no administrator rights.
New-Item -ItemType Junction -Path (Join-Path $work 'kafka') -Target (Join-Path $repo 'kafka') | Out-Null
# The other repository files the tests read, at the same place relative to the submodule: the
# error hierarchy generator, the Java deprecation list and the library's sources, which a test
# scans. The project does not build from there, so the package stays its only Confluent.Kafka.
foreach ($dir in 'rust\xtask\src', 'design\current', 'dotnet\src\Confluent.Kafka') {
    New-Item -ItemType Directory -Path (Split-Path (Join-Path $work $dir)) -Force | Out-Null
    New-Item -ItemType Junction -Path (Join-Path $work $dir) -Target (Join-Path $repo $dir) | Out-Null
}
Write-NuGetConfig $unit
$project = Join-Path $unit 'tests\Confluent.Kafka.UnitTests\Confluent.Kafka.UnitTests.csproj'
$unitProperties = @("-p:ConfluentKafkaPackageVersion=$version", "-p:PlatformTarget=$Platform")
Invoke-Retry { Invoke-Native $dotnet (@('restore', '--configfile', (Join-Path $unit 'nuget.config'), $project) + $unitProperties) }
$unitProperties += @('-c', 'Release', "-p:AbiHeaderPath=$Header")
Invoke-Native $dotnet (@('build', '--no-restore', $project) + $unitProperties)
# A .NET Framework test project has no RID, so NuGet copies it no native library; the package's
# build targets copy both Windows libraries, and without them every net462 test would fail with
# DllNotFoundException.
$framework = Join-Path $unit 'tests\Confluent.Kafka.UnitTests\bin\Release\net462'
foreach ($windowsRid in $WindowsRids.Values) {
    if (-not (Test-Path (Join-Path $framework "runtimes\$windowsRid\native\confluent_kafka.dll"))) {
        throw "the net462 unit tests' output has no runtimes\$windowsRid\native\confluent_kafka.dll: the package's build targets did not copy it"
    }
}
$failed = @()
foreach ($tfm in $tfms) {
    Write-Output "== unit tests, $tfm, $Platform, against the package"
    try {
        Invoke-Native $dotnet (@('test', '--no-build', $project, '-f', $tfm, '--blame-hang-timeout', '15m', '--blame-hang-dump-type', 'none') + $unitProperties)
    } catch {
        Write-Output "$_"
        $failed += $tfm
    }
}
if ($failed) {
    throw "The unit tests failed against the package on $($failed -join ', ') ($Platform)"
}

if ($ChecksumFile) {
    [System.IO.File]::WriteAllText($ChecksumFile, "$checksum`n", (New-Object System.Text.UTF8Encoding $false))
    Write-Output 'Tested:'
    Write-Output $checksum
}
Write-Output "Confluent.Kafka $version passed every test ($Platform)"
