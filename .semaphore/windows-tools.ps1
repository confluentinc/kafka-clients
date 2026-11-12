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
# The helpers of the Windows scripts of semaphore.yml (build-dotnet-native.ps1,
# verify-dotnet-windows.ps1, test-nupkg.ps1), dot-sourced by each: running native commands,
# verified downloads, retries, and the .NET install of one platform. Windows PowerShell 5.1, the
# shell of the Windows agents.

$ErrorActionPreference = 'Stop'
# Windows PowerShell's progress bar makes Invoke-WebRequest many times slower.
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

# The package's Windows platforms: the Rust target and the RID of each.
$WindowsTriples = @{ x64 = 'x86_64-pc-windows-msvc'; x86 = 'i686-pc-windows-msvc' }
$WindowsRids = @{ x64 = 'win-x64'; x86 = 'win-x86' }

$ToolsDir = Join-Path ([System.IO.Path]::GetTempPath()) 'confluent-kafka-ci'
New-Item -ItemType Directory -Force -Path $ToolsDir | Out-Null

# Runs a native command and fails on a non-zero exit code. Its standard error is not an error:
# git, cargo and dotnet write their progress there.
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

# Runs a script block up to $Attempts times, $DelaySeconds apart, for steps that can fail for a moment:
# a download, or a file Windows still holds. Any failure is retried, a lasting one too.
function Invoke-Retry {
    param([scriptblock] $Action, [int] $Attempts = 3, [int] $DelaySeconds = 30)
    for ($attempt = 1; ; $attempt++) {
        try {
            & $Action
            return
        } catch {
            if ($attempt -ge $Attempts) {
                throw
            }
            Write-Output "$_; retrying in $DelaySeconds seconds (attempt $attempt of $Attempts)"
            Start-Sleep -Seconds $DelaySeconds
        }
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

# Installs the .NET 10 SDK and the .NET 8 runtime for $Platform into a directory of their own, and
# points this process at them, so that dotnet runs in a process of that platform. Returns the path
# of dotnet.exe. The .NET 10 SDK builds every TFM and runs net10.0; the .NET 8 runtime runs net8.0,
# which does not roll forward to .NET 10. net462 runs on the agent's .NET Framework.
function Install-DotNet {
    param([string] $Platform)
    $root = Join-Path $ToolsDir "dotnet-$Platform"
    $installer = Join-Path $ToolsDir 'dotnet-install.ps1'
    # The output goes to the host, so that the function returns only the path.
    Invoke-Retry { Invoke-WebRequest -Uri 'https://dot.net/v1/dotnet-install.ps1' -OutFile $installer -UseBasicParsing } | Out-Host
    Invoke-Retry { Invoke-Native powershell @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $installer, '-Channel', '10.0', '-Architecture', $Platform, '-InstallDir', $root) } | Out-Host
    Invoke-Retry { Invoke-Native powershell @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $installer, '-Channel', '8.0', '-Runtime', 'dotnet', '-Architecture', $Platform, '-InstallDir', $root) } | Out-Host
    $env:DOTNET_ROOT = $root
    $env:PATH = "$root;$env:PATH"
    $env:DOTNET_MULTILEVEL_LOOKUP = '0'
    $env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
    $env:DOTNET_NOLOGO = '1'
    $env:DOTNET_SKIP_FIRST_TIME_EXPERIENCE = '1'
    $dotnet = Join-Path $root 'dotnet.exe'
    Invoke-Native $dotnet @('--info') | Out-Host
    return $dotnet
}
