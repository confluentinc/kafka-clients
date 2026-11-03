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
# TEMPORARY probe, to be removed: reports whether this Windows agent can run Linux containers
# through WSL 2 (Docker Engine inside a WSL 2 distro), and whether WSL can be enabled without a
# reboot. It prints what it finds and never fails the job.

$ErrorActionPreference = 'Continue'
$env:WSL_UTF8 = '1'

function Section([string] $title) {
    Write-Output ''
    Write-Output "==================== $title ===================="
}

# Runs an executable with a time limit, printing its output and exit code. wsl.exe can wait forever
# when its VM cannot start, so nothing here runs without a limit.
function Invoke-Exe([string] $file, [string] $arguments, [int] $timeoutSeconds = 300) {
    Write-Output "> $file $arguments"
    $out = [System.IO.Path]::GetTempFileName()
    $err = [System.IO.Path]::GetTempFileName()
    try {
        $start = @{
            FilePath = $file; NoNewWindow = $true; PassThru = $true
            RedirectStandardOutput = $out; RedirectStandardError = $err
        }
        if ($arguments) {
            $start.ArgumentList = $arguments
        }
        $p = Start-Process @start
        # Without the handle taken now, ExitCode reads empty once the process has exited.
        $null = $p.Handle
        if (-not $p.WaitForExit($timeoutSeconds * 1000)) {
            Write-Output "  (still running after $timeoutSeconds s; killed)"
            try { $p.Kill() } catch { }
        } else {
            Write-Output "  exit code: $($p.ExitCode)"
        }
        Get-Content $out, $err | ForEach-Object { "  $_" }
    } catch {
        Write-Output "  failed to start: $_"
    } finally {
        Remove-Item $out, $err -ErrorAction SilentlyContinue
    }
}

function Invoke-Safe([string] $label, [scriptblock] $block) {
    Write-Output "> $label"
    try {
        & $block 2>&1 | Out-String -Width 200 | ForEach-Object { $_.TrimEnd() }
    } catch {
        Write-Output "  error: $_"
    }
}

Section 'Agent'
Invoke-Safe 'OS' {
    Get-CimInstance Win32_OperatingSystem | Select-Object Caption, Version, BuildNumber
}
Invoke-Safe 'Administrator' {
    ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator)
}
Invoke-Safe 'EC2 instance type (IMDSv2)' {
    $token = Invoke-RestMethod -Method Put -Uri 'http://169.254.169.254/latest/api/token' `
        -Headers @{ 'X-aws-ec2-metadata-token-ttl-seconds' = '60' } -TimeoutSec 5
    Invoke-RestMethod -Uri 'http://169.254.169.254/latest/meta-data/instance-type' `
        -Headers @{ 'X-aws-ec2-metadata-token' = $token } -TimeoutSec 5
}

Section 'Virtualization'
Invoke-Safe 'Processor' {
    Get-CimInstance Win32_Processor | Select-Object Name, VirtualizationFirmwareEnabled, `
        VMMonitorModeExtensions, SecondLevelAddressTranslationExtensions
}
Invoke-Safe 'Hypervisor present' {
    (Get-CimInstance Win32_ComputerSystem).HypervisorPresent
}
Invoke-Exe 'systeminfo.exe' $null 120

Section 'Windows features'
Invoke-Safe 'Optional features' {
    Get-WindowsOptionalFeature -Online | Where-Object {
        $_.FeatureName -match 'Subsystem-Linux|VirtualMachinePlatform|Hyper-V|Containers'
    } | Select-Object FeatureName, State
}

Section 'Existing WSL and Docker'
Invoke-Safe 'wsl.exe on PATH' { Get-Command wsl.exe -ErrorAction Stop | Select-Object Source }
Invoke-Exe 'wsl.exe' '--version' 60
Invoke-Exe 'wsl.exe' '--status' 60
Invoke-Exe 'wsl.exe' '--list --verbose' 60
Invoke-Safe 'docker on PATH' { Get-Command docker -ErrorAction Stop | Select-Object Source }
Invoke-Safe 'docker service' { Get-Service docker -ErrorAction Stop | Select-Object Name, Status }

Section 'Enable the WSL features without a reboot'
# RestartNeeded says whether the features work only after a reboot.
Invoke-Safe 'Enable-WindowsOptionalFeature -NoRestart' {
    Enable-WindowsOptionalFeature -Online -All -NoRestart `
        -FeatureName Microsoft-Windows-Subsystem-Linux, VirtualMachinePlatform |
        Select-Object Online, RestartNeeded
}
Invoke-Safe 'Feature state afterwards' {
    Get-WindowsOptionalFeature -Online -FeatureName Microsoft-Windows-Subsystem-Linux, VirtualMachinePlatform |
        Select-Object FeatureName, State
}

Section 'Install WSL and a distro without a reboot'
Invoke-Exe 'wsl.exe' '--install --no-distribution --web-download' 600
Invoke-Exe 'wsl.exe' '--version' 60
Invoke-Exe 'wsl.exe' '--status' 60
Invoke-Exe 'wsl.exe' '--install --distribution Ubuntu --web-download --no-launch' 900
Invoke-Exe 'wsl.exe' '--list --verbose' 60

Section 'Run Linux under WSL 2'
Invoke-Exe 'wsl.exe' '--set-default-version 2' 60
Invoke-Exe 'wsl.exe' '--distribution Ubuntu --user root -- uname -a' 300
Invoke-Exe 'wsl.exe' '--list --verbose' 60

Section 'Run Linux under WSL 1'
# WSL 1 needs no virtualization, but it has no Linux kernel of its own, so Docker cannot run on it.
Invoke-Exe 'wsl.exe' '--set-version Ubuntu 1' 600
Invoke-Exe 'wsl.exe' '--distribution Ubuntu --user root -- uname -a' 300

Section 'Docker Engine inside WSL 2'
Invoke-Exe 'wsl.exe' '--set-version Ubuntu 2' 600
Invoke-Exe 'wsl.exe' '--distribution Ubuntu --user root -- sh -c "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io >/dev/null && (service docker start || dockerd >/tmp/dockerd.log 2>&1 &) ; sleep 10; docker run --rm hello-world"' 900

Write-Output ''
Write-Output 'Probe finished.'
exit 0
