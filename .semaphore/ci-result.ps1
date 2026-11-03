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
# Records the result of a Windows job of semaphore.yml for create-release-tag.yml, as the bash
# global prologue (.semaphore/job-start.sh) and epilogue of semaphore.yml do for the other jobs:
# `<result> <commit tested> <job id>` in ci-result/<job name> in the workflow's artifacts. The job
# name is mapped byte by byte, as `LC_ALL=C tr -c 'A-Za-z0-9._-' '_'` maps it, so that an em dash
# gives the same three underscores on every agent.
#
# `running` fails the job if it cannot be recorded on a build of master or of a release branch
# <major>.<minor>.x, and only warns elsewhere; `passed` and `failed` only warn, and the job then
# stays recorded as running, which create-release-tag.yml refuses.
#
# Usage: .\.semaphore\ci-result.ps1 running|passed|failed

param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('running', 'passed', 'failed')]
    [string] $Result
)

$ErrorActionPreference = 'Stop'

$marker = -join ([System.Text.Encoding]::UTF8.GetBytes([string]$env:SEMAPHORE_JOB_NAME) | ForEach-Object {
        if (($_ -ge 0x30 -and $_ -le 0x39) -or ($_ -ge 0x41 -and $_ -le 0x5A) -or ($_ -ge 0x61 -and $_ -le 0x7A) `
                -or $_ -eq 0x2E -or $_ -eq 0x5F -or $_ -eq 0x2D) {
            [char]$_
        } else {
            '_'
        }
    })

$commit = $null
try {
    $commit = git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) {
        $commit = $null
    }
} catch {
    $commit = $null
}
if (-not $commit) {
    $commit = 'unknown'
}

$recorded = $false
try {
    $resultFile = [System.IO.Path]::GetTempFileName()
    [System.IO.File]::WriteAllText($resultFile, "$Result $commit $env:SEMAPHORE_JOB_ID`n", (New-Object System.Text.UTF8Encoding $false))
    artifact push workflow $resultFile --destination "ci-result/$marker" --force
    $recorded = $LASTEXITCODE -eq 0
} catch {
    Write-Output "$_"
}

if (-not $recorded) {
    if ($Result -ne 'running') {
        Write-Output "Warning: could not record this job's result; the job stays recorded as running, which create-release-tag.yml refuses"
    } elseif ($env:SEMAPHORE_GIT_REF_TYPE -ceq 'branch' `
            -and ($env:SEMAPHORE_GIT_BRANCH -ceq 'master' -or $env:SEMAPHORE_GIT_BRANCH -cmatch '^[0-9]+\.[0-9]+\.x$')) {
        [Console]::Error.WriteLine('Could not record that this job started, so an earlier result of this job could still be counted by create-release-tag.yml; rerun the job')
        exit 1
    } else {
        Write-Output 'Warning: could not record that this job started; an earlier result of this job remains recorded'
    }
}
exit 0
