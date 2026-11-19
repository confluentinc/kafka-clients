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
# The start of every Windows job of semaphore.yml and release.yml, the first command of each Windows block's task
# prologue: what .semaphore/job-start.sh does for the Linux and macOS jobs, in PowerShell, the
# shell of the Windows agents. Python is not set up: no Windows job uses it.

$ErrorActionPreference = 'Stop'

# A "Run on branch" override: see .semaphore/job-start.sh.
if ($env:SEMAPHORE_GIT_BRANCH_CHECKOUT) {
    Write-Output "SEMAPHORE_GIT_BRANCH_CHECKOUT=$env:SEMAPHORE_GIT_BRANCH_CHECKOUT -- switching working tree"
    git fetch origin $env:SEMAPHORE_GIT_BRANCH_CHECKOUT
    if ($LASTEXITCODE -ne 0) {
        exit 1
    }
    git checkout -B $env:SEMAPHORE_GIT_BRANCH_CHECKOUT FETCH_HEAD
    if ($LASTEXITCODE -ne 0) {
        exit 1
    }
}

& (Join-Path $PSScriptRoot 'ci-result.ps1') running
exit $LASTEXITCODE
