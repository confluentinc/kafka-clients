#!/usr/bin/env bash
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
# Records the result of a Linux or macOS job of semaphore.yml for create-release-tag.yml, as
# ci-result.ps1 does for the Windows jobs: `<result> <commit tested> <job id>` in
# ci-result/<job name> in the workflow's artifacts, the job name reduced to the characters that are
# safe in an artifact path (LC_ALL=C, so that Linux and macOS (BSD tr) map multibyte characters
# such as the em dash identically; create-release-tag.yml applies the same mapping). Each Linux and
# macOS block calls it from its own epilogue; job-start.sh records `running` when the job starts.
#
# A failed upload produces only a warning: the job then stays recorded as running (on a master or
# release branch build job-start.sh guarantees that it was recorded), and create-release-tag.yml
# refuses to tag the commit.
#
# Usage: .semaphore/ci-result.sh passed|failed

case "${1:-}" in
  passed | failed) ;;
  *)
    echo "usage: $0 passed|failed" >&2
    exit 2
    ;;
esac

marker=$(printf '%s' "${SEMAPHORE_JOB_NAME}" | LC_ALL=C tr -c 'A-Za-z0-9._-' '_')
result_file=$(mktemp) \
  && printf '%s %s %s\n' "$1" "$(git rev-parse HEAD 2>/dev/null || echo unknown)" "${SEMAPHORE_JOB_ID}" > "${result_file}" \
  && artifact push workflow "${result_file}" --destination "ci-result/${marker}" --force \
  || echo "Warning: could not record this job's result; the job stays recorded as running, which create-release-tag.yml refuses"
