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
# The start of every Linux and macOS job of semaphore.yml and release.yml, sourced as the first command of each
# block's task prologue (`. .semaphore/job-start.sh`) so that its environment stays in the job's
# shell. The global prologue runs only `checkout`, because it also runs on the Windows agents,
# whose shell is PowerShell; the Windows blocks start with .semaphore/job-start.ps1 instead.
# A step that fails returns non-zero, which fails the job.

# Runs a command up to 5 times, 5 seconds apart: for a download, which can fail for a moment.
# Defined here, in the job's shell, for the job's commands and for the scripts the job sources
# (.semaphore/dependencies-macos.sh).
retry() {
    _n=1
    while [ "$_n" -le 5 ]; do
        if "$@"; then return 0; fi
        [ "$_n" -eq 5 ] && break
        echo "retry: attempt $_n/5 of '$*' failed; sleeping 5s" >&2
        _n=$((_n + 1))
        sleep 5
    done
    echo "retry: '$*' failed after 5 attempts" >&2
    return 1
}

# When the Task is triggered manually with a "Run on branch" override, Semaphore exposes the
# desired branch as SEMAPHORE_GIT_BRANCH_CHECKOUT but `checkout` still lands on the build's
# original ref. Switch to the requested branch so submodule init + pip install operate against
# the right tree. Semaphore's shallow single-branch clone doesn't create
# refs/remotes/origin/<other-branch>, so origin/<branch> isn't a valid start point. Use
# FETCH_HEAD (which the explicit fetch updates) and -B so re-runs reset an existing local branch
# instead of failing. Bash reads a sourced file whole before it runs it, so the switch does not
# change the rest of this file.
if [ -n "${SEMAPHORE_GIT_BRANCH_CHECKOUT}" ]; then
  echo "SEMAPHORE_GIT_BRANCH_CHECKOUT=${SEMAPHORE_GIT_BRANCH_CHECKOUT} -- switching working tree"
  git fetch origin "${SEMAPHORE_GIT_BRANCH_CHECKOUT}" || return 1
  git checkout -B "${SEMAPHORE_GIT_BRANCH_CHECKOUT}" FETCH_HEAD || return 1
fi

# Replaces this job's previous result with `running` (see ci-result.sh and "CI results" in semaphore.yml),
# so that a rerun that is still running, was stopped, or could not upload its result does not
# leave an earlier `passed` in place. On a build of master or of a release branch
# <major>.<minor>.x, the only kinds create-release-tag.yml tags, a job that cannot record
# `running` fails before it runs anything, because an earlier `passed` of this job would
# otherwise still count. Elsewhere it only warns, so that an artifact store outage does not fail
# pull requests.
marker=$(printf '%s' "${SEMAPHORE_JOB_NAME}" | LC_ALL=C tr -c 'A-Za-z0-9._-' '_')
if ! { result_file=$(mktemp) \
  && printf 'running %s %s\n' "$(git rev-parse HEAD 2>/dev/null || echo unknown)" "${SEMAPHORE_JOB_ID}" > "${result_file}" \
  && artifact push workflow "${result_file}" --destination "ci-result/${marker}" --force; }; then
  if [ "${SEMAPHORE_GIT_REF_TYPE}" = "branch" ] \
    && [[ "${SEMAPHORE_GIT_BRANCH}" == master || "${SEMAPHORE_GIT_BRANCH}" =~ ^[0-9]+\.[0-9]+\.x$ ]]; then
    echo "Could not record that this job started, so an earlier result of this job could still be counted by create-release-tag.yml; rerun the job" >&2
    return 1
  fi
  echo "Warning: could not record that this job started; an earlier result of this job remains recorded"
fi
unset marker result_file

sem-version python 3.10
