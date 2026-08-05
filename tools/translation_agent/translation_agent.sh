#!/bin/bash
set -e

prepare_translation_agent() {
  ./.semaphore/dependencies.sh
  # Ensure all Python dependencies are installed
  make
  (cd tools/translation_agent && pip install -e '.[dev]')
}

if [ -n "${SEMAPHORE_GIT_BRANCH_CHECKOUT}" ]; then
    echo "Override: SEMAPHORE_GIT_BRANCH=${SEMAPHORE_GIT_BRANCH} -> ${SEMAPHORE_GIT_BRANCH_CHECKOUT}"
    SEMAPHORE_GIT_BRANCH="${SEMAPHORE_GIT_BRANCH_CHECKOUT}"
fi
echo "SEMAPHORE_GIT_BRANCH=${SEMAPHORE_GIT_BRANCH}"
echo "SEMAPHORE_GIT_PR_BRANCH=${SEMAPHORE_GIT_PR_BRANCH}"
echo "SEMAPHORE_GIT_PR_NUMBER=${SEMAPHORE_GIT_PR_NUMBER}"
echo "MAIN_BRANCH=${MAIN_BRANCH}"
if [ "${SEMAPHORE_GIT_PR_BRANCH}" = "${MAIN_BRANCH}" ] && [ -n "${SEMAPHORE_GIT_PR_NUMBER}" ]; then
    echo "Skipping: PR build (#${SEMAPHORE_GIT_PR_NUMBER}) on ${MAIN_BRANCH} -- neither sweep nor per-PR phase runs for this combination"
elif [ "${SEMAPHORE_GIT_BRANCH}" = "${MAIN_BRANCH}" ] && [ -z "${SEMAPHORE_GIT_PR_NUMBER}" ]; then
    # The prologue's `artifact pull ... || true` lands the DB at
    # repo-root `translation_agent.db`. If the artifact does not
    # exist yet (no DB, or the artifact hub failed to serve it --
    # e.g. a transient signed-URL error), skip the sweep and let
    # this job pass instead of letting `_run_sweep`'s own strict
    # pull raise and abort it. The tree is still validated by the
    # "Verification" block, which runs regardless.
    if [ -f translation_agent.db ]; then
        prepare_translation_agent
        echo "On main branch (${MAIN_BRANCH}) -- running sweep"
        translation-agent \
        --ak-repo-path "${AK_REPO_PATH}" \
        --rust-branch "${MAIN_BRANCH}"
    else
        echo "On main branch (${MAIN_BRANCH}) -- no translation_agent.db artifact; skipping sweep (the Verification block still runs 'make verify')"
    fi
elif [ -n "${SEMAPHORE_GIT_PR_NUMBER}" ]; then
    # The prologue's `artifact pull ... || true` lands the DB at
    # repo-root `translation_agent.db`. If the artifact does not
    # exist yet (no DB), skip the translation-agent cascade and
    # let this job pass -- a PR build with no orchestrator state
    # to act on is still validated by the "Verification" block.
    if [ -f translation_agent.db ]; then
        prepare_translation_agent
        echo "PR build -- running --pr ${SEMAPHORE_GIT_PR_NUMBER} cascade"
        translation-agent \
        --ak-repo-path "${AK_REPO_PATH}" \
        --pr "${SEMAPHORE_GIT_PR_NUMBER}"
    else
        echo "PR build -- no translation_agent.db artifact; skipping translation-agent cascade (the Verification block still runs 'make verify')"
    fi
else
    echo "Skipping: not on ${MAIN_BRANCH} and no PR number set"
fi