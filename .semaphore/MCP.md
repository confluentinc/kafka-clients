# Semaphore CI/CD

## ID Caching (Required)

Before using Semaphore tools, cache org and project IDs in `.semaphore/config.json`:

```json
{
  "organization_id": "<uuid>",
  "organization_name": "semaphore.ci.confluent.io",
  "project_id": "<uuid>",
  "project_name": "example-confluent-kafka-rust"
}
```

Discover IDs once with `organizations_list` and `projects_list`, then always use cached values.

**Known state (2026-07-07):** `organization_id` resolves to `6ab08ce0-d948-4a80-b8e7-748bbb9cdf64`
(the only org visible to this MCP token; confirmed to back both `mcp.ci.confluent.io` and the
`semaphore.ci.confluent.io` SSO redirect, so it is the right org). `project_id` resolves to
`0e20cd45-0962-4ddf-a71a-a872849989aa` — note that `projects_search`/`projects_list` never surfaced
it (tried `example`, `rust`, `kafka-rust`, `confluent-kafka-rust`, and the repo URL in both `git@`
and `https://` forms, all zero matches), so this token's project index/listing appears incomplete
or restricted for this project. It was found instead by resolving a known workflow/pipeline URL
(`pipelines_list` with a `workflow_id` copied from the Semaphore UI returns `projectId` in its
response) — that's the fallback if `project_id` ever needs re-resolving.

## Debugging Workflow

1. `workflows_search` → find failing workflow
2. `pipelines_list` → get pipeline from workflow
3. `pipeline_jobs` → find failed jobs and check `result_reason`
4. If `result_reason=test`: use `get_test_results` first (structured failure data), fall back to `jobs_logs` only if no test results
5. Otherwise: use `jobs_logs` → read error output

## Test Results

`get_test_results` returns a signed URL that **expires quickly**.

**Always:** Download once, analyze locally:
```bash
curl -s "<url>" -o /tmp/test-results.json
```

**Never:** Call get_test_results repeatedly.

## Tips

- Use `mode="summary"` to reduce response size
- Filter with `branch` and `limit` parameters
- Read `.semaphore/config.json` before each session
