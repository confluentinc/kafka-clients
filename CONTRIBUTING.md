# Contributing

Thanks for your interest in this project. This is an Apache-2.0-licensed Rust client for Apache Kafka. Issues and pull requests are welcome.

This document sets expectations so contributors know what to expect from us, and reviewers have a consistent bar to apply.

## Project status

This project is under active, early-stage development. Feature parity with the Java client is incomplete, performance is not yet validated across all workloads, and APIs may change without notice. Treat it as a preview, not a stable release.

## Governance

- **Commits:** For now, merge access is limited to a small group of core maintainers. As is common for open-source projects, especially early ones.
- **Community contributions:** Contributions from the community are welcome today through issues and pull requests. Expanding who can merge is a goal we will grow into as our tooling and review process mature.
- **License:** The core library and language bindings are Apache 2.0.
- **CLA:** Most contributions are accepted under the project's Apache 2.0 license without needing a separate agreement. For large contributions, we may ask you to sign a Contributor License Agreement.

## Filing issues

- Search existing issues first to avoid duplicates.
- Include enough detail to reproduce: version, environment, expected vs. actual behavior, and logs if relevant.
- We'll label issues to indicate status (e.g., `status:planned`, `status:waiting-for-interest`, `status:needs-more-info`). A missing label just means it hasn't been triaged yet.
- You can add a comment to issues which are important but have not been triaged. Use your judgement on how long to wait but it can take 1-2 weeks to triage issues.

## Submitting pull requests

A PR is more likely to be reviewed quickly if it:

1. **Addresses an existing issue.** Reference an open issue or prior discussion. Unsolicited PRs with no context are harder to evaluate and may sit longer or get closed.
2. **Passes CI.** PRs with failing checks won't be reviewed until they're green.
3. **Includes tests.** Bug fixes should include a regression test; new functionality should include unit or integration tests.
4. **Follows existing code style and conventions.** Match the surrounding code — naming, formatting, structure. Run `cargo xtask format-check` before opening the PR.
5. **Is scoped and focused.** One logical change per PR. Don't mix refactors, features, and bug fixes.
6. **Stays responsive.** Please respond to review feedback promptly. A PR with no activity for 90 days is marked stale, and after a further 30 days without activity it is closed automatically. Closed PRs can be reopened when you're ready to pick them up again.

We evaluate large or foundational changes (new modules, protocol handling, public API shape) more carefully than small fixes. Expect more back-and-forth on those.

## Roadmap: better tooling for contributors

Ahead of GA, we're rolling out tooling to make it easier to file well-structured issues and PRs (templates, guided issue forms, and similar), and to speed up how quickly we can review what comes in. Review capacity and process are expected to mature over time.

## AI-assisted contributions

If you use AI tools to help write code, docs, issues, or PR descriptions, see [AI_POLICY.md](AI_POLICY.md).

If you used AI tools to prepare a contribution, please add one of the following commit trailers identifying the tool and version. This gives reviewers a clear signal of which tools are in use.

```
Co-Authored-By: <AI tool name and version>
Assisted-By: <AI tool name and version>
Generated-By: <AI tool name and version>
```

You remain responsible for everything you submit, regardless of the tools used.

## Code of conduct

Be respectful and assume good faith. The community is part of what makes this project succeed.
