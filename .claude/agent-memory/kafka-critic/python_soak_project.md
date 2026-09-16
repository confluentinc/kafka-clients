---
name: python-soak-project
description: The Python soak client (bindings/python/soak) is tasks 1-2 of a six-task soak effort; first 14-day batch was scheduled to start 2026-08-13 on a shared EC2 box running four variants
metadata:
  type: project
---

`bindings/python/soak/` is a long-running (2+ week) soak driver for the Rust
Kafka client, written in **Python** driving `bindings/python/` — a decision that
superseded an earlier Rust-soak draft.

It covers **tasks 1 and 2 of six**. The other four are owned by other people and
are not ours to build or review: cluster provisioning (K1 normal / K2 rolled), the
rolling CronJob (`cc-roll-kafka-cron`), EC2 + host setup, and the telemetry
pipeline (OTEL scrape → AWS Managed Prometheus).

Key operating facts that shape review judgement:

- **Four soak variants run on one shared EC2 box**, one tmux window each. Anything
  per-host — PID matching, log/metrics disk use, a wedged shutdown occupying the
  machine — has to be evaluated at 4x.
- **Rolling is entirely cluster-side.** The soak never rolls anything and never
  bounces its own consumer; it only observes and quantifies.
- **Only gaps are a hard failure.** Duplicates are expected and bounded, because
  the binding bridges no rebalance listener and `enable.idempotence` is accepted
  but inert in the Rust producer. No exactly-once claims.
- First batch was scheduled for **2026-08-13**, i.e. reviews around then are
  deadline-driven triage.

**Why:** the six-task split means scope creep into clusters/telemetry is out of
bounds, and the shared-box and cluster-side-rolling facts are the reason several
otherwise-cosmetic issues (supervisor restart behaviour, unbounded files, metric
baselines) are actually load-bearing.

**How to apply:** when reviewing this area, weight operational survivability of an
unattended 14-day run as highly as code correctness, and check the four-on-one-box
multiplier. See [[review-expectations]] for how to deliver the findings.
