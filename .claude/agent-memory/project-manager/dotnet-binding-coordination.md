---
name: dotnet-binding-coordination
description: How to coordinate the .NET binding sub-project (bindings/dotnet) Actor/Critic loop — which personas, which comment files
metadata:
  type: project
---

The `bindings/dotnet` .NET binding is coordinated with its OWN Actor/Critic personas, distinct from the Rust-translation workflow.

- Actor persona = `dotnet-actor`; Critic persona = `dotnet-critic`. Both are N-numbered exactly like the Rust workflow (first milestone used N=1). Do NOT use `actor-executor` / `kafka-critic` for the .NET binding — those are Rust-translation personas and are the wrong tool for the C# side.
- Review comments: active items in `bindings/dotnet/COMMENTS.<N>.md` (this file is **git-ignored** — it is a working scratch, not a committed record); resolved items + durable milestone decisions/deferrals move to `bindings/dotnet/COMMENTS.DONE.<N>.md` (which **is** tracked/committed).

**Why:** The .NET binding is a separate sub-project with its own CLAUDE.md (`bindings/dotnet/CLAUDE.md`), its own rules (`.claude/rules/ffi-marshalling.md`), and its own agent personas under `.claude/agents/`. Rust-translation artifacts (design/current, marked_classes.txt) do not apply to it.

**How to apply:** When coordinating a `bindings/dotnet` milestone, spawn `dotnet-actor`/`dotnet-critic` with an explicit "your assigned number N is <n>". Look for the durable record of prior milestone decisions in `COMMENTS.DONE.<N>.md`, not `COMMENTS.<N>.md`. If a future manager finds `COMMENTS.<N>.md` absent, that is expected (git-ignored) — it does not mean no review happened.
