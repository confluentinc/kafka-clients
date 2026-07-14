# Python binding — FFI boundary contracts (G3)

Deep-dive rulebook for the correctness contracts across the CPython-extension /
C-ABI boundary. Referenced on demand from `bindings/python/CLAUDE.md`; not
auto-loaded. Follows the repo's single-themed-rule-file precedent
(`.claude/rules/consumer-threading.md`): numbered sections, each stated as
**Rule / Why / How to apply / Anti-patterns / Tests required**.

> **Status: forthcoming.** The layout and section outline are locked (below);
> the content is authored when G3 is implemented. See the roadmap in
> `bindings/python/CLAUDE.md`.

## Locked outline

1. **GIL & threading** — which thread each callback runs on; releasing the GIL
   around blocking core calls; the `send_thread` / `poll_futures_thread` model.
2. **Free-threading (PEP 703)** — whether to declare `Py_MOD_GIL_NOT_USED`, and
   the invariants that must hold under a no-GIL interpreter.
3. **Handle ownership & lifecycle** — `_destroy` obligations, no double-free /
   no leak on early-return paths, ownership transfer through callbacks.
4. **Zero-copy & buffer lifetime** — borrow `bytes` without copying; keep the
   Python buffer alive across the async send window.
5. **Error model** — precondition validation (`ValueError` / `TypeError`) vs.
   core `KafkaError`; null-handle-means-success; error travel through a Future.
6. **Async / Future model** — `concurrent.futures.Future` bridging; completion
   and cancellation semantics.
