---
name: python-binding-callback-refcount-teeth
description: How to give a CPython-extension callback's INCREF/DECREF lifecycle real test teeth (getrefcount/weakref) and prove the teeth by perturbation
metadata:
  type: feedback
---

Wrapper-attribute tracking of a registered callback is NOT a substitute for
asserting the C refcount — the Python attribute (`self._ack_commit_bridge`) is
independent of the extension's `Py_INCREF`/`Py_DECREF`, so a leaked or doubled
INCREF passes attribute-only tests. This bit us in Phase 4: the register /
replace / clear / close tests asserted only the attribute and the Critic flagged
"no teeth."

**Why:** The mock's Rust setter is a no-op, so the C trampoline never fires
end-to-end in unit tests — the only mock-reachable C refcount work is the
setter's INCREF-on-set / DECREF-old-on-clear. That balance must be probed
directly.

**How to apply (pure-Python, no FFI scaffold needed):**
- Exactly-one INCREF: `assert sys.getrefcount(c._attr) == 3`. The three live
  refs are the wrapper attribute + the single extension INCREF + the temporary
  argument to `getrefcount`. Missing INCREF reads 2; double reads 4. Access the
  attribute inline (don't bind a local, or the count shifts).
- Release on clear/replace/close: `weakref.ref(bridge)` then act + `gc.collect()`;
  assert `ref() is None`. A leaked INCREF keeps it alive.
- No accumulation: collect a weakref per cycle over ~200 set/clear cycles;
  assert `sum(r() is not None for r in refs) == 0`.
- Bridge closures must not capture `self` (they capture only the user callback),
  else a cycle defeats immediate refcount collection.

**Proving the teeth (do this, don't just claim it):** temporarily
double-`Py_INCREF` the setter, rebuild, run — all the probes fail *cleanly*
(getrefcount reads 4, weakrefs survive), then revert + rebuild green. Prefer
double-INCREF over *removing* the INCREF: removal leaves the DECREF-on-clear as
an over-DECREF → UAF → segfault that aborts pytest messily. The removed-INCREF
direction is still covered by the getrefcount probe reading 2.

The trampoline's own owned-handle destroys and its deliberate lack of a per-call
DECREF can only run against a live broker; document them as FFI-layer coverage
in a test comment so a future reader knows why there's no mock-firing test.

See [[phase4-share-consumer-python-notes]], [[python-bindings-macos-build]] (the
shim rebuild recipe), and [[workflow-comments-gitignore-and-staging]].
