# Milestone: Python package layout (confluent_kafka4 src-layout)

**Track:** Python binding (`bindings/python/`) · **Driver:** ship as the installable
`confluent-kafka4` package (per the org Rust-clients repository & packaging plan).
**Named per-track milestone** — no global integer; its presence in this
`milestones/` dir *is* the registry entry (avoids the shared-`MILESTONES.md`
conflict/collision problem).

## Goal

Convert today's flat scripts (`producer.py` / `consumer.py` imported by bare name)
into a real, installable package — distribution `confluent-kafka4`, import
`confluent_kafka4`, **src-layout** — with **no behavior change**. Pure restructure.

## References

- **Reasoning:** `design/current/bindings/python/binding-design.md` → `## Packaging & layout`.
- **Packaging decision (org):** Confluence "Rust based clients repository and packages"
  (dist `confluent-kafka4` / import `confluent_kafka4`; must coexist with the old
  `confluent-kafka`; versions track AK).
- **Precedent:** `confluent-kafka-python` (`src/confluent_kafka/` src-layout).

## Scope

**In:** the package move + intra-package import rewiring + build config
(`pyproject.toml` / `setup.py`) + `test/unit` imports + both gRPC Dockerfiles +
`Makefile` + harness (`grpc_*`) imports.
**Out:** the `error.py` split (→ `error-model-parity` milestone); any API/logic
change; the namespace-package option (`confluent.kafka` — considered, rejected:
Option B chosen); the monorepo rename `bindings/python` → `python/` (happens at the
repo split, not here).

## Decisions locked in

- **Option B** — folder/import = `confluent_kafka4` (matches the packaging doc +
  reference; the `4` lives in both the dist and import names for coexistence +
  loose-pin safety).
- **src-layout** (`src/confluent_kafka4/`) — forces testing the *installed* package.
- **C ext becomes `confluent_kafka4._confluentkafka`** (inside the package).
- **`KafkaError` stays in `producer.py`** for now — the `error.py` split belongs to
  the `error-model-parity` milestone, not this move.
- **Harness stays OUT of the shipped package**; tests import the installed package.

## Phases (dependency-ordered)

### Phase 1 — Package move + config + imports  *(non-Docker verifiable)*
Move `consumer.py` / `producer.py` / `_confluentkafka.c` → `src/confluent_kafka4/`;
add `__init__.py` (public-API re-exports) + `py.typed`; rewire intra-package imports
to relative (`from . import _confluentkafka`, `from .producer import KafkaError`);
`pyproject.toml` (name `confluent-kafka4`, `packages.find where=["src"]`) + `setup.py`
(ext `confluent_kafka4._confluentkafka`, source `src/confluent_kafka4/_confluentkafka.c`);
fix `test/unit` imports to `confluent_kafka4.*`.
- **Deliverable:** an installable package.
- **DoD:** `pip install -e .[dev]` + `pytest test/unit` green; `import confluent_kafka4`
  works; no module imported by bare name.

### Phase 2 — Harness + Docker + Makefile  *(Docker-verified)*
Rewire `grpc_server.py` / `grpc_server_async.py` / `grpc_translate.py` imports to
`confluent_kafka4.*`; both Dockerfiles copy `src/` and `pip install .` (build a wheel
in the builder stage, install it in runtime), replacing the per-file `COPY` +
`build_ext --inplace`; keep the `libconfluent_kafka.so` copy + rpath /
`CONFLUENT_KAFKA_LIB_DIR` handling; update `Makefile` paths.
- **Deliverable:** working gRPC images.
- **DoD:** `make grpc-image` + `make grpc-image-async` build; `make test-multilanguage`
  green.

## Sequencing

1 → 2. Phase 1 keeps the non-Docker python path working; Phase 2 needs a Docker build
to verify — the multi-stage rpath/pip wiring is the only real check. Each phase:
Actor builds, Critic reviews, `make verify` green.

## Adjacent (NOT this milestone)

- `error.py` typed hierarchy → `error-model-parity` milestone.
- `aio/` / `admin/` / `schema_registry/` submodules → future features, not this move.
- Monorepo rename to `python/` → at the repo split; the internal layout is unchanged.

## Open decisions (defaults set — implementer confirms; don't re-litigate)

- **`test/` dir name:** *default —* keep `test/` (don't rename to `tests/`); minimizes
  Makefile/Docker churn.
- **Dockerfile runtime install:** *default —* build a wheel in the builder stage,
  `pip install` it in runtime (vs copying site-packages).
- **`__init__.py` export list:** *default —* the public classes only; confirm the exact
  names against the modules when applying.

## Definition of done (milestone)

`import confluent_kafka4` exposes the public API; `test/unit` + the multilanguage
Docker tests green; nothing imports by bare name; **no behavior change** vs pre-move;
all phases reviewed and `make verify` green.
