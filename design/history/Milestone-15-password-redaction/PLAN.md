# Milestone 15 — `Password` type and secret redaction

**Status:** APPROVED (2026-09-29). Execution: Actor/Critic 85 on the branch below.
**Origin:** security code-review finding (severity Low): secret-bearing structs derive `Debug`.
**Baseline:** `master` at `b76de2e1`. Java reference: Apache Kafka 4.3.1 — the `kafka/` submodule
is pinned at `26b251a451ce941d3d7a55e6487bcb7f16b5ad48` but is **not checked out in this clone** (§10.4).
**Scope:** Rust only. No C FFI / Python / gRPC change is needed (§3.4).
**Agent numbers:** 85 (Actor 85 / Critic 85). Highest number used anywhere in history is 84.
**Branch:** `fix/password-type-redaction` off `master`. The Actor commits there; nobody pushes
or opens a PR until the user says so.

---

## 1. What this milestone delivers

The finding reports that secret-bearing structs use `#[derive(Debug)]`, so any `{:?}` — a `debug!`
line, an error wrapper, a panic message — prints credentials verbatim. Nothing does so today
(verified, §2), so the defect is latent, but it is one line away from a leak.

Rather than hand-redacting each struct, this milestone **aligns with Java**. Java never holds a client
secret as a bare `String`: every `ConfigDef.Type.PASSWORD` value is wrapped in
`org.apache.kafka.common.config.types.Password`, whose `toString()` returns `"[hidden]"`
(`Password.java:24`, `:55-56`), and every wire wrapper that carries a secret overrides `toString()`
to redact it. The Rust translation dropped `Password` and typed those values as `Option<String>`,
which is exactly where the redaction was lost.

Deliverables:

1. `common::config::types::Password` — a full translation of `Password.java`.
2. Every config field Java defines as `Type.PASSWORD` becomes `Option<Password>` in `SslConfig` and
   `SaslConfig`. The *derived* `Debug` of those two structs, and of `ConsumerConfig` and
   `AdminClientConfig` which embed them, is then safe by construction.
3. Hand-written `Debug` where Java redacts in `toString()` but Rust still derives: `ProducerConfig`
   (its verbatim `originals` map), `DelegationToken`, six request/response wrappers, and
   `UserScramCredentialUpsertion`.
4. A test per struct asserting the `{:?}` rendering does not contain the secret.

## 2. Verified baseline — the finding, corrected

Every claim below was checked against `b76de2e1`; the finding's own line numbers are from `a6e6b71`
and `4880d36`, both ancestors of HEAD, so some have shifted.

**Reported variants, all still present:**

| Struct | Where | Secret fields printed by derived `Debug` |
|---|---|---|
| `SaslConfig` | `src/common/config/sasl_configs.rs:71` | `jaas_config`, `password` |
| `SslConfig` | `src/common/config/ssl_configs.rs:128` | `truststore_password`, `keystore_password`, `keystore_key` (PEM private key), `key_password` |
| `DelegationToken` | `src/common/security/token/delegation/delegation_token.rs:56` | `hmac` — its hand-written `Display` redacts (`hmac=[*******]`, tested at `:153`), derived `Debug` bypasses it |
| `ProducerConfig` | `src/producer/producer_config.rs:53` | embeds both sub-configs **and** `originals: HashMap<String, String>` (`:266`), the verbatim user map |
| `ConsumerConfig` | `src/consumer/consumer_config.rs:47` | embeds both sub-configs |

**Correction:** `ConsumerConfig` has **no** `originals` map (the finding says it does). Only
`ProducerConfig` carries one. The consumer stays in scope through the embedded sub-configs.

**Same defect, not in the finding — added to scope:**

- `AdminClientConfig` (`src/admin/admin_client_config.rs:30`) derives `Debug` and embeds `sasl_config`
  / `ssl_config` as private fields (`:51`, `:54`).
- `UserScramCredentialUpsertion` (`src/admin/user_scram_credential_upsertion.rs:26`) derives `Debug`
  over `password: Vec<u8>` and `salt`. Java defines no `toString()` on it at all
  (`UserScramCredentialUpsertion.java:31-34`), so Java has no printing path; the Rust derive adds one.
- Six wrappers with a redacting `Display` (mirroring Java's `toString()`) that **also**
  `#[derive(Debug, Clone)]`, the `DelegationToken` pattern exactly:
  `SaslAuthenticateRequest`, `SaslAuthenticateResponse`, `CreateDelegationTokenResponse`,
  `DescribeDelegationTokenResponse`, `RenewDelegationTokenRequest`, `ExpireDelegationTokenRequest`
  (all under `src/common/requests/`).
- `MockAdminClient` derives `Debug` over `State.all_tokens: Vec<DelegationToken>`, so it inherits
  the HMAC leak. Fixed transitively by the `DelegationToken` change; gets a regression test only.

**"Latent" confirmed.** No production code Debug-formats any of these types (`src`, `src/ffi`,
workspace crates, `tests`, Python bindings' `__repr__`s all checked). `KafkaProducer`,
`AsyncKafkaConsumer`, `KafkaAdminClient` do not derive `Debug`. `SaslChannelBuilder` already has the
safe pattern: a hand-written `Debug` printing only `sasl_config.mechanism` and ending with
`finish_non_exhaustive()` (`src/common/network/sasl_channel_builder.rs:52`). `SaslClientAuthenticator`,
which holds the plaintext password, has no `Debug`.

## 3. Scope decisions

### 3.1 Which fields become `Password` — exactly Java's `Type.PASSWORD` set

From `SslConfigs.addClientSslSupport` (`SslConfigs.java:133-140`) and
`SaslConfigs.addClientSaslSupport` (`SaslConfigs.java:380`), restricted to keys the Rust client parses:

| Java key | Rust field | Note |
|---|---|---|
| `ssl.truststore.password` | `SslConfig::truststore_password` | |
| `ssl.truststore.certificates` | `SslConfig::truststore_certificates` | public certs, but Java hides them — we follow Java |
| `ssl.keystore.password` | `SslConfig::keystore_password` | |
| `ssl.keystore.key` | `SslConfig::keystore_key` | PEM private key |
| `ssl.keystore.certificate.chain` | `SslConfig::keystore_certificate_chain` | public certs, but Java hides them — we follow Java |
| `ssl.key.password` | `SslConfig::key_password` | |
| `sasl.jaas.config` | `SaslConfig::jaas_config` | embeds the password |

Java also types `sasl.oauthbearer.client.credentials.client.secret` and
`sasl.oauthbearer.assertion.private.key.passphrase` as PASSWORD (`SaslConfigs.java:392`, `:402`);
OAuth is not translated (Milestone 13 exclusion), so nothing to type here.

### 3.2 Rust-only convenience fields

`SaslConfig::username` / `SaslConfig::password` have no Java counterpart (they are set
programmatically; no `sasl.username`/`sasl.password` key is parsed anywhere in `src`).
`password` becomes `Option<Password>` — it is a password. `username` stays `Option<String>`.
`resolve_username()` / `resolve_password()` keep their `Option<&str>` signatures: the authenticator
must send the plaintext, exactly as Java's `PlainLoginModule` yields a plain `String`. They read
`.value()` on the `Password` internally.

### 3.3 Where Java redacts in `toString()`, Rust's `Debug` must redact too

Java has a single stringification (`toString()`); Rust has two. For `DelegationToken` and the six
wrappers the existing `Display` is the faithful `toString()` translation, so `Debug` is implemented
by **delegating to `Display`** (`fmt::Display::fmt(self, f)`). No second redaction logic to keep in
sync. `Clone` (and `PartialEq, Eq, Hash` on `DelegationToken`) stay derived.

### 3.4 Bindings are unaffected

The C FFI (`src/ffi/*.rs`) and Python bindings build configs from property maps and never touch
these fields (verified by grep). `cargo xtask check-bindings` is a CPython format-arity checker,
unrelated to Rust type changes. No header, `.py`, or gRPC change. `make verify` covers what matters.

### 3.5 Explicitly out of scope

- Zeroizing secrets on drop (would need a new crate; Java does not do it either).
- Translating `ConfigDef` / `AbstractConfig` (Milestone 13 exclusion stands). §4.3 carries the one
  piece of `ConfigDef` knowledge this milestone needs.
- `Deref<Target = str>`, `From<&str>` / `From<String>`, or `Default` on `Password` — none exist in
  Java, and each would make an accidental plaintext path easier. Construction is explicit:
  `Password::new(..)`.
- Removing `Debug` from the client configs altogether (Java's `AbstractConfig` has no `toString()`).
  Rust users expect `{:?}` to work; a redacted `Debug` is what the finding asks for.

## 4. Design

### 4.1 `Password` (`src/common/config/types/password.rs`)

Translation of `Password.java` (Apache 2.0 header, Confluent Inc.), rustdoc citing the Java class:

```rust
#[derive(Clone, PartialEq, Eq, Hash)]           // Java: equals/hashCode on `value`
pub struct Password { value: String }

impl Password {
    pub const HIDDEN: &'static str = "[hidden]";   // Java: Password.HIDDEN
    pub fn new(value: impl Into<String>) -> Self;  // Java: Password(String value)
    pub fn value(&self) -> &str;                   // Java: value()
}
impl fmt::Display for Password { /* writes Self::HIDDEN */ }   // Java: toString()
impl fmt::Debug   for Password { /* writes Self::HIDDEN */ }   // Rust has two; both redact
```

Both formatters write exactly `[hidden]` — no type name, no length (length is information too).

### 4.2 Field typing and its ripple

Parse sites (wrap with `Password::new(value)`):
- `SslConfig::apply_ssl_config_key` (`ssl_configs.rs:206-246`) — the six SSL arms.
- The three parents' `sasl.jaas.config` arms: `producer_config.rs:603`, `consumer_config.rs:811`,
  `admin_client_config.rs:131`.

Read sites (call `.value()`):
- `src/common/security/ssl/ssl_factory.rs:211`, `:248`, `:284` (`truststore_certificates`,
  `keystore_certificate_chain`, `keystore_key` PEM parsing).
- `SaslConfig::resolve_username` / `resolve_password` (`sasl_configs.rs:233`, `:245`).
- `SaslChannelBuilder` reads only through `resolve_*` (`sasl_channel_builder.rs:117-167`) — unchanged.

Construction sites in tests (`Some("x".to_owned())` → `Some(Password::new("x"))`): `ssl_configs.rs`
(~24), `sasl_configs.rs` (~26), `sasl_channel_builder.rs` tests (~8), `channel_builders.rs`,
`ssl_transport_layer.rs`, `tests/integration/ssl_sasl_test.rs:68`, `:101`. The compiler finds them all.

Public API note: `SslConfig` / `SaslConfig` fields are `pub`, so this is a breaking change to their
type. Allowed — CLAUDE.md: the public API is not stable below 1.0.

### 4.3 `ProducerConfig`: hand-written `Debug`, `originals` redacted by key

Java's `originals()` is the raw user map too, but Java never prints it; Rust's derive would. The
`originals` values that are secrets are exactly the `Type.PASSWORD` keys — knowledge Java keeps in
`ConfigDef`, which is out of scope. Minimal carrier of that knowledge, hosted on the structs that
already own the key constants (CLAUDE.md §2: statics live on the defining struct):

```rust
impl SslConfigs  { pub(crate) fn is_password_config(key: &str) -> bool }  // the 6 SSL keys of §3.1
impl SaslConfigs { pub(crate) fn is_password_config(key: &str) -> bool }  // sasl.jaas.config
```

each documented as the translation of the `Type.PASSWORD` markings in `addClientSslSupport` /
`addClientSaslSupport`, with the Java line cited. **DoD #7 justification:** two `pub(crate)` free
functions, no new struct or trait; they exist because `ConfigDef` is not translated.

`impl fmt::Debug for ProducerConfig` uses `debug_struct` over **every** field in declaration order
(so the rendering stays a useful debugging aid), rendering `originals` through a temporary
`BTreeMap<&str, &str>` whose value is `Password::HIDDEN` wherever either predicate is true.
`BTreeMap` makes the output deterministic for the test. Omitting a future field from this impl is
safe (nothing leaks), so the maintenance cost is accepted; the Critic checks completeness once.

`ConsumerConfig` and `AdminClientConfig` keep `#[derive(Debug)]` — safe once §4.2 lands — and get
regression tests. If either ever gains an `originals` map it must follow §4.3; say so in a comment
on each struct.

### 4.4 Delegating `Debug` (§3.3)

`DelegationToken` + the six wrappers: drop `Debug` from the derive list, add
`impl fmt::Debug for T { fn fmt(&self, f) -> fmt::Result { fmt::Display::fmt(self, f) } }`.

### 4.5 `UserScramCredentialUpsertion`

Java has no `toString()`. Hand-written `Debug` via `debug_struct("UserScramCredentialUpsertion")`
printing `user` and `info`, then `.field("salt", &Password::HIDDEN).field("password", &Password::HIDDEN)`
(salt is keyed material; hide both), `finish()`.

## 5. Module layout

```
src/common/config/
  mod.rs                 # + pub mod types;
  types/
    mod.rs               # mod password; pub use password::Password;
    password.rs          # Password (Java: common.config.types.Password)
  sasl_configs.rs        # jaas_config/password: Option<Password>; SaslConfigs::is_password_config
  ssl_configs.rs         # six fields: Option<Password>; SslConfigs::is_password_config
```

Import path per CLAUDE.md §2: `use crate::common::config::types::Password;` (parent-module
re-export, never `types::password::Password`).

## 6. Phase 1 — single phase, two commits (Actor/Critic 85)

**Commit A — `Password` and the typed fields.**
1. Add `Password` + unit tests (§7.1).
2. Retype the seven fields (§3.1) plus `SaslConfig::password`; wrap at parse sites; `.value()` at read
   sites; fix every test construction site.
3. Add `SslConfigs::is_password_config` / `SaslConfigs::is_password_config` + tests (both directions
   over every key constant the struct defines, precedent: `test_retriable_errors_match_java_hierarchy`).
4. `Debug` redaction tests for `SaslConfig`, `SslConfig`, `ConsumerConfig`, `AdminClientConfig` (§7.2).

**Commit B — hand-written `Debug` impls.**
5. `ProducerConfig` (§4.3) + test.
6. `DelegationToken` + six wrappers (§4.4) + tests; `MockAdminClient` regression test.
7. `UserScramCredentialUpsertion` (§4.5) + test.
8. `SaslChannelBuilder` regression test on the existing manual `Debug`.
9. Comments on `ConsumerConfig` / `AdminClientConfig` per §4.3 last paragraph.

Both commits: `make verify` green (build, format-check, lint, workspace tests, check-bindings).

## 7. Tests

### 7.1 Java tests to translate
None. `clients/src/test/java/org/apache/kafka/common/config/types/PasswordTest.java` does **not**
exist at the pinned SHA (HTTP 404 verified). Java exercises `Password` only through `ConfigDefTest`
/ `AbstractConfigTest`, both out of scope with `ConfigDef`. Rust-side tests below are additive.

### 7.2 Rust tests (all assert on the rendered string; every secret is a distinctive literal)
- `password.rs`: `value()` round-trips; `Display` and `Debug` both render exactly `[hidden]`; two
  passwords of different lengths render identically; `PartialEq`/`Hash` follow `value`; `Clone`.
- `sasl_configs.rs`: the finding's requested test — `format!("{:?}", cfg)` contains neither the password nor the
  JAAS string, does contain `mechanism` and `username`; `resolve_password()` still yields plaintext.
- `ssl_configs.rs`: `{:?}` hides all six §3.1 fields, shows `truststore_location` / `keystore_type`.
- `producer_config.rs`: build from props holding all seven §3.1 keys plus a non-secret key; `{:?}`
  contains none of the seven values, contains the non-secret value, contains `[hidden]`; also hides
  the typed sub-config fields.
- `consumer_config.rs`, `admin_client_config.rs`: `{:?}` hides JAAS + SSL secrets from props.
- `is_password_config` (both structs): exhaustive over every `pub const` key on the struct.
- `delegation_token.rs`: `debug_redacts_hmac` mirroring `display_redacts_hmac`.
- Each of the six wrappers: `debug_redacted` mirroring the existing `test_display_redacted` /
  `display_redacts_*`.
- `user_scram_credential_upsertion.rs`: `{:?}` shows `user`, hides password bytes and salt.
- `mock_admin_client.rs`: `{:?}` of a mock holding a token does not contain the HMAC.
- `sasl_channel_builder.rs`: `{:?}` of a builder with credentials contains neither.

## 8. Definition of Done (per `definition-of-done.md`)

1. CLAUDE.md + rules: naming (`types::Password`, `new`, `value`), license header, rustdoc from the
   Java class, no "exception" wording, statics on the defining struct.
2. All `Password` methods translated: `HIDDEN`, constructor, `hashCode`/`equals` (derives),
   `toString` (`Display` + `Debug`), `value`.
3. Tests per §7; `@RepeatedTest`/wire tests: N/A (no wire type changes).
4. Blockers: none — no new dependency.
5. `make verify` green; `cargo test --workspace` green.
6. No duplicate: the only other `Password` identifier is the unrelated `admin::ConfigType::Password`
   enum variant (a `DescribeConfigs` type tag) — coexistence is fine, different modules.
7. Items not in Java: `SslConfigs::is_password_config`, `SaslConfigs::is_password_config` — §4.3.
   `Debug` impls are Rust-required (not new types).
8. No TODO/FIXME.
9. C/Python test suites: not re-run — no binding source changes (§3.4); `check-bindings` runs in
   `make verify`.
10. Hot-path audit: **N/A** — config parsing is startup-time; `Password::value()` returns `&str`
    with no allocation; the authenticator path is unchanged. Stated here per admin-client.md §10.
11. Consumer trait surface: N/A (no trait touched).
12. Fixture fidelity: N/A.

## 9. Manager loop

1. On approval: create branch `fix/password-type-redaction`; commit this PLAN.md as its first commit.
2. Spawn **Actor 85** with §6, §7, §8 and the Java-source instructions of §10.4.
3. Spawn **Critic 85** to review each commit; findings to `COMMENTS.85.md`. Critic focus list:
   every §3.1 key maps to a `Password` field; no `Option<String>` secret remains in either
   sub-config; `Password` renders neither value nor length; no `Deref`/`From`/`Default` added;
   `ProducerConfig::Debug` lists every field; `is_password_config` tests are exhaustive both ways;
   wrapper `Debug` delegates to `Display` rather than re-implementing; `resolve_*` still return
   plaintext; all test-construction sites use `Password::new` (no helper that hides it).
4. Fix loop until `COMMENTS.85.md` is empty; then handoff: update `design/current/structure.md`
   (config listing, line 100) and `design/current/status.md`; add Milestone 14 and 15 entries to
   `design/history/MILESTONES.md` (14 is currently missing); copy `COMMENTS.DONE.85.md` here.
5. The user opens the PR. Repo artifacts (plan, branch, commits, PR text) carry no internal tracker identifiers.

## 10. Assumptions and open points for the reviewer

1. **Certificates are hidden too** (`ssl.keystore.certificate.chain`, `ssl.truststore.certificates`)
   because Java types them PASSWORD. Say so if you would rather keep them visible.
2. **`originals` redaction** is by key (§4.3). Alternative: print only the keys of `originals`. The
   by-key form is closer to Java's `logAll()` behaviour and more useful when debugging.
3. **`Password` `Debug` == `Display` == `[hidden]`**, no type-name wrapper.
4. **Java source access.** `kafka/` is not checked out. Agents will read the needed Java files
   read-only from `https://raw.githubusercontent.com/apache/kafka/26b251a451ce941d3d7a55e6487bcb7f16b5ad48/clients/src/main/java/org/apache/kafka/...`
   (`common/config/types/Password.java`, `common/config/SslConfigs.java`,
   `common/config/SaslConfigs.java`, `common/config/AbstractConfig.java`,
   `common/security/token/delegation/DelegationToken.java`, `common/requests/SaslAuthenticateRequest.java`
   and siblings, `clients/admin/UserScramCredentialUpsertion.java`) and will **not** run
   `git submodule update --init kafka`. If you prefer the local tree, initialise the submodule before
   approving and they will use it.
5. `resolve_password()` keeps returning `Option<&str>` (§3.2).
6. No zeroize-on-drop (§3.5).
7. Breaking the `pub` field types of `SslConfig` / `SaslConfig` is acceptable below 1.0.
