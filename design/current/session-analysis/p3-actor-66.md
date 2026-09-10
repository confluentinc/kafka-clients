# P3 — Actor 66 session analysis

Phase **P3** of `PLAN-python-interface-implementation.md`: the serialization layer
(`confluent_kafka.common.serialization`) and the config helpers
(`confluent_kafka._config`) of the new `confluent_kafka` package. Branch
`dev_python-interface-implementation`. Committed, not pushed. Clients are NOT
wired (P4/P5).

## What was built

### 1. `confluent_kafka/common/serialization/` (deliverable 1)

Mirror of `org.apache.kafka.common.serialization` (Apache Kafka 4.3.1). One Java
class per file where a Java class exists, plus the factory / supply API.

- `_protocols.py` — `Serializer(Protocol[T_contra])` /
  `Deserializer(Protocol[T_co])` with the single collapsed `__call__(topic, data/
  value, headers=None)` shape (spec §5.4 / D6); `Configurable` / `Closable`
  `@runtime_checkable` protocols; `SerdeBase` no-op base.
- Built-in serde **classes** (Java-faithful, one per Java class where one exists):
  - `string_serializer.py` / `string_deserializer.py` — `configure` honours
    `key/value.serializer|deserializer.encoding` + generic fallback, first two
    win; unknown charset raises `SerializationError` **at configure time** (Java's
    `Charset.forName` in `configure`).
  - `int_serializer.py` / `int_deserializer.py` — collapses Java `Integer`
    (size 4) + `Long` (size 8); big-endian signed; deserializer size-check message
    keyed to the Java class the size selects.
  - `float_serializer.py` / `float_deserializer.py` — collapses `Float` (4) +
    `Double` (8); big-endian IEEE-754 via `struct`; deserializer message
    "…received by Deserializer is not N" (Java's array-path text).
  - `bool_serializer.py` / `bool_deserializer.py` — 1-byte 0x01/0x00; unexpected
    byte printed as Java's **signed** value.
  - `uuid_serializer.py` / `uuid_deserializer.py` — string-form round-trip via our
    `Uuid` (base64), honours the encoding config keys; parse error wrapped as
    "Error parsing data into UUID".
  - `byte_array_serializer.py` (passthrough, producer default),
    `byte_array_deserializer.py` (owned-`bytes` copy, consumer default),
    `byte_buffer_deserializer.py` (zero-copy `memoryview`, opt-in).
  - `json_serde.py` — `JsonSerializer` / `JsonDeserializer` via the `json` module.
- `_encoding.py` — charset-name validation shared by string/UUID serdes (Java's
  `Charset.forName` → `SerializationError` analog).
- `_factories.py` — the **exact** spec §5.4 factory set, each returning a typed
  callable so `K`/`V` infer: `bytes_serializer`, `bytes_deserializer`,
  `memoryview_deserializer`, `string_serializer(*, encoding="utf_8")`,
  `string_deserializer`, `int_serializer(*, size=4)`, `int_deserializer`,
  `float_serializer(*, size=8)`, `float_deserializer`, `bool_serializer`,
  `bool_deserializer`, `uuid_serializer`, `uuid_deserializer`, `json_serializer`,
  `json_deserializer`. Size args keyword-only, validated (int 4|8, float 8|4).
- `_supply.py` — `resolve_serde(kwarg, config, key, *, is_key, default)`
  (kwarg wins; class-as-kwarg rejected with the redirect message; config
  dotted-path/class-object resolve → no-arg construct → `configure(conf, is_key)`;
  instance-in-config rejected), `configure_if_defined`, `close_if_defined` (close
  exceptions logged, never raised — Java's `closeQuietly`).
- `__init__.py` re-exports protocols, factories, supply helpers, and the concrete
  serde classes (38 exports).

### 2. `confluent_kafka/_config.py` (deliverable 2)

Config helpers the clients share (spec §5.7, rule 9; D25) — not wired here.

- `ConfigType` enum (Java's `ConfigDef.Type` value members) +
  `coerce_config_value(name, value, type)` — Java `ConfigDef.parseType` coercion
  for bool/int/short/long/double/string/list/class, `"true"`/`True` and
  `"1000"`/`1000` equivalence, Java's `ConfigException` message text as
  `ConfigError`; String-route range checks mirror `Integer/Short/Long.parseXxx`.
- `log_unused(unused_keys)` — Java `AbstractConfig.logUnused()` INFO log; unknown
  keys accepted, never rejected.
- `duration_to_ms(timeout, *, default_ms)` — float seconds or `timedelta`;
  negative → `IllegalArgumentError("Timeout must not be negative")`; `None` →
  default.
- `reject_callback_config_keys(config)` + `REJECTED_CALLBACK_KEYS` — the
  old-client callback config keys (`error_cb`, `logger`, `on_delivery`,
  `on_commit`, `stats_cb`, `throttle_cb`, `oauth_cb`, `dr_cb`, `dr_msg_cb`,
  `rebalance_cb`) each rejected with a `ConfigError` naming the replacement
  (§11.1). `group.id` deliberately NOT rejected (optional, Java-faithful).

### 3. Tests (deliverable 3)

- `test/unit/test_serialization.py` — translates Java `SerializationTest`:
  round-trip per type, null passthrough (all serdes), string encodings (UTF-8/16,
  Java + Python names), configure-throws-on-unknown-charset, float/int/long/bool
  size-check messages, boolean `@ParameterizedTest`, float NaN (adapted — C17),
  `memoryview` input path, UUID string-form + parse-error, JSON round-trip/errors,
  factory size validation + keyword-only enforcement. **Byte-vector** assertions
  against Java's encoders (DoD #3): big-endian int/long, IEEE-754 float/double,
  bool bytes, UTF-8 string.
- `test/unit/test_serde_supply.py` — `resolve_serde` routes (default, kwarg wins,
  function, class rejected, config dotted-path + `configure` per-slot, `is_key`
  slot selection, class object, instance rejected, unresolvable path) +
  lifecycle helpers (`configure_if_defined`, `close_if_defined` logs-not-raises).
- `test/unit/test_config.py` — coercion rules with Java's exact `ConfigException`
  message text, `duration_to_ms`, `log_unused` (caplog), callback-key rejection,
  `group.id` not rejected.
- `test/unit/test_typing.py` — extended with `assert_type` on each factory's
  typed return (`Serializer[T]` / `Deserializer[T]`) so generics inference is
  machine-verified; a bare-function serde type-checks as `Deserializer[str]`.

## Java tests translated / skipped

| Java test (`SerializationTest`) | Status | Notes |
|---|---|---|
| `allSerdesShouldRoundtripInput` | translated | per-type round-trips; `ByteBuffer`/`Bytes` rows → `bytes`/`memoryview` serdes; Short row → C16 (rejected). |
| `allSerdesShouldSupportNull` | translated | every serde maps `None`→`None`. |
| `stringSerdeShouldSupportDifferentEncodings` | translated | UTF-8/UTF-16, Java + Python charset names. |
| `stringSerdeConfigureThrowsOnUnknownEncoding` | translated | `SerializationError` at configure time. |
| `floatDeserializerShouldThrow…OnZero/TooFew/TooManyBytes` | translated | parametrized on length 0/3/5. |
| `booleanDeserializerShouldThrowOnEmptyInput` | translated | + unexpected-byte cases (signed value). |
| `floatSerdeShouldPreserveNaNValues` | **adapted** | canonical-NaN round-trip; raw signaling-NaN payload is a Python-float limitation (C17). |
| `testBooleanSerializer` / `testBooleanDeserializer` (`@ParameterizedTest`) | translated | true/false byte vectors. |
| `stringDeserializerSupportByteBuffer` | translated | the `memoryview` input path. |
| `testSerializeVoid` / `testDeserializeVoid` / `voidDeserializerShouldThrowOnNotNullValues` | **skipped** | `Void` serde not in v1 roster (D6) — no factory. C16. |
| all `listSerde…` (14 cases) | **skipped** | `List` serde not in v1 roster (D6, needs inner-serde story). C16. |
| `testSerdeFromUnknown` / `testSerdeFromNotNull` | **skipped** | `Serde`/`Serdes` bundle+catalog are Streams-only, not on this surface (D6 finding); factories tested directly. |

There is no Java test for the supply routes or the config helpers (Java resolves
these in `Deserializers` / `AbstractConfig` with no dedicated test class); the
Java-parity behavior (D6 supply routes, `ConfigDef.parseType` messages) is
asserted directly with Java's exact message text.

## Byte-vector evidence (DoD #3, wire-level)

- int32 `1` → `00 00 00 01`; `-1` → `ff ff ff ff`; `423412424` → `struct.pack(">i")`.
- int64 `1` → `00…01` (8B); `-1` → `ff`×8; `922337203685477580` → `struct.pack(">q")`.
- float32 `1.5` → `3f c0 00 00`; float64 `1.5` → `3f f8 00 00 00 00 00 00`.
- bool `True` → `01`, `False` → `00`. string `"my string"` → UTF-8 bytes.
- Size-check messages asserted verbatim: `"Size of data received by
  IntegerDeserializer is not 4"`, `"…LongDeserializer is not 8"`, `"…Deserializer
  is not 4"` (float), `"…BooleanDeserializer is not 1"`, `"Unexpected byte
  received by BooleanDeserializer: 5"` / `"…: -1"` (signed).
- Config coercion messages asserted verbatim vs Java `ConfigException`:
  `"Invalid value <v> for configuration <k>: Expected value to be either true or
  false"`, `"…: Not a number of type INT/SHORT/DOUBLE"`, `"…: Expected value to be
  a string, but it was a <type>"`.

## Clarifications logged (`implementation-clarifications.md`, C15–C19)

- **C15** — `uuid_*` use our `Uuid` (base64 wire form), not `java.util.UUID`
  (dashed); mechanism faithful, wire bytes differ — owner to confirm interop.
- **C16** — Java serdes with no spec factory: Short (size 2 rejected), Void, List,
  ByteBuffer-serializer, Bytes → not offered per D6 roster; skipped Java tests
  accounted for.
- **C17** — float serde does not preserve raw signaling-NaN payloads (Python-float
  limitation); the Java NaN test adapted to canonical-NaN round-trip.
- **C18** — built-ins run in Python for P3; native C-ext execution (no FFI serde
  hook exists today) deferred to P4/P5. No FFI added in P3.
- **C19** — serde/config error classes imported from their concrete generated
  modules (`errors._generated`, `config._generated_errors`) not the package
  re-export, because `mypy --strict` resolves the package star-re-export as
  `object` (the errors package ships no `__init__.pyi`). Typing gap belongs to the
  errors-package owner (Actor/Critic 64); imports move back once closed.

## Verification

- `mypy --strict confluent_kafka test/unit/test_typing.py` → clean (52 files).
- `pytest test/unit` → **703 passed, 2 skipped** (573 P2 baseline + 130 new: 60
  serialization, 24 supply, 46 config; some parametrized).
- Two commits, each gated by the pre-commit hook (`make verify-sandbox`: release
  build, C build, format-check, lint, Docker integration + C tests) — both exit 0.

## Concurrency

Critic 65 (read-only) and Actor 65 (fixes in `common/`, `consumer/`, `producer/`
value-type files) worked in parallel. I owned only
`confluent_kafka/common/serialization/`, `confluent_kafka/_config.py`, and my test
files + the `test_typing.py` extension + the clarifications append. All commits
used path-limited `git commit -F <msg> -- <paths>`; the shared index (which at one
point held Actor 65's staged `common/__init__.py`, `headers.py`,
`consumer_record.py`, `producer_record.py`, `COMMENTS.DONE.65.md` and their tests)
was never swept — the pathspec commit touched only my files, leaving theirs staged.

## Commits

1. `4a1ad452` — P3: confluent_kafka.common.serialization — protocols, built-in
   serdes, supply routes (+ tests, test_typing extension, clarifications C15–C19).
2. `<pending>` — P3: confluent_kafka._config — config coercion, duration,
   callback-key rejection (+ test_config).

`COMMENTS.66.md` was created empty and was empty at start and end.
