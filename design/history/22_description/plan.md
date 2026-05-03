# Translation Plan: KAFKA-13022 Optimize ClientQuotasImage#describe

**AK commit:** `ff2ba93a5c5bd4884d6ec2eecde182d1b481ca05`
**AK PR:** apache/kafka#19079
**Rust PR:** #22
**AK JIRA:** KAFKA-13022

---

## Summary of the Java Change

`ClientQuotasImage` stores a `Map<ClientQuotaEntity, ClientQuotaImage>` of all quota entities.
The old `describe` method iterated the entire map on every call, checking each entity's type/name
fields against the filter — O(N) per query.

This commit adds two pre-built index structures to the constructor and rewrites `matches()` to use
them, reducing describe queries to O(k) where k is the size of the result set rather than the total
number of entities:

1. **`entitiesByTypeAndName`** — `Map<entityType, Map<entityName, Map<ClientQuotaEntity, ClientQuotaImage>>>`
   - Allows O(1) lookup by (type, name) pair for `MATCH_TYPE_EXACT` and `MATCH_TYPE_DEFAULT` filters.

2. **`entitiesByType`** — `Map<entityType, Map<ClientQuotaEntity, ClientQuotaImage>>`
   - Allows O(1) lookup by entity type for `MATCH_TYPE_SPECIFIED` filters.

Both indexes are built once in the constructor and are immutable after that. The indexes are derived
from the primary `entities` map so correctness is maintained; `equals`/`hashCode` are still based
solely on `entities`.

The commit also adds `ClientQuotasImageTest` (306 lines, comprehensive describe tests covering all
three match types, strict mode, error conditions) and `ClientQuotasImageDescribeBenchmark` (JMH
benchmark with 10/100/1000 entities per type).

---

## Files Changed in AK

| File | Change |
|------|--------|
| `metadata/src/main/java/org/apache/kafka/image/ClientQuotasImage.java` | +99/-28: add two index fields, rewrite constructor and `matches()` |
| `metadata/src/test/java/org/apache/kafka/image/ClientQuotasImageTest.java` | +306: new test class |
| `jmh-benchmarks/src/main/java/org/apache/kafka/jmh/metadata/ClientQuotasImageDescribeBenchmark.java` | +100: new JMH benchmark |

---

## Rust Translation Scope

This is a **new subsystem** — there is no existing `ClientQuotasImage` in the Rust codebase. The
translation introduces the metadata image layer for client quotas. The scope is limited to what the
AK commit modifies: the image, its describe logic, and corresponding tests. The delta
(`ClientQuotasDelta`) and writer (`ImageWriter`) integration are out of scope for this PR.

### New Rust Files

| Rust file | Java equivalent |
|-----------|----------------|
| `src/image/mod.rs` | image module root |
| `src/image/client_quotas_image.rs` | `ClientQuotasImage.java` |
| `src/image/client_quota_image.rs` | `ClientQuotaImage.java` (quota values) |
| `src/image/client_quota_entity.rs` | `ClientQuotaEntity.java` (entity key) |

Or alternatively, all image types in a single `src/image/client_quotas.rs` if the types are small
enough. The Actor should prefer the flat single-file layout if the resulting file stays under ~400
lines; otherwise split as above.

---

## Data Type Mapping

### `ClientQuotaEntity` (key type)

Java: `Map<String, String> entries` where keys are `"user"`, `"client-id"`, or `"ip"`.

Rust:
```rust
/// Wraps a sorted map of (entity_type -> Option<entity_name>).
/// `None` name means the default entity for that type (MATCH_TYPE_DEFAULT).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientQuotaEntity {
    pub entries: BTreeMap<String, Option<String>>,
}

impl ClientQuotaEntity {
    pub const USER: &'static str = "user";
    pub const CLIENT_ID: &'static str = "client-id";
    pub const IP: &'static str = "ip";
}
```

`BTreeMap` is used instead of `HashMap` so that the `Hash` derive is deterministic and the type can
serve as a map key without manual `Hash` / `Ord` impls.

### `ClientQuotaImage` (value type — per-entity quotas)

Java: `Map<String, Double>` mapping quota key strings to values.

Rust:
```rust
#[derive(Clone, Debug, PartialEq)]
pub struct ClientQuotaImage {
    pub quotas: HashMap<String, f64>,
}
```

### `ClientQuotasImage` (aggregate image)

Java fields:
- `entities: HashMap<ClientQuotaEntity, ClientQuotaImage>`
- `entitiesByTypeAndName: HashMap<String, HashMap<String, HashMap<ClientQuotaEntity, ClientQuotaImage>>>`
- `entitiesByType: HashMap<String, HashMap<ClientQuotaEntity, ClientQuotaImage>>`

Rust: The index maps contain `Arc`-wrapped values (or `Rc` if single-threaded) to avoid cloning the
quota images into every index bucket. Since Java's `ClientQuotasImage` is documented as thread-safe,
use `Arc<ClientQuotaImage>`.

```rust
pub struct ClientQuotasImage {
    entities: HashMap<ClientQuotaEntity, Arc<ClientQuotaImage>>,
    entities_by_type_and_name: HashMap<String, HashMap<Option<String>, HashSet<ClientQuotaEntity>>>,
    entities_by_type: HashMap<String, HashSet<ClientQuotaEntity>>,
}
```

**Design note:** Rather than storing the full `(entity, image)` pair in each index bucket (which
would require cloning), the indexes store only the `ClientQuotaEntity` keys; the `ClientQuotaImage`
value is looked up from the primary `entities` map when building the response. This keeps the
indexes lightweight and avoids the Rust borrow-checker complexity of nested mutable index maps that
reference the outer map's values.

The `Option<String>` in `entities_by_type_and_name` maps `None` to the default entity name (Java's
`null` for `MATCH_TYPE_DEFAULT`).

---

## `describe` Method Translation

### Request types

The `DescribeClientQuotasRequestData` and `DescribeClientQuotasResponseData` are generated from the
Kafka protocol schema. The generator already produces these types; verify they are present in
`OUT_DIR/generated/`.

Match type constants:
```rust
pub const MATCH_TYPE_EXACT: i8 = 0;
pub const MATCH_TYPE_DEFAULT: i8 = 1;
pub const MATCH_TYPE_SPECIFIED: i8 = 2;
```

### Error mapping

| Java exception | Rust equivalent |
|----------------|----------------|
| `InvalidRequestException` | `KafkaError::InvalidRequest(String)` (existing type) |
| `UnsupportedVersionException` | `KafkaError::UnsupportedVersion(String)` (existing type) |

The `describe` method returns `Result<DescribeClientQuotasResponseData, KafkaError>`.

### Algorithm (direct translation of Java `matches()`)

```
Case 1 — exactMatch is non-empty:
  For each (type, name) in exactMatch:
    look up entities_by_type_and_name[type][name] -> set of entity keys
    intersect with candidates (None = use first set as initial candidates)
  For each type in typeMatch:
    intersect candidates with entities_by_type[type]

Case 2 — exactMatch empty, typeMatch non-empty:
  For each type in typeMatch:
    look up entities_by_type[type] -> set of entity keys
    intersect with candidates

Case 3 — no filters, not strict:
  return all entities

Final pass over candidates:
  if strict: only include entities where entity.entries.len() == exactMatch.len() + typeMatch.len()
  build EntryData from entity + image
```

Set intersection in Rust: `candidates.retain(|k| other_set.contains(k))`.

---

## Testing

Translate `ClientQuotasImageTest` fully. Key test cases to cover:

1. **`test_empty_image`** — describe on empty image returns empty.
2. **`test_describe_non_strict_exact_match`** — parameterized over USER/CLIENT_ID/IP. Exact match
   on "foo" returns 1 entry; exact match on "nonexistent" returns 0.
3. **`test_describe_strict_mode`** — strict=true + multiple exact-match components returns only
   entity with exactly those components.
4. **`test_describe_type_match`** — `MATCH_TYPE_SPECIFIED` with strict=false returns all entities
   containing that type.
5. **`test_describe_default_match`** — `MATCH_TYPE_DEFAULT` (null name) matches entities whose
   name for that type is `None`.
6. **`test_describe_all_non_strict`** — empty component list + strict=false returns all entities.
7. **`test_describe_invalid_entity_type`** — unsupported type returns `UnsupportedVersionException`.
8. **`test_describe_ip_with_user_error`** — IP + USER/CLIENT_ID combination returns error.
9. **`test_describe_duplicate_entity_type`** — same type appears twice returns error.
10. **`test_equals_and_hash`** — two images with same entities are equal; order-insensitive.

The JMH benchmark (`ClientQuotasImageDescribeBenchmark`) should be translated as a Rust criterion
benchmark in `benches/client_quotas_image.rs`, covering the same three scenarios
(specified/default/exact) with 10/100/1000 entities per type.

---

## Implementation Steps

1. **Add supporting types** (`ClientQuotaEntity`, `ClientQuotaImage`) with `PartialEq`, `Eq`,
   `Hash`, `Clone`, `Debug`. Use `BTreeMap` for `ClientQuotaEntity::entries` to get deterministic
   hashing.

2. **Implement `ClientQuotasImage::new`** — build both indexes in the constructor from the `entities`
   map. Use `entry().or_default()` patterns to avoid redundant `contains_key` checks.

3. **Implement `describe`** — validate request components (match type, entity type, IP/user
   exclusion), then call `matches()`.

4. **Implement `matches`** — three-case logic as described above. Use `HashSet::retain` for
   intersection.

5. **Implement `to_describe_entry`** — build `EntryData` from `ClientQuotaEntity` +
   `ClientQuotaImage`.

6. **Wire up the module** — add `pub mod image;` to `src/lib.rs` (or under a relevant parent
   module) and add `pub mod client_quotas_image;` to `src/image/mod.rs`.

7. **Write unit tests** — cover all scenarios from `ClientQuotasImageTest`.

8. **Add criterion benchmark** (optional, can be a follow-up PR) — mirror JMH benchmark scenarios.

9. **`cargo test`** — all tests pass. **`cargo clippy`** — no warnings.

---

## Out of Scope

- `ClientQuotasDelta` (Java: `ClientQuotasDelta.java`) — applies metadata record log changes to
  build a new `ClientQuotasImage`. Not modified by this AK commit.
- `ImageWriter` integration — writing quotas back to the metadata log.
- `ClientQuotasImageNode` — the metadata tree node for describe output formatting.
- Admin client `DescribeClientQuotas` RPC handler — out of scope for the image layer.
