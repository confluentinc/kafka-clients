# Translation Design: MINOR: Update jackson to 2.20.1 and jackson-annotations to 2.20 (#20951)

- **AK Commit:** ae4dd6c67c9d2ec1fa78a7a1ac2af60a6da3665b
- **AK Branch:** trunk
- **PR:** #110
- **Rust Branch:** kafka-translate/ae4dd6c67c9d2ec1fa78a7a1ac2af60a6da3665b

## Summary of the Original Java Change

This commit updates the Jackson library suite from version 2.19.2 to 2.20.1. Starting
from 2.20, `jackson-annotations` uses a different versioning scheme (2.20 instead of
2.20.x), so it is separated from the other Jackson dependencies in the build config.

Additionally, the deprecated `ObjectMapper.setSerializationInclusion()` method is
replaced with `ObjectMapper.setDefaultPropertyInclusion()` in three files:
- `core/src/main/scala/kafka/docker/KafkaDockerWrapper.scala`
- `generator/src/main/java/org/apache/kafka/message/MessageGenerator.java`
- `trogdor/src/main/java/org/apache/kafka/trogdor/common/JsonUtil.java`

The `LICENSE-binary` file is updated to reflect the new version numbers.

## Files Changed

| File | Change |
|------|--------|
| `gradle/dependencies.gradle` | Bump jackson 2.19.2 -> 2.20.1; add separate `jacksonAnnotations: "2.20"` |
| `LICENSE-binary` | Update listed jackson artifact versions |
| `core/.../KafkaDockerWrapper.scala` | `setSerializationInclusion` -> `setDefaultPropertyInclusion` |
| `generator/.../MessageGenerator.java` | `setSerializationInclusion` -> `setDefaultPropertyInclusion` |
| `trogdor/.../JsonUtil.java` | `setSerializationInclusion` -> `setDefaultPropertyInclusion` |

## Translation to Rust: Analysis

### Applicability

This commit is a **dependency version bump and API migration** in the Java/Gradle build
system. It has **no direct translation** to the Rust codebase because:

1. **Build system changes (`gradle/dependencies.gradle`):** The Rust project uses
   `Cargo.toml` for dependency management, not Gradle. Jackson is a Java-specific
   JSON/YAML serialization library with no equivalent dependency in this Rust project
   (the Rust equivalent would be `serde`/`serde_json`/`serde_yaml`, which are managed
   independently).

2. **`LICENSE-binary` updates:** These reflect Java binary distribution artifacts and
   do not apply to the Rust build.

3. **API migration (`setSerializationInclusion` -> `setDefaultPropertyInclusion`):**
   This is a Jackson-specific API change. The Rust codebase uses `serde` for
   serialization, which has a completely different configuration model (derive macros,
   `#[serde(skip_serializing_if = "...")]`, etc.).

### Recommendation

**No code changes required.** This commit is purely a Java ecosystem dependency
management change with no behavioral impact that would need to be reflected in the
Rust translation.

## Action Items

- [x] Analyze original commit
- [x] Determine applicability to Rust codebase
- [ ] No translation work needed -- mark PR as "no-op" translation
