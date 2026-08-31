# Deprecation of the librdkafka-based clients

Part of [RFC-0001](README.md).

The existing librdkafka-based clients remain fully supported through a defined window.
Nothing here changes an existing application until its maintainers choose to migrate.

## Timeline

The clock is anchored to the new client reaching GA across all supported languages.

1. **Intent to deprecate (GA + 6 months).** Confluent announces its intent to deprecate.
   This is a heads-up, not the formal deprecation. Waiting six months lets a working
   replacement prove itself in production before anyone plans around it.
2. **Formal deprecation (GA + 1 year).** Formal deprecation follows one year after GA. At
   this point the librdkafka-based clients log a message recommending an upgrade, and no
   new features are added.
3. **End-of-Life (EoL), 3 years.** Security patches and critical bug fixes continue for
   three years after the deprecation announcement.
4. **End-of-Service-Life (EoSL), 1 additional year.** Security patches only, for one year
   after EoL.

After EoSL, the existing packages and repositories remain available for download but no
longer receive updates. Previous major versions remain available, and the community can
fork and maintain the code independently if it wishes.

## What this means in practice

- Existing produce, consume, and admin code keeps working on the current client for the
  whole window.
- Applications can adopt the new client at their own pace, and can run the current and new
  clients side by side during migration.
- The long window, combined with the migration tooling and guides in
  [api-and-migration.md](api-and-migration.md), is intended to give teams ample time to
  migrate.
