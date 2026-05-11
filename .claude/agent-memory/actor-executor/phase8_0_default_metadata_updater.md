---
name: Phase 8.0 DefaultMetadataUpdater + MetadataUpdaterContext
description: How the Java inner class on NetworkClient that needs enclosing-instance access was modelled in Rust + the Option<M> take/put-back pattern in NetworkClient::poll
type: project
---

Phase 8.0 translates `org.apache.kafka.clients.NetworkClient.DefaultMetadataUpdater`
(Java inner class, NetworkClient.java:1174-1380) as a free `pub(crate) struct`
in `src/default_metadata_updater.rs`.

**Why:** Java's inner class accesses the enclosing `NetworkClient`'s private
helpers (`canSendRequest`, `sendInternalMetadataRequest`, `initiateConnect`,
`leastLoadedNode`, `isAnyNodeConnecting`). Rust has no inner-class semantics,
so this access must be modelled explicitly.

**How to apply:** Three coupled choices made the translation work without
bidirectional Arc cycles:

1. **New `pub trait MetadataUpdaterContext`** (in `metadata_updater.rs`) holds
   the callback surface — one method per Java NetworkClient helper invoked by
   `DefaultMetadataUpdater::maybeUpdate(long, Node)`. NetworkClient impls this
   trait for itself.

2. **`MetadataUpdater::maybe_update` signature changed** to take
   `&mut dyn MetadataUpdaterContext` plus the now. All impls
   (`ManualMetadataUpdater`, `DefaultMetadataUpdater`, the
   `RecordingMetadataUpdater` test mock in `network_client.rs::tests`) take
   the context; the manual ones ignore it.

3. **`NetworkClient::metadata_updater` is `Option<M>`**, not `M`. In `poll()`
   the implementation does:
   ```rust
   let mut updater = self.metadata_updater.take().expect("present");
   let timeout = updater.maybe_update(self, now);
   self.metadata_updater = Some(updater);
   ```
   This satisfies the borrow checker (the updater is OUT of `self` while
   `&mut self` is passed as the context). Two private helper functions
   `metadata_updater()` / `metadata_updater_mut()` panic with a clear message
   if invoked while the slot is `None` — a backstop for code that re-enters
   during the context dispatch.

4. **Re-entry guards in `initiate_connect` and `do_send`:** these are called
   from inside the context dispatch (`MetadataUpdaterContext::initiate_connect`
   and `…::send_internal_metadata_request`) where `metadata_updater` is
   already `None`. The two sites that previously called
   `self.metadata_updater_mut().handle_failed_request` / `handle_server_disconnect`
   are now guarded with `if let Some(updater) = self.metadata_updater.as_mut()`
   — non-recursion-safe paths get skipped (Java's inner-class code doesn't
   re-trigger them either because the metadata request never reaches the wire
   on those paths).

**Metadata-builder factory injection:** `Metadata` now holds an optional
`MetadataRequestBuilderFn` closure (set via `set_request_builder_fn`).
`ProducerMetadata::new` installs a closure that returns
`MetadataRequest::Builder::for_topic_names(...)` over the producer's known
topics. The plain `Metadata` (no factory) defaults to `all_topics()` for full
updates with partial updates disabled — matches Java's base-class behaviour.

**Test gotcha:** `metadata::tests::partial_metadata_update_full_vs_partial`
needs to install a builder-factory explicitly (matching Java's anonymous
subclass override of `newMetadataRequestBuilderForNewTopics`). Before this
change the Rust code returned `is_partial_update = true` purely based on the
gate, which diverged from Java's "fall back to full when partial-builder is
null" semantics.
