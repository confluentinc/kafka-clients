---
name: phase19-ssl-recv-selector-perf
description: Milestone-8 Phase 19 — Arc<str> selector channel ids + synchronous SslTransportLayer::try_read; public-API constraint kept connected/disconnected as String
metadata:
  type: project
---

Milestone-8 Phase 19 (two consumer receive/poll hot-path CPU optimizations, profiled under SASL_SSL).

**Fix 2 — Arc<str> channel ids in Selector** (`src/common/network/selector.rs`):
- Converted to `Arc<str>`: `channels` map key, `closing_channels` key, `explicitly_muted_channels`, `channels_with_buffered_read`, `immediately_connected_keys`, `failed_sends`. Per-poll `channels.keys().cloned()` became refcount bumps (was 2.65% CPU in malloc).
- **Public-API constraint**: `Selectable` trait returns `disconnected() -> &HashMap<String, ChannelState>` and `connected() -> &[String]` (selectable.rs). These are the API contract — kept those two FIELDS as `String` (task said "do NOT change public API shape", overriding PLAN.md which listed them). They are cleared/repopulated per poll, not the dominant clone, so no loss.
- `IdleExpiryManager.lru_connections` left `String`-keyed: interning there needs `self.channels` borrow while mgr is mutably borrowed (conflict); its `update` only allocates on channels with I/O activity, same as Java.
- Added `intern_id(&self, id: &str) -> Arc<str>` helper: `channels.get_key_value(id).map(|(k,_)| Arc::clone(k)).unwrap_or_else(|| Arc::from(id))` — reuses the one Arc per connection across tracking sets.
- Borrow gotchas solved: `HashSet<Arc<str>>::remove`/`HashMap<Arc<str>,_>::contains_key` with a `&String` does NOT compile (`Arc<str>: Borrow<String>` not impl). Use `.remove(id.as_str())` / `.contains_key(id.as_str())`. A `&Arc<str>` DOES coerce to `&str` for fn args via Deref.
- `mute()`: get `Arc<str>` via `get_key_value(id)` then `Arc::clone` BEFORE `get_mut` (can't intern via `&self` while channel is `&mut`).
- `send()` Err arm: can't call `self.intern_id()` (needs `&self`) while `channel = get_mut(...)` is live; used `Arc::from(connection_id.as_str())` (error path, rare, matches Java alloc).

**Fix 1 — synchronous SslTransportLayer::try_read** (`src/common/network/ssl_transport_layer.rs`):
- `supports_try_read()` flipped `false`→`true`; added sync `try_read(&mut self, dst)` = byte-for-byte the async `read` logic without `Box::pin`/`.await`: non-blocking `read_tls` via `TryReadAdapter` (tracks `tcp_eof` on Ok(0)) → `process_new_packets()` (map err `io::Error::other`) → `conn.reader().read(dst)` with n>0→Ok(n), n==0&&tcp_eof→Ok(0), n==0→WouldBlock, reader-WouldBlock→(tcp_eof?Ok(0):WouldBlock).
- Why safe re join-stall: the rootcause is `select!` cancelling `poll_default` mid-connect (a future with side-effects-before-await). `try_read` is sync → never awaited → never cancelled. Did NOT touch selector poll/wakeup/Notify/cancel machinery.
- `has_bytes_buffered()` (`!conn.wants_read()`) unchanged → when `NetworkReceive` drain loop fills `dst` and breaks with leftover plaintext, selector's `channels_with_buffered_read` re-poll still fires. Receive-loop semantics in network_receive.rs Phase 3 already match (Ok(0)→UnexpectedEof, WouldBlock→return-so-far).
- Tests added: `test_try_read_before_handshake` (WouldBlock), `test_try_read_buffered_plaintext_then_eof` (sync drain → WouldBlock-while-open → Ok(0)-after-server-close). Use `#[tokio::test(flavor="multi_thread", worker_threads=2)]` + `drive_server_send` helper.

**Workspace gotcha**: `cargo xtask format` reformats ALL crates incl out-of-scope `consumer-perf/src/main.rs` (pre-existing uncommitted change). Use `rustfmt <file>` to format only your file; `consumer-perf/src/main.rs:549` diff in format-check is pre-existing, not mine.
