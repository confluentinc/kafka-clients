# Root cause: a transient TLS-handshake connection-reset on one broker stops the whole consumer

**Status:** Hypothesis CONFIRMED. The Rust client misclassifies a TCP
connection-reset *during* the TLS handshake as a fatal authentication failure,
whereas Java treats it as a retriable per-node disconnect. A second,
independent layer (the harness) then turns the fatal error into process exit.

**Symptom recap:** steady-state consume against Confluent Cloud over SASL_SSL;
~150s in, broker `54.213.232.68` resets one TLS handshake
(`Connection reset by peer (os error 104)`); the entire benchmark aborts with
`benchmark failed: TLS handshake failed: Connection reset by peer`. librdkafka
and Java KafkaConsumer transparently reconnect through the same blip.

---

## 1. Rust code-path trace (selector handshake error → consumer `poll()` → harness exit)

### 1a. Origin: the reset is wrapped as a generic `ErrorKind::Other`

`src/common/network/ssl_transport_layer.rs`, `handshake()` (the read/write/process loop):

- L246 (write path): `Err(e) => return Err(io::Error::other(format!("TLS handshake failed: {e}")))`
- **L275 (read path): `Err(e) => return Err(io::Error::other(format!("TLS handshake failed: {e}")))`** ← the actual site for "Connection reset by peer (os error 104)" coming out of `read_tls`
- L278 (process_new_packets): `return Err(io::Error::other(format!("TLS handshake failed: {e}")))`

All three failure modes — a transport-level connection reset (os error 104), a
genuine TLS protocol/cert failure surfaced by `process_new_packets()`, and a
TLS write failure — are collapsed into the **same** `io::Error::other(...)`
(`ErrorKind::Other`) with the **same** `"TLS handshake failed: ..."` prefix.
There is no surviving distinction between "transport disconnect" and "TLS
authentication rejection" at this layer.

(`io::Error::other(x)` constructs an error with `kind() == ErrorKind::Other`.)

### 1b. `KafkaChannel::prepare()` stamps the channel `AuthenticationFailed` for ANY handshake error

`src/common/network/kafka_channel.rs`, `prepare()`:

```rust
194        if let Err(e) = result {
195            let remote_desc = self.remote_address.map(|a| a.to_string());
196            self.state = ChannelState::with_error(State::AuthenticationFailed, &e.to_string(), remote_desc.as_deref());
197            if authenticating {
198                self.delay_close_on_authentication_failure();
199            }
200            return Err(e);
201        }
```

`result` (L180–192) is the combined outcome of `transport_layer.handshake()`
**and** `authenticator.authenticate()`. **Any** error from either — including a
plain TCP reset from the TLS handshake — sets `state = AuthenticationFailed`.
There is no check of the error's nature. This is the **primary divergence**
(see §2 for the Java contract).

### 1c. Selector logs it as "Failed authentication" via an `ErrorKind` heuristic

`src/common/network/selector.rs` — two identical sites (read-phase
`poll_channel` result handling and write-phase `poll_channels_write_concurrent`):

```rust
655            if e.kind() == io::ErrorKind::Other || e.kind() == io::ErrorKind::InvalidInput {
656                kafka_error!(self.log_context, "Failed authentication with {} ({})", desc, e);
657            } else {
658                kafka_debug!(self.log_context, "Connection with {} disconnected: {}", desc, e);
659            }
```

(duplicate at L713–717). Because the reset arrived as `ErrorKind::Other`, this
prints the `Failed authentication with 54.213.232.68:9092 (... TLS handshake
failed: Connection reset by peer (os error 104))` line from the log. This
heuristic is the wrong axis (see §3) and is doubly broken (it both
mis-catches resets *and* misses real SASL failures, which use
`ErrorKind::InvalidData` — see §1f).

`close_channel_internal` then records the channel's `AuthenticationFailed`
state into the `disconnected` map (selector.rs L836 / L851:
`self.disconnected.insert(id, channel.state().clone())`).

### 1d. NetworkClient turns the `AuthenticationFailed` channel-state into a stored, fatal auth error

`src/network_client.rs`, `process_disconnection()`:

```rust
880        match disconnect_state.state() {
881            channel_state::State::AuthenticationFailed => {
882                let auth_err = disconnect_state.error().unwrap_or("unknown").to_string();
883                self.connection_states.authentication_failed(node_id, now, auth_err.clone());
884                kafka_error!( ... "Connection to node {} ({}) failed authentication due to: {}" ... );
```

This is the second log line in the report. `connection_states.authentication_failed`
(src/cluster_connection_states.rs:292) stores
`node_state.authentication_error = Some(error)` (L297) for node 25.

### 1e. The stored auth error is surfaced to any pending request as fatal `SaslAuthenticationFailed`

When the next request to node 25 completes (or its `ClientResponse` is built
with `authentication_error` set), `NetworkClientDelegate::on_complete`
(`src/consumer/internals/network_client_delegate.rs`):

```rust
339        if let Some(msg) = response.authentication_error() {
340            self.on_failure(
341                completion_time_ms,
342                KafkaError::with_message(Errors::SaslAuthenticationFailed, msg.to_string()),
343            );
344            return;
345        }
```

Likewise `check_disconnects` (L784) and `maybe_return_auth_failure` (L544–548)
synthesize `Errors::SaslAuthenticationFailed` from a stored auth error.

### 1f. `SaslAuthenticationFailed` is NOT retriable → request manager treats it as fatal

`src/common/protocol/errors.rs`, `is_retriable()` (L393): the retriable set
includes `NetworkException` (L402) but **not** `SaslAuthenticationFailed`.

So in, e.g., `CoordinatorRequestManager::on_failed_response_inner`
(`src/consumer/internals/coordinator_request_manager.rs`):

```rust
267        if error.is_retriable() {            // false for SaslAuthenticationFailed
268            log::debug!("FindCoordinator request failed due to retriable exception: {error}");
269            return;
270        }
...
279        log::warn!("FindCoordinator request failed due to fatal exception: {error}");
280        *inner.fatal_error.lock()... = Some(error);   // stored as FATAL
```

The fatal error is later propagated as a background/error event
(`AbstractHeartbeatRequestManager::maybe_propagate_coordinator_fatal_error_event`,
abstract_heartbeat_request_manager.rs:223) and surfaces out of
`AsyncKafkaConsumer::poll()`.

**Had the same reset been classified `NetworkException`, `is_retriable()` would
be `true`, `on_failed_response` would back off and retry (L267–269), and the
consumer would survive — exactly the librdkafka/Java behavior.**

### 1g. Harness: `poll()?` makes any error fatal to the process

`consumer-perf/src/main.rs`: every poll uses `?`:

- L611 `let recs = consumer.poll(poll_timeout).await?;` (join loop)
- L649 (settle loop), L721 (measure loop), etc.

Any `Err` propagates out of `run()` → `main` (L517–518):

```rust
517    if let Err(e) = run(args).await {
518        eprintln!("benchmark failed: {e}");
```

→ process exit. This is the `benchmark failed: ...` line. The harness makes no
retriable-vs-fatal distinction.

---

## 2. Java code-path (the intended retriable-disconnect vs fatal-auth distinction)

### 2a. `SslTransportLayer.handshake()` distinguishes `SSLException` from `IOException`

`kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java`,
`handshake()` (L280–332):

```java
305        } catch (SSLException e) {
306            maybeProcessHandshakeFailure(e, true, null);   // → SslAuthenticationException (fatal)
307        } catch (IOException e) {
308            maybeThrowSslAuthenticationException();          // only if a real SSL failure is pending
...
323            // If we get here, this is not a handshake failure, throw the original IOException
324            throw e;                                          // ← connection-reset path: plain IOException
325        }
```

A `Connection reset by peer` is a `java.io.IOException`, **not** an
`SSLException`. With no pending SSL failure, Java re-throws the original
`IOException` (L324). A genuine cert/protocol failure is an `SSLException` and
goes through `maybeProcessHandshakeFailure` → `SslAuthenticationException`
(which `extends AuthenticationException`).

### 2b. `KafkaChannel.prepare()` only stamps `AUTHENTICATION_FAILED` for `AuthenticationException`

`kafka/clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java`,
`prepare()` (L174–198):

```java
183        } catch (AuthenticationException e) {
184            // Clients are notified of authentication exceptions to enable operations to be terminated
185            // without retries. Other errors are handled as network exceptions in Selector.
186            String remoteDesc = remoteAddress != null ? remoteAddress.toString() : null;
187            state = new ChannelState(ChannelState.State.AUTHENTICATION_FAILED, e, remoteDesc);
...
192            throw e;
193        }
```

The catch is typed `AuthenticationException`. A plain `IOException`
(connection reset) is **not** caught here — it propagates out of `prepare()`
with the channel state left as `AUTHENTICATE`, never `AUTHENTICATION_FAILED`.
The comment (L184–185) states the contract verbatim: *"Other errors are handled
as network exceptions in Selector."* **This is exactly what Rust violates at
kafka_channel.rs:196.**

### 2c. `Selector` routes by exception type, not by an opaque error kind

`kafka/clients/src/main/java/org/apache/kafka/common/network/Selector.java`
(L609–631):

```java
609            } catch (Exception e) {
611                if (e instanceof IOException) {
612                    log.debug("Connection with {} disconnected", desc, e);   // ← retriable disconnect
613                } else if (e instanceof AuthenticationException) {
...
622                    log.info("Failed {}authentication with {} ({})", ...);    // ← fatal auth
624                } else {
625                    log.warn("Unexpected error from {}; closing connection", desc, e);
626                }
631                    close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
```

`IOException` → "disconnected" (debug), channel state unchanged
(not auth-failed), graceful close → NetworkClient sees a plain disconnect.

### 2d. NetworkClient: a disconnect (no stored auth exception) is retriable

In `NetworkClient.processDisconnection`, only a channel whose state is
`AUTHENTICATION_FAILED` stores an `authenticationException` (fatal, surfaced to
the user and *not* cleared until reconnect). A plain disconnect transitions the
node to `DISCONNECTED`, backs off `reconnect.backoff.ms`, and is retried. In the
KIP-848 async path the per-node disconnect maps to `DisconnectException` /
`NetworkException` (retriable) — the coordinator/heartbeat/fetch managers retry
the node, they do not stop the consumer. An `AuthenticationException` is the
*only* thing that terminates operations without retries (KafkaChannel.java
L184).

---

## 3. Does Rust diverge from Java? Yes — at two layers, on two different axes

| | Java | Rust |
|---|---|---|
| **Classification axis** | exception **type** (`SSLException`/`AuthenticationException` vs `IOException`) | opaque `io::ErrorKind` heuristic (`Other`/`InvalidInput` ⇒ "auth") |
| **TLS transport** | `SSLException` ≠ `IOException`; reset re-thrown as IOException (SslTransportLayer.java L324) | reset, cert failure, and TLS write failure ALL → `io::Error::other` (ssl_transport_layer.rs L246/275/278). Distinction destroyed at the source. |
| **Channel state on handshake error** | `AUTHENTICATION_FAILED` only for `AuthenticationException` (KafkaChannel.java L183) | `AuthenticationFailed` for **any** error (kafka_channel.rs L196) |
| **Reset during handshake** | retriable disconnect → reconnect with backoff | fatal `SaslAuthenticationFailed` → consumer stops |
| **Real SASL auth failure** | `ErrorKind::InvalidData` in `SaslClientAuthenticator` (sasl_client_authenticator.rs L376/385/394/421/477/485/525/545) — **not** caught by the `Other`/`InvalidInput` heuristic at selector.rs:655 | mislabeled as "disconnected" (debug) — the heuristic is wrong in *both* directions |

The single most important divergence is **kafka_channel.rs:196**: it stamps
`AuthenticationFailed` for every error, where Java stamps it only for
`AuthenticationException`. Everything downstream (the stored auth error, the
non-retriable `SaslAuthenticationFailed`, the fatal coordinator error, the
harness exit) is a faithful consequence of that one mislabel. The
`ErrorKind`-heuristic at selector.rs:655 is a parallel symptom of the same
"no typed auth error" design gap.

---

## 4. Recommended fix (described, not implemented)

The clean fix mirrors Java's "auth errors are a distinct type; everything else
is a network disconnect." Introduce a way to carry "this was a genuine
authentication failure" through the `io::Result` boundary, instead of inferring
it from `ErrorKind`.

**Primary fix — preserve the auth-vs-IO distinction at the source and at
`prepare()`:**

1. **`ssl_transport_layer.rs` (origin):** stop collapsing every handshake
   failure into `ErrorKind::Other`. Distinguish:
   - A genuine TLS protocol/cert failure (errors from `process_new_packets()`
     at L277–278, and TLS alerts) → a *typed authentication* error.
   - A transport-level failure from `read_tls`/`write_tls` whose underlying
     `e.kind()` is `ConnectionReset` / `BrokenPipe` / `UnexpectedEof` /
     `ConnectionAborted` (the L246/275 arms, and the EOF arm at L266) →
     preserve the original I/O error kind (do **not** rewrap to `Other`).

2. **`kafka_channel.rs` `prepare()` (L194–200, the linchpin):** only set
   `State::AuthenticationFailed` when the error is a genuine authentication
   error (matching Java's `catch (AuthenticationException)`). For any other
   I/O error (connection reset etc.), leave the state as `Authenticate` and
   return the error unchanged, so the Selector/NetworkClient treat it as a
   network disconnect → retriable. This is the smallest change that restores
   Java's contract (KafkaChannel.java L183–185).

3. **`selector.rs` (L655 and L713):** replace the `ErrorKind::Other ||
   InvalidInput` heuristic with a check that matches the *typed* auth error
   (or, equivalently, the channel's `state()` after `prepare()`), so "Failed
   authentication" is logged only for genuine auth failures and connection
   resets log "disconnected". As a bonus this fixes the current bug that real
   SASL failures (`ErrorKind::InvalidData`) are logged as "disconnected".

**Mechanism options for carrying the auth signal across `io::Result`:**
   - Wrap the genuine-auth error as a custom error type implementing
     `std::error::Error` and put it in `io::Error::new(ErrorKind::Other, AuthErr)`,
     then downcast via `io::Error::get_ref()/downcast_ref::<AuthErr>()` in
     `prepare()` / selector — this is the most faithful to "type, not kind."
   - Or reserve a dedicated `ErrorKind` for auth (e.g. map genuine TLS/SASL
     auth failures to `ErrorKind::PermissionDenied`) and use connection-reset
     kinds for transport failures. Simpler but less precise; acceptable if the
     set of auth-producing call sites is fully enumerated (SSL
     `process_new_packets`, SASL authenticator at the `InvalidData` sites in
     sasl_client_authenticator.rs).

Either way, ensure genuine SASL auth failures (currently `ErrorKind::InvalidData`
in `sasl_client_authenticator.rs`) ARE classified as auth, and a TLS/TCP reset is
NOT.

**Secondary / defense-in-depth — harness resilience (`consumer-perf/src/main.rs`):**
Even with the client fixed, the harness should not exit on a transient error.
Recommend: in the poll loops, on `Err(e)`, check `e.is_retriable()` (KafkaError
exposes this); if retriable, log and continue (optionally with a small backoff
and a cumulative-failure budget) rather than `?`-propagating. A genuinely fatal
error (real auth failure, fenced, fatal config) should still terminate. This
makes the benchmark robust to single-broker blips the way a real consumer is —
but it is strictly secondary: the harness exiting on a *fatal* error is correct;
the bug is that the client manufactured a fatal error from a transient one.

**Suggested fix order:** (1) kafka_channel.rs `prepare()` + (2) the typed error
from ssl_transport_layer.rs are the load-bearing change; (3) selector.rs logging
follows for free; (4) harness resilience is optional hardening. A regression
test should simulate a mid-handshake connection reset and assert the channel
state is NOT `AuthenticationFailed`, the node is marked disconnected/retriable,
and `poll()` does not return a fatal error (mirroring the Java behavior that a
handshake `IOException` is a disconnect, not an auth failure).

---

## 5. Secondary findings

- **The selector `ErrorKind` heuristic is wrong in both directions.** Beyond
  mislabeling resets as auth (the reported bug), it also FAILS to label genuine
  SASL authentication failures as auth: `SaslClientAuthenticator` emits
  `ErrorKind::InvalidData` (sasl_client_authenticator.rs L376, 385, 394, 421,
  477, 485, 525, 545), which the `Other || InvalidInput` test at selector.rs:655
  does not match — so a real bad-credentials failure currently logs as
  "Connection with ... disconnected" at debug and may be retried instead of
  surfaced as fatal. The recommended typed-error fix repairs both directions.

- **Both selector sites must be fixed** (read-phase L655 and write-phase L713)
  — the handshake can fail on either the read or the write half.

- **`SslState::Closed` arms** in ssl_transport_layer.rs use
  `ErrorKind::NotConnected` (e.g. L159/183/218), which the heuristic correctly
  treats as a disconnect — no change needed there, but the typed-error approach
  keeps them correct by construction.

- **Out of scope (known, separate):** the ~25–46s KIP-848 first-record/join
  latency is tracked elsewhere and is unrelated to this fatal-classification
  bug. Not investigated here.
