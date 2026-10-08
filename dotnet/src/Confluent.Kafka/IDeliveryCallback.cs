// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace Confluent.Kafka;

/// <summary>
/// The completion callback of a record send — the C# realization of Java's
/// <c>org.apache.kafka.clients.producer.Callback</c>. Pass an implementation to
/// <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/> or
/// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, IDeliveryCallback, System.Threading.CancellationToken)"/>,
/// which restore Java's <b>second</b> <c>send</c> signature
/// (<c>Future&lt;RecordMetadata&gt; send(ProducerRecord, Callback)</c>,
/// <c>Producer.java:86</c>) — the callback is an <b>additional</b> parameter, not an alternative:
/// the overload still returns the record's <see cref="KafkaFuture{T}"/> (sync) / yields the record's
/// <see cref="AsyncKafkaFuture{T}"/>, whose <see cref="AsyncKafkaFuture{T}.Get"/> is the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/> (async), exactly as the plain overload does.
/// </summary>
/// <remarks>
/// <para>
/// <b>The method is synchronous, not <see cref="System.Threading.Tasks.Task"/>-returning
/// (the §4 delivery-callback divergence, M14/P1 decision D2).</b> Java's
/// <c>Callback.onCompletion</c> returns <c>void</c> (<c>Callback.java:61</c>), and the invocation
/// site here is a thread the producer owns — its send-completion pump, on both surfaces, or on the
/// async surface its send-batch thread — so an awaited callback would stall every other completion
/// behind it. This is the same divergence, for the same reason, as
/// <see cref="IOffsetCommitCallback"/> and <see cref="IConsumerRebalanceListener"/>.
/// </para>
/// <para>
/// <b>Which thread it runs on.</b> Java documents that its callback "will generally execute in
/// the background I/O thread so it should be fast" (<c>Callback.java:20-21</c>). The .NET
/// analogue depends on which surface you sent through:
/// <list type="bullet">
/// <item><b>Async</b> (<see cref="IAsyncProducer{TKey, TValue}"/>) — for a record whose
/// core-reported completion the binding reads, on the producer's <b>send-completion pump
/// thread</b>, the .NET counterpart of Java's I/O thread. There is one such thread per producer and
/// it walks its completions one at a time, so the invocations it makes are serialized with respect
/// to one another, and a slow callback there delays every other send's completion. Keep it short.
/// Failure outcomes can be delivered from elsewhere: a record the core rejects as the batch is
/// handed over is delivered from the producer's <b>send-batch thread</b>.</item>
/// <item><b>Sync</b> (<see cref="IProducer{TKey, TValue}"/>) — on the same <b>send-completion pump
/// thread</b>, which the sync producer starts on its first <c>Send</c> (M11/P4.2 decisions D3/D4),
/// before the returned future's <see cref="KafkaFuture{T}.Get"/> returns or throws. Every sync
/// invocation runs there, so one producer's sync callbacks are serialized with respect to one
/// another, and a slow callback delays every later send's <c>Get</c> and callback. ⚠ Until
/// M11/P4.2 the sync callback ran inline on each calling thread, so one instance passed to
/// concurrent <c>Send</c> calls was entered on N threads at once; since decision D3 it is not.</item>
/// </list>
/// <b>What this means for you: a callback instance you share across producers, or across async
/// sends, must itself be thread-safe</b> — synchronize any mutable state it touches (a per-send
/// instance needs nothing). A single instance shared across <em>two</em> producers can be entered
/// from each producer's own pump, on either surface. On the <b>async</b> surface an invocation made
/// away from the pump — the send-batch thread's per-record rejection — can overlap one the pump is
/// making. See the §4 delivery-callback divergence in the binding's <c>CLAUDE.md</c>. ⚠ These
/// remarks used to give the async surface an unqualified per-producer non-concurrency guarantee, on
/// the reasoning that the pump was the only thread firing them; the send-batch thread's site
/// falsifies it (Critic 72 finding 72.10). ⚠ They then justified the obligation by
/// asserting Java runs <em>every</em> <c>Callback</c> on one background I/O thread so a Java user
/// never has to make one thread-safe — also false, and the cited line does not say it:
/// <c>Callback.java:20-21</c> reads "<em>generally</em> execute in the background I/O thread".
/// <c>KafkaProducer.doSend</c>'s <c>catch (ApiException)</c> invokes the callback on the
/// <b>application</b> thread while <c>ProducerBatch.completeFutureAndFireCallbacks</c> fires others
/// on the Sender thread, so one shared <c>Callback</c> can be entered from two threads in Java too
/// (finding 72.17). The obligation above stands on its own; it needs no claim about Java.
/// Do not block on producer progress from inside it while it is running on the pump thread: that is
/// the same thread that must resolve every other in-flight send. On the synchronous surface,
/// calling <see cref="KafkaFuture{T}.Get"/> there on a not-yet-completed future of the same producer
/// throws <see cref="System.InvalidOperationException"/> instead of deadlocking (M11/P4.2 decision
/// D9); wait for it from another thread.
/// </para>
/// <para>
/// <b>Sending again from inside it IS supported</b> — the canonical retry-on-failure shape — because
/// no managed lock is held while the callback runs, wherever it runs. The reentrant send is accepted
/// and queued as usual: on the async surface the accumulator drains it, and on the sync surface
/// <c>Send</c> returns once the core has accepted it (blocking the pump only while the core waits
/// for metadata or for <c>buffer.memory</c>, up to <c>max.block.ms</c>). Just do not wait for it
/// there (above). Both surfaces are covered by a regression test.
/// </para>
/// <para>
/// <b>Tearing the producer down from inside it is NOT supported, on either surface</b>: it is
/// running on a thread the producer owns — the send-completion pump (both surfaces) or the async
/// surface's send-batch thread. <c>Close</c> / <c>Dispose</c> stop and <b>join</b> both, so calling
/// either from there asks a thread to wait for itself. Java guards the equivalent explicitly (its
/// <c>close</c> detects being called from the sender thread and closes with a zero timeout,
/// <c>close(0)</c>, skipping the join); this binding does not (M11/P4.2 §13 R6, follow-up FU-3).
/// Close the producer from the thread that owns it instead. ⚠ Until M11/P4.2 these remarks said
/// this self-join did not arise on the sync surface, where the callback then ran inline on the
/// caller's thread; since decisions D3/D4 the sync callback runs on the pump, so it does.
/// </para>
/// <para>
/// <b>Ordering — it runs BEFORE the send's result is observable (decision D3).</b> Java sets the
/// future's value, fires the callbacks, and only then releases the future's waiters
/// (<c>ProducerBatch.java:303-323</c> — <c>produceFuture.done()</c> is last). This binding
/// reproduces that exactly: the callback is invoked immediately before the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/> is completed (async) and before the returned
/// future's <see cref="KafkaFuture{T}.Get"/> returns or throws (sync). Note this is <b>stricter</b>
/// than the Python sibling, which resolves
/// its future first and then invokes <c>on_delivery</c> (<c>producer.py:322-327</c>), so a Python
/// awaiter can be released before the callback has run. ⚠ On the async surface the callback and
/// the delivery task are <b>unordered relative to <c>Send</c>'s first stage</b> (M11/P3.5): a
/// saturated send's first stage can complete after its record was already delivered.
/// </para>
/// <para>
/// <b>The <c>metadata</c> argument is never <see langword="null"/> — not even on failure
/// (decision D2/D6).</b> Java's user callback never sees a null: the wrapper Java registers
/// substitutes a placeholder before forwarding
/// (<c>KafkaProducer.java:1597-1599</c> — <c>new RecordMetadata(topicPartition(), -1, -1,
/// NO_TIMESTAMP, -1, -1)</c>), and the interface documents that as the contract: "When exception
/// is not null in the callback, metadata will contain the special -1 value for all fields"
/// (<c>Callback.java:28-33</c>). So on the failure path this binding delivers a
/// <see cref="RecordMetadata"/> carrying the record's topic, the record's explicit partition (or
/// <c>-1</c> when it let the producer choose), and <c>-1</c> for offset and timestamp. This makes
/// .NET <b>stricter</b> than Python, whose <c>on_delivery</c> receives <c>None</c> on failure.
/// </para>
/// <para>
/// <b>Which outcomes fire it, and which do not (decision D5).</b> The rule is: <b>a throw out of
/// <c>Send</c> means no callback; a send whose core-reported completion the binding reads —
/// successfully or not — fires it.</b> Java's <c>doSend</c> splits
/// the same way — by whether the failure is an <c>ApiException</c>, not by who produced it: the
/// terminal
/// <c>catch (InterruptedException / KafkaException / Exception)</c> clauses re-throw without
/// invoking the callback (<c>KafkaProducer.java:1069-1081</c>), while an appended record's
/// callback fires later on the I/O thread. Note the two halves of that rule are <em>not</em>
/// complements: a record can be accepted by the core and still never have its completion read
/// (the third entry below, and residual 4 under <b>Recorded residuals</b>) — though on the
/// <b>async</b> surface that no longer surfaces as a throw out of <c>Send</c>, because since
/// M11/P3.1 the async <c>Send</c> hands the record to a binding-side accumulator and does not touch
/// the core at all. Concretely:
/// <list type="bullet">
/// <item>a <see cref="System.ArgumentNullException"/> (null record or null callback), an
/// <see cref="System.ObjectDisposedException"/> (closed producer), a
/// <see cref="SerializationException"/> from a serializer, or a
/// <see cref="System.OperationCanceledException"/> from a token that was already canceled →
/// <b>no callback</b> (nothing was sent). Java's <c>SerializationException</c> extends
/// <c>KafkaException</c>, not <c>ApiException</c>, so it too takes the throwing branch;</item>
/// <item>a <see cref="KafkaException"/> raised <em>synchronously</em> by the send itself — the core
/// rejected the record before accepting it → <b>no callback</b>. Nothing was accepted, so nothing
/// is owed;</item>
/// <item>an unexpected failure — in practice an <see cref="System.OutOfMemoryException"/> — raised
/// <em>after</em> the core accepted the record but before the binding could arrange to read its
/// completion → <b>no callback</b>. Here the record <em>was</em> accepted and may still be
/// delivered, so this outcome is neither "nothing was sent" nor "the core rejected
/// it": it is a recorded <em>drop</em>, residual 4 below. On the sync surface it throws out of
/// <c>Send</c>; on the async surface it faults the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/>, because the site is the send-batch
/// thread;</item>
/// <item>the record was accepted and the core later reported success → <b>fires</b> with the real
/// metadata and a <see langword="null"/> exception;</item>
/// <item>the record was accepted and the core later reported a delivery failure → <b>fires</b>
/// with the placeholder metadata and the <see cref="KafkaException"/>, and the future's
/// <see cref="KafkaFuture{T}.Get"/> still throws (sync) / the
/// <see cref="System.Threading.Tasks.Task{TResult}"/> still faults (async) with the same error.</item>
/// </list>
/// It also fires for an async send whose delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/> was already canceled through its
/// <see cref="System.Threading.CancellationToken"/> (decision D7): the
/// delivery notification is owed per record, not per awaiter — the same obligation Python states
/// ("invoked exactly once per record, even if the returned <c>Future</c> was cancelled or already
/// resolved", <c>producer.py:301-303</c>).
/// </para>
/// <para>
/// <b>Recorded residuals — the exhaustive set of paths that fault a send, or throw out of
/// <c>Send</c>, WITHOUT notifying you.</b> The callback reports a <em>core</em> completion, so
/// wherever the binding faults a send <em>itself</em> instead of reading one, the notification is
/// dropped: the send fails — its delivery <see cref="System.Threading.Tasks.Task{TResult}"/> faults
/// (async), its future's <see cref="KafkaFuture{T}.Get"/> throws (sync), or <c>Send</c> throws — and
/// the callback does not fire. The set below is not a remembered list — it is obtained by walking
/// <em>every</em> site between the core's acceptance of the record (a live future and no
/// synchronous error) and the callback's invocation that can fault the send or throw out of
/// <c>Send</c>, through every frame the record's future travels, on every thread it reaches, for
/// both surfaces. The walk was redone in M11/P4.2, when the synchronous send moved its completion
/// onto the pump. There are exactly five such <em>sites</em>, and no others, under four residual
/// numbers: residuals 1, 2 and 3 each have one site that both surfaces reach, and residual 4 has one
/// site per surface, because each surface hands its future to the pump from its own code. Each site
/// carries a note in the code that points here. Note a site is not the same as a
/// <em>condition</em> — residual 3's single site is reached by two conditions, one either side of
/// the completion's arrival, and both are stated under it:
/// <list type="number">
/// <item><b>Teardown raced the enqueue</b> — the record was handed to native, but by the time the
/// binding went to hand it to the completion pump the pump's gate had closed, so the send is faulted
/// in place.</item>
/// <item><b>Teardown drained a still-queued send</b> — the producer was closed with that send queued
/// for the pump and not yet resolved; the queue is faulted wholesale, with no blocking read of the
/// core's results.</item>
/// <item><b>An unexpected failure on the completion pump, after the send was handed to it and
/// before the callback was invoked</b> — <em>not</em> a teardown path. It spans <b>both sides of
/// the completion's arrival</b>, because the pump can throw on either side of its read and one
/// fault site covers both. <b>(a) After</b> the read reported: the core <em>did</em> report the
/// completion. An async group's read reported for the <em>whole</em> batch, so the indices the pump
/// had already reached fired normally and the rest are faulted with none; a sync send's one record
/// is faulted with none. This is the sub-case that makes firing from the fault path unsafe (see
/// below).
/// ⚠ <b>Sub-case (a) NARROWED in M11/P3.2 (§3B, S3) — like (b), it did not vanish.</b> "The whole
/// batch" used to mean whatever the pump's flat per-record queue happened to hold: up to 1100
/// records drawn from arbitrarily many unrelated sends. The pump's unit is now <b>one
/// <c>send_batch</c> call's</b> records, so one such event faults only records that were sent
/// together — a bounded and <em>related</em> blast radius rather than an arbitrary mixture. The
/// batched read itself is unchanged, so this stays a recorded residual rather than a closed one.
/// <b>(b) Before</b> the read reported: the pump threw out of the read itself — an async group's
/// batched read, or a sync send's singular blocking <c>get</c> — so no completion was ever in hand
/// and the entry is faulted. This condition does <b>not</b> need an allocation failure to be
/// reachable: a native-side failure surfacing from the pump's first read, for example an
/// <see cref="System.EntryPointNotFoundException"/> against a stale or mismatched native library,
/// lands here.
/// ⚠ <b>Sub-case (b) NARROWED in M11/P3.1 (§12.3) — it did not vanish.</b> It used to have two
/// triggers: the batched read itself, and the pump throwing while <em>setting the batch up</em> (the
/// three marshalling arrays, allocated per batch outside the processing <c>try</c>). Those arrays
/// are now reused fields allocated once, so there is no pre-read allocation left to fail and that
/// trigger is gone. The batched read remains, which is why this stays a recorded residual rather
/// than a closed one.
/// ⚠ <b>The synchronous send reaches this site itself since M11/P4.2.</b> Its record is read on the
/// pump by the singular <c>get</c>, as its own entry in the same queue, so a throw out of that read,
/// or after it and before the callback, lands in this fault site and faults the send's future. These
/// remarks used to record only a window the synchronous surface "shares", between its blocking
/// <c>get</c> on the caller's thread and the callback, with the throw propagating out of
/// <c>Send</c>; that window no longer exists.</item>
/// <item><b>An unexpected failure between the core accepting the record and the send being handed
/// to the completion pump</b> — <em>not</em> a teardown path. The record <em>was</em> accepted (the
/// core returned a live future and no error) and may still be delivered, but the binding destroys
/// that future unread, so no completion is ever read. Each surface has its own site:
/// <b>Async</b> — on the <b>send-batch thread</b>, between <c>send_batch</c> returning a live future
/// for that index and the accumulator handing it over. In practice an
/// <see cref="System.OutOfMemoryException"/>, whether the pump's enqueue gate is still open or has
/// already closed, or a P/Invoke failure from a later chunk of the same node.
/// ⚠ <b>M11/P3.2 (§3B, S3) WIDENED this residual's window and narrowed neither trigger — checked,
/// not assumed.</b> The hand-over is now one <c>Enqueue</c> per <c>send_batch</c> call rather than
/// one per record, and it runs after <em>every</em> chunk of the node has been sent (which is what
/// keeps the pins released before any future reaches the pump). So both triggers survive, and the
/// window they open now covers <b>all</b> of a call's accepted records rather than only the ones
/// not yet individually handed over: a failure anywhere in a call's walk leaves that whole call's
/// records untransferred. That is a change of <em>scope</em>, not of kind — same site, same
/// condition, same reason — so it is recorded here rather than filed as a fifth residual. (Had the
/// hand-over instead run immediately after each chunk's own <c>send_batch</c>, the later-chunk
/// trigger would have gone away; it does not, and the narrowing is deliberately not written.)
/// <b>Sync</b> (M11/P4.2) — inside <c>Send</c>, on the caller's thread, between
/// <c>Producer_send</c> returning the live future and its hand-over to the pump. In practice an
/// <see cref="System.OutOfMemoryException"/>, whether the pump's enqueue gate is still open or has
/// already closed. The future is destroyed unread and <c>Send</c> rethrows.
/// <para>
/// ⚠ <b>The async site moved in M11/P3.1 and its shape narrowed with it.</b> While the async
/// send called the core inline, the window sat inside <c>Send</c> itself — so it surfaced as a
/// <b>throw out of <c>Send</c></b> for a record the core had accepted, and it was the reason the D5
/// outcome list could not attribute every no-callback throw to "nothing was sent". Since the send
/// submission became deferred, <c>Send</c> no longer touches the core at all: it pins, appends to
/// the accumulator and returns, so <b>every</b> throw out of the async <c>Send</c> is now a case
/// where nothing reached the core. The window itself did not disappear — it moved onto the batch
/// thread, where it faults the awaiter rather than throwing at the caller. A record that the
/// accumulator accepted but that is abandoned <em>before</em> <c>send_batch</c> is deliberately
/// <b>not</b> a residual: the core never saw it, so the binding faults it <em>and</em> fires the
/// callback, which invents nothing and cannot duplicate.
/// </para></item>
/// </list>
/// <b>The distinguishing axes, stated here and nowhere else.</b> This paragraph is the single
/// place the residuals are compared with each other; the numbered notes in the code state each
/// site's own identity and local reason and point here rather than paraphrasing these axes. Several
/// review rounds were spent on paraphrases that went stale one at a time, and re-scoping a
/// paraphrase produced the next round's stale clause each time — so outside this paragraph the
/// comparison is not re-worded, it is simply not made.
/// By <em>cause</em>: residuals 1 and 2 are teardown; residuals 3 and 4 are unexpected failures,
/// and "out of memory" does not characterize them as a group — the frames in residual 3's window
/// include bare P/Invokes, so an entry-point failure against a stale or mismatched native library
/// reaches it too. By <em>what the core reported</em>: a completion had arrived only in residual
/// 3's sub-case (a); residuals 1, 2, 4 and sub-case (b) never read one. By <em>how the send
/// surfaces the failure</em>: on the <b>async</b> surface all four fault the send's
/// <see cref="System.Threading.Tasks.Task{TResult}"/>, because none of their sites is on the
/// caller's thread; on the <b>synchronous</b> surface residuals 1, 2 and 3 fault the send's future,
/// so its <see cref="KafkaFuture{T}.Get"/> throws, while residual 4's site is inside <c>Send</c> and
/// throws out of it, for a record the core had accepted. By <em>which thread the site is on</em>:
/// residual 1 is the send-batch thread (async) or the caller's thread inside <c>Send</c> (sync);
/// residual 2 is the thread tearing the producer down, after the pump thread has exited; residual 3
/// is the completion pump, on both surfaces; residual 4 is the send-batch thread (async) or the
/// caller's thread inside <c>Send</c> (sync).
/// </para>
/// <para>
/// None of the four is "fixed" by firing a fabricated notification, for two distinct reasons.
/// Residual 3's sub-case (a) faults its entry <em>wholesale</em> and keeps no record of which
/// callbacks already fired, so firing there would deliver a <em>duplicate</em> notification for
/// every record whose callback ran before the throw — on the async surface, every index the pump had
/// already reached. A duplicate is worse than a drop — the obligation is exactly-once per record —
/// so the drop is the recorded residual and the duplicate is prevented by construction. Residuals
/// 1, 2, 4 and residual 3's sub-case (b) never read a completion at all, so anything fired there
/// would be an invented <em>failure</em> for a record the core may yet deliver successfully.
/// </para>
/// <para>
/// All four residuals are reachable on both surfaces since M11/P4.2, when the synchronous send
/// moved its completion onto the pump (decisions D3/D4). ⚠ Until then these remarks recorded
/// residuals 1, 2 and 4 as async-surface only and residual 3 as a window the synchronous surface
/// shared, because the synchronous send read its own record's completion inline on the caller's
/// thread. Residuals 1 and 2 match Python, whose <c>close()</c> likewise cancels the pending futures
/// without invoking <c>on_delivery</c>. Wait for your sends — await the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/>, or call <see cref="KafkaFuture{T}.Get"/> —
/// before closing if you need the notification.
/// </para>
/// <para>
/// <b>Throwing is NOT meaningful (decision D4).</b> Java logs and swallows an exception from the
/// user callback (<c>ProducerBatch.java:318-320</c>), and so does Python
/// (<c>producer.py:108-117</c>). This binding does the same: the exception is caught, written to
/// <see cref="System.Diagnostics.Trace"/> so the failure is not silent, and then <b>swallowed</b>.
/// It does not fail the send, it does not surface on the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/> (async) or from the future's
/// <see cref="KafkaFuture{T}.Get"/> (sync), and it does not stop the pump from completing the other
/// records — those in the same completion batch, or queued behind it.
/// </para>
/// <para>
/// <b>Two distinct error surfaces.</b> An exception faulting the delivery
/// <see cref="System.Threading.Tasks.Task{TResult}"/> (async), or thrown by the future's
/// <see cref="KafkaFuture{T}.Get"/> (sync), is the send's own outcome as the caller sees it; the
/// <c>exception</c> delivered here is that same
/// outcome delivered to the callback — one completion driving both, exactly as in Java, where one
/// <c>completeFutureAndFireCallbacks</c> both resolves the future and fires the callbacks. The two
/// surfaces always report the same <em>failure</em>, and for every <see cref="KafkaException"/>
/// outcome — i.e. every ordinary delivery failure — they report the very same <em>object</em>.
/// They differ in exactly one case — <em>not</em> one of the recorded residuals above, since the
/// callback does fire there: on the path where the send succeeded but its metadata
/// could not be marshalled (an <see cref="System.OutOfMemoryException"/> decoding the topic), the
/// awaiter, or the caller of <see cref="KafkaFuture{T}.Get"/>, receives that exception raw, while
/// this parameter is typed
/// <see cref="KafkaException"/> and therefore receives it wrapped — the original survives as the
/// <see cref="System.Exception.InnerException"/>, so the type and <see cref="System.Exception.Message"/>
/// of the two surfaces are not identical there.
/// </para>
/// </remarks>
public interface IDeliveryCallback
{
    /// <summary>
    /// Invoked when the record's send completes (Java
    /// <c>onCompletion(RecordMetadata, Exception)</c>, <c>Callback.java:61</c>) — <b>at most once
    /// per record</b>, never twice, on any path. It is invoked <b>exactly once</b> for every record
    /// whose core-reported completion the binding reads and turns into the send's result: every
    /// normal outcome, success or failure, including a record whose awaiter had already been
    /// canceled. It is <b>not</b> invoked on the paths where the
    /// binding faults a send <em>itself</em> for a reason Java has no callback for — a bounded set
    /// of residuals (teardown paths, plus
    /// unexpected-failure windows before the send reaches the completion pump and on the pump
    /// itself), enumerated exhaustively under <b>Recorded residuals</b> in the remarks on
    /// <see cref="IDeliveryCallback"/>, which is also the one place they are compared with each
    /// other.
    /// </summary>
    /// <param name="metadata">
    /// The published record's metadata on success. <b>Never <see langword="null"/></b>: on failure
    /// this is the Java placeholder — the record's topic and explicit partition (or <c>-1</c>),
    /// with <c>-1</c> for offset and timestamp (<c>Callback.java:28-33</c>).
    /// </param>
    /// <param name="exception">
    /// The send's outcome: <see langword="null"/> on success, mirroring Java's "Null if no error
    /// occurred" (<c>Callback.java:34</c>).
    /// </param>
    void OnCompletion(RecordMetadata metadata, KafkaException? exception);
}
