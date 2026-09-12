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
/// the overload still returns the <see cref="RecordMetadata"/> /
/// <see cref="System.Threading.Tasks.Task{TResult}"/> the plain overload does.
/// </summary>
/// <remarks>
/// <para>
/// <b>The method is synchronous, not <see cref="System.Threading.Tasks.Task"/>-returning
/// (the §4 delivery-callback divergence, M14/P1 decision D2).</b> Java's
/// <c>Callback.onCompletion</c> returns <c>void</c> (<c>Callback.java:61</c>), and the invocation
/// site here is either the producer's send-completion pump thread or a caller thread blocked
/// inside <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, IDeliveryCallback)"/>
/// — an awaited callback would stall every other completion in the same pump batch. This is the
/// same divergence, for the same reason, as <see cref="IOffsetCommitCallback"/> and
/// <see cref="IConsumerRebalanceListener"/>.
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
/// <item><b>Sync</b> (<see cref="IProducer{TKey, TValue}"/>) — <b>inline on the calling thread</b>,
/// before <c>Send</c> returns or throws. The blocking send has no pump (it waits on the record's
/// future itself), so there is no other thread to run it on. Concurrent <c>Send</c> on one producer
/// is explicitly supported and deliberately unsynchronized (the Rust core's <c>Mutex</c>
/// serializes, and a binding-side send lock is an ffi §A1 anti-pattern), so one
/// <see cref="IDeliveryCallback"/> instance passed to <c>Send</c> from N threads <b>is</b> entered
/// on N threads at once.</item>
/// </list>
/// <b>What this means for you: a callback instance you share across sends must itself be
/// thread-safe</b> — synchronize any mutable state it touches (a per-send instance needs nothing).
/// On the sync surface concurrent <c>Send</c> calls enter it on their own threads. On the
/// <b>async</b> surface an invocation made away from the pump — the send-batch thread's
/// per-record rejection — can overlap one the pump is making. A single instance shared across
/// <em>two</em> producers can also be entered from each producer's own pump. See the §4
/// delivery-callback divergence in the binding's <c>CLAUDE.md</c>. ⚠ These remarks used to give the
/// async surface an unqualified per-producer non-concurrency guarantee, on the reasoning that the
/// pump was the only thread firing them; the send-batch thread's site falsifies it (Critic 72
/// finding 72.10). ⚠ They then justified the obligation by
/// asserting Java runs <em>every</em> <c>Callback</c> on one background I/O thread so a Java user
/// never has to make one thread-safe — also false, and the cited line does not say it:
/// <c>Callback.java:20-21</c> reads "<em>generally</em> execute in the background I/O thread".
/// <c>KafkaProducer.doSend</c>'s <c>catch (ApiException)</c> invokes the callback on the
/// <b>application</b> thread while <c>ProducerBatch.completeFutureAndFireCallbacks</c> fires others
/// on the Sender thread, so one shared <c>Callback</c> can be entered from two threads in Java too
/// (finding 72.17). The obligation above stands on its own; it needs no claim about Java.
/// Do not block on producer progress from inside it while it is running on the pump thread: that is
/// the same thread that must resolve every other in-flight send.
/// </para>
/// <para>
/// <b>Sending again from inside it IS supported</b> — the canonical retry-on-failure shape — because
/// no managed lock is held while the callback runs, wherever it runs. Where it runs on the pump
/// thread the reentrant send is queued and drained by the accumulator as usual; on the sync surface
/// the callback runs after the blocking wait has already returned. Both are covered by a regression
/// test.
/// </para>
/// <para>
/// <b>Tearing the producer down from inside it is NOT supported while it is running on a thread the
/// producer owns</b> — the async surface's send-completion pump or its send-batch thread.
/// <c>Close</c> / <c>Dispose</c> stop and <b>join</b> both, so calling either from there asks a
/// thread to wait for itself. Java guards the equivalent explicitly (its <c>close</c> detects being
/// called from the sender thread and skips the join); this binding does not, and adding such a guard
/// is out of scope here. Close the producer from the thread that owns it instead. Where the callback
/// runs inline on a caller's own thread — the sync surface — that
/// particular self-join does not arise, but the advice is unchanged.
/// </para>
/// <para>
/// <b>Ordering — it runs BEFORE the send's result is observable (decision D3).</b> Java sets the
/// future's value, fires the callbacks, and only then releases the future's waiters
/// (<c>ProducerBatch.java:303-323</c> — <c>produceFuture.done()</c> is last). This binding
/// reproduces that exactly: the callback is invoked immediately before the
/// <see cref="System.Threading.Tasks.Task{TResult}"/> is completed (async) and before <c>Send</c>
/// returns or throws (sync). Note this is <b>stricter</b> than the Python sibling, which resolves
/// its future first and then invokes <c>on_delivery</c> (<c>producer.py:322-327</c>), so a Python
/// awaiter can be released before the callback has run.
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
/// <c>Send</c>; on the async surface it faults the returned
/// <see cref="System.Threading.Tasks.Task{TResult}"/>, because the site is the send-batch
/// thread;</item>
/// <item>the record was accepted and the core later reported success → <b>fires</b> with the real
/// metadata and a <see langword="null"/> exception;</item>
/// <item>the record was accepted and the core later reported a delivery failure → <b>fires</b>
/// with the placeholder metadata and the <see cref="KafkaException"/>, and the send still throws
/// (sync) / the <see cref="System.Threading.Tasks.Task{TResult}"/> still faults (async) with the
/// same error.</item>
/// </list>
/// It also fires for a send whose <see cref="System.Threading.Tasks.Task{TResult}"/> was already
/// canceled through its <see cref="System.Threading.CancellationToken"/> (decision D7): the
/// delivery notification is owed per record, not per awaiter — the same obligation Python states
/// ("invoked exactly once per record, even if the returned <c>Future</c> was cancelled or already
/// resolved", <c>producer.py:301-303</c>).
/// </para>
/// <para>
/// <b>Recorded residuals — the exhaustive set of paths that fault a send, or throw out of
/// <c>Send</c>, WITHOUT notifying you.</b> The callback reports a <em>core</em> completion, so
/// wherever the binding faults a send
/// <em>itself</em> instead of reading one, the notification is dropped: the send's
/// <see cref="System.Threading.Tasks.Task{TResult}"/> faults (or <c>Send</c> throws) and the
/// callback does not fire. The set below is not a remembered list — it is obtained by walking
/// <em>every</em> site between the core's acceptance of the record (a live future and no
/// synchronous error) and the callback's invocation that can fault the send or throw out of
/// <c>Send</c>, through every frame the record's future travels on both threads. There are exactly
/// four such <em>sites</em>, and no others; each carries a numbered note in the code, and residual 3
/// carries one further note on the synchronous send, for the window that surface shares. Note a site
/// is not the same as a <em>condition</em> — residual 3's
/// single site is reached by two conditions, one either side of the completion's arrival, and both
/// are stated under it:
/// <list type="number">
/// <item><b>Teardown raced the enqueue</b> (async only) — the record was handed to native, but by
/// the time the binding went to hand it to the completion pump the pump's gate had closed, so the
/// send is faulted in place.</item>
/// <item><b>Teardown drained a still-queued send</b> (async only) — the producer was closed with
/// that send queued for the pump and not yet resolved; the queue is faulted wholesale, with no
/// blocking read of the core's results.</item>
/// <item><b>An unexpected failure on the completion pump, after the send was handed to it and
/// before the callback was invoked</b> — <em>not</em> a teardown path. It spans <b>both sides of
/// the completion's arrival</b>, because the pump can throw on either side of its batched read and
/// one wholesale-fault site covers both. <b>(a) After</b> the read reported: it reported for the
/// <em>whole</em> batch, so the core <em>did</em> report these completions; the indices the pump had
/// already reached fired normally and the rest are faulted with none. This is the sub-case that
/// makes firing from the fault
/// path unsafe (see below).
/// ⚠ <b>Sub-case (a) NARROWED in M11/P3.2 (§3B, S3) — like (b), it did not vanish.</b> "The whole
/// batch" used to mean whatever the pump's flat per-record queue happened to hold: up to 1100
/// records drawn from arbitrarily many unrelated sends. The pump's unit is now <b>one
/// <c>send_batch</c> call's</b> records, so one such event faults only records that were sent
/// together — a bounded and <em>related</em> blast radius rather than an arbitrary mixture. The
/// batched read itself is unchanged, and the synchronous surface still shares both conditions, so
/// this stays a recorded residual rather than a closed one.
/// <b>(b) Before</b> the read reported: the pump threw out of the batched
/// read itself, so no completion was ever in hand and the whole batch is faulted. This condition
/// does <b>not</b> need an allocation failure to be reachable: a
/// native-side failure surfacing from the pump's first batched read, for example an
/// <see cref="System.EntryPointNotFoundException"/> against a stale or mismatched native library,
/// lands here.
/// ⚠ <b>Sub-case (b) NARROWED in M11/P3.1 (§12.3) — it did not vanish.</b> It used to have two
/// triggers: the batched read itself, and the pump throwing while <em>setting the batch up</em> (the
/// three marshalling arrays, allocated per batch outside the processing <c>try</c>). Those arrays
/// are now reused fields allocated once, so there is no pre-read allocation left to fail and that
/// trigger is gone. The batched read remains, which is why this stays a recorded residual rather
/// than a closed one.
/// <b>This is the residual the synchronous surface shares, and it shares BOTH conditions</b>: that
/// surface has the same narrow window for its own single record, between the blocking
/// <c>get</c> reporting and the callback being invoked — reading that record's reported error can
/// fail for (a)'s reason, and the blocking <c>get</c> is its own separate native entry point, so it
/// can fail for (b)'s reason too. What the synchronous surface does <em>not</em> have is a batch:
/// there is nothing to set up and nothing to fault wholesale, so its throw simply propagates out of
/// <c>Send</c>. That is why this is one residual and not two.</item>
/// <item><b>An unexpected failure between the core accepting the record and the send being handed
/// to the completion pump</b> (async only) — on the <b>send-batch thread</b>, between
/// <c>send_batch</c> returning a live future for that index and the accumulator handing it over.
/// In practice an <see cref="System.OutOfMemoryException"/> (the pump's queue growing), or a
/// P/Invoke failure from a later chunk of the same node.
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
/// The record <em>was</em> accepted (the core
/// returned a live future and no error) and may still be delivered, but the binding destroys that
/// future unread, so no completion is ever read. It is <em>not</em> a teardown path, and no
/// completion had arrived — as in residuals 1 and 2, and as in residual 3's sub-case (b), where the
/// pump's batch read had not reported either; residual 3's sub-case (a) is the <em>only</em> one
/// where a completion had arrived. The synchronous surface has no window of this shape at all: it
/// reads its own record's completion immediately, with nothing in between. (What it does share is
/// residual 3's window — see there.)
/// <para>
/// ⚠ <b>This residual's SITE moved in M11/P3.1 and its shape narrowed with it.</b> While the async
/// send called the core inline, the window sat inside <c>Send</c> itself — so it surfaced as a
/// <b>throw out of <c>Send</c></b> for a record the core had accepted, and it was the reason the D5
/// outcome list could not attribute every no-callback throw to "nothing was sent". Since the send
/// submission became deferred, <c>Send</c> no longer touches the core at all: it pins, appends to
/// the accumulator and returns, so <b>every</b> throw out of the async <c>Send</c> is now a case
/// where nothing reached the core. The window itself did not disappear — it moved onto the batch
/// thread, where it faults the awaiter like residuals 1, 2 and 3 rather than throwing at the caller.
/// A record that the accumulator accepted but that is abandoned <em>before</em> <c>send_batch</c>
/// is deliberately <b>not</b> a residual: the core never saw it, so the binding faults it
/// <em>and</em> fires the callback, which invents nothing and cannot duplicate.
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
/// reaches it too. By <em>what the core reported</em>: a
/// completion had arrived only in residual 3's sub-case (a); residuals 1, 2, 4 and sub-case (b)
/// never read one. By <em>how the send surfaces the failure</em>: on the <b>async</b> surface all
/// four fault the send's <see cref="System.Threading.Tasks.Task{TResult}"/>, because none of their
/// sites is on the caller's thread any more; on the <b>synchronous</b> surface, which has no
/// <c>Task</c>, residual 3's shared window throws out of <c>Send</c> instead. By <em>which thread
/// the site is on</em>: residuals 1 and 4 are the send-batch thread, residuals 2 and 3 the
/// completion pump (and residual 3's shared window, the sync caller's own thread).
/// </para>
/// <para>
/// None of the four is "fixed" by firing a fabricated notification, for two distinct reasons.
/// Residual 3's sub-case (a) faults the batch <em>wholesale</em> on the async surface and cannot
/// tell which indices already fired, so firing there would deliver a <em>duplicate</em>
/// notification for every index that completed before the throw; a duplicate is worse than a drop —
/// the obligation is exactly-once per record — so the drop is the recorded residual and the
/// duplicate is prevented by construction. Residuals 1, 2, 4 and residual 3's sub-case (b) never
/// read a completion at all, so anything fired there would be an invented <em>failure</em> for a
/// record the core may yet deliver successfully.
/// </para>
/// <para>
/// Residuals 1, 2 and 4 are async-surface only: the synchronous send has no accumulator, no queue
/// and no pump, and it reads its own record's completion inline with nothing in between — it still
/// calls the singular <c>Producer_send</c> on the caller's thread, which M11/P3.1 deliberately left
/// unchanged. <b>Residual 3 is
/// the one the synchronous surface shares</b>, in both of its conditions (see there). What is
/// async-only <em>inside</em> residual 3 is the wholesale-fault site itself — the synchronous surface
/// has no batch, so its own throw simply propagates out of <c>Send</c>. (Sub-case (b)'s
/// batch-setup half used to be listed here too; §12.3 removed that trigger.) Residuals 1 and 2 match Python, whose <c>close()</c>
/// likewise cancels the pending futures without invoking <c>on_delivery</c>. Await your sends, or
/// <c>Flush</c>, before closing if you need the notification.
/// </para>
/// <para>
/// <b>Throwing is NOT meaningful (decision D4).</b> Java logs and swallows an exception from the
/// user callback (<c>ProducerBatch.java:318-320</c>), and so does Python
/// (<c>producer.py:108-117</c>). This binding does the same: the exception is caught, written to
/// <see cref="System.Diagnostics.Trace"/> so the failure is not silent, and then <b>swallowed</b>.
/// It does not fail the send, it does not surface on the returned
/// <see cref="System.Threading.Tasks.Task{TResult}"/> or from <c>Send</c>, and it does not stop
/// the other records in the same completion batch from being completed.
/// </para>
/// <para>
/// <b>Two distinct error surfaces.</b> An exception thrown <em>by <c>Send</c> itself</em> (or
/// faulting the returned <see cref="System.Threading.Tasks.Task{TResult}"/>) is the send's own
/// outcome as the caller sees it; the <c>exception</c> delivered here is that same
/// outcome delivered to the callback — one completion driving both, exactly as in Java, where one
/// <c>completeFutureAndFireCallbacks</c> both resolves the future and fires the callbacks. The two
/// surfaces always report the same <em>failure</em>, and for every <see cref="KafkaException"/>
/// outcome — i.e. every ordinary delivery failure — they report the very same <em>object</em>.
/// They differ in exactly one case — <em>not</em> one of the recorded residuals above, since the
/// callback does fire there: on the path where the send succeeded but its metadata
/// could not be marshalled (an <see cref="System.OutOfMemoryException"/> decoding the topic), the
/// awaiter receives that exception raw, while this parameter is typed
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
    /// other. The residual sites are on the async surface; the synchronous surface, having no pump,
    /// shares residual 3's read-then-fire gap for its own single record.
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
