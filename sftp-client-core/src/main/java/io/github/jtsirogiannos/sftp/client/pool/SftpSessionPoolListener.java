package io.github.jtsirogiannos.sftp.client.pool;

import java.time.Duration;

/**
 * Events a pool reports as they happen, for whoever is watching — a metrics binder, a log, a test.
 *
 * <p>The pool already exposes <em>state</em>: {@link BoundedSftpSessionPool#liveSessions()} and
 * {@link BoundedSftpSessionPool#idleSessions()} answer "how many, right now" and are what gauges
 * read. This interface exists because the interesting failures are <em>events</em> that state can
 * never show. A pool can look permanently healthy — live at 3, idle at 3 — while every tenth borrow
 * waits two seconds for a slot, every parked session dies before reuse, and sessions are discarded
 * and reopened in a churn that gauges sample right past.
 *
 * <p>Every method has a no-op default, so a listener implements only what it cares about. Callbacks
 * are invoked <strong>outside the pool's lock</strong> but on the borrowing or returning thread, so
 * implementations must be quick and must not throw — a listener that blocks slows every caller, and
 * one that throws turns bookkeeping into an operation failure. Incrementing a counter or recording
 * a timer qualifies; I/O does not.
 */
public interface SftpSessionPoolListener {

    /**
     * A borrow found the pool at capacity and waited for a slot.
     *
     * <p>Reported once per borrow, with the total time spent waiting, whether or not a slot was
     * eventually obtained. Frequent non-trivial waits are the early warning that the pool is
     * undersized for the load — the signal that precedes {@link #borrowTimedOut}.
     */
    default void borrowWaited(Duration waited) {
    }

    /**
     * A borrow gave up: the acquire timeout elapsed with every session still in use.
     *
     * <p>This is the pool's terminal failure, the one that surfaces to callers as an exception.
     * {@link #borrowWaited} fires alongside it with the time spent.
     */
    default void borrowTimedOut() {
    }

    /**
     * A session was handed back as suspect and closed instead of parked.
     *
     * <p>Discards are the pool's churn: each one means a future borrow pays a full reconnect. A
     * discard rate near the operation rate usually means transfers are being abandoned mid-stream —
     * every mid-transfer session is discarded by design — or a caller is discarding what it should
     * recycle.
     */
    default void sessionDiscarded() {
    }

    /**
     * A parked session sat idle past the limit and was closed.
     *
     * <p>Evictions are healthy in themselves — that is the idle limit doing its job after a burst —
     * but a steady eviction rate alongside a steady open rate means the pool oscillates: sized for a
     * peak, draining between peaks, reconnecting for each one.
     */
    default void sessionEvicted() {
    }

    /**
     * A parked session failed {@code isValid()} on the way out and was replaced.
     *
     * <p>Distinct from {@link #sessionDiscarded}: nobody chose this. The server or the network ended
     * the session while it sat parked. A cluster of these after quiet periods usually points at a
     * server-side idle timeout shorter than the pool's — worth aligning {@code maxIdle} to it.
     */
    default void sessionValidationFailed() {
    }
}
