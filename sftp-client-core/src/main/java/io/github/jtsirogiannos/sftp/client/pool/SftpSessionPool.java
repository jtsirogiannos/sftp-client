package io.github.jtsirogiannos.sftp.client.pool;

import io.github.jtsirogiannos.sftp.client.spi.SftpSessionLifecycle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Where sessions come from. Lives in the core, never in the API, so a provider author never sees it —
 * the same reason a JDBC driver depends on the JDK and not on HikariCP.
 *
 * <p>Generic in the session type, over {@link SftpSessionLifecycle} — the only part of a session a
 * pool ever touches. That is what lets one implementation serve both the blocking
 * {@link io.github.jtsirogiannos.sftp.client.spi.SftpSession} and the thread-free
 * {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession} without duplicating a line of the
 * validation, LIFO reuse, idle eviction or capacity accounting.
 *
 * <p>The contract is three-way on purpose, and getting it right is what keeps a pool from leaking:
 *
 * <ul>
 *   <li>{@link #borrow()} — take a session; the caller owns it exclusively until it goes back.</li>
 *   <li>{@link #recycle} — the session finished an operation cleanly and can serve another caller.</li>
 *   <li>{@link #discard} — the session is suspect; close it and forget it.</li>
 * </ul>
 *
 * <p><strong>Every borrow must be followed by exactly one recycle or discard.</strong> A session
 * that is neither consumes a slot forever, which is the failure the README flags as needing a proper
 * decision in Phase 1. The current callers are deliberately conservative: any mid-transfer session
 * is discarded rather than recycled — on success, failure, or cancellation alike — because a stream
 * abandoned partway leaves unread bytes in the channel that would corrupt the next caller's read.
 */
public interface SftpSessionPool<S extends SftpSessionLifecycle> extends AutoCloseable {

    /**
     * Takes a session, opening one if none is idle. Blocks if the pool is at capacity.
     *
     * @throws io.github.jtsirogiannos.sftp.client.api.SftpException if a new session cannot be opened
     */
    S borrow();

    /**
     * Takes a session without blocking the calling thread.
     *
     * <p>Exists because {@link #borrow()} is the last blocking call left on the core's async path: a
     * provider can hand out an {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession} that
     * never parks a thread, and then the acquire parks one anyway. This is additive — implementations
     * that have nothing better to offer inherit the default and lose nothing.
     *
     * <p><strong>The returned stage may complete on the calling thread.</strong> An implementation
     * that can satisfy the borrow immediately — a validated idle session is the common case — is
     * expected to, rather than bouncing through {@code executor} for no reason. Callers must
     * therefore not do anything blocking in a dependent action without hopping deliberately: on an
     * event loop, that action may still be running on the event loop.
     *
     * @param executor where to run the parts that genuinely block — opening a new connection, or
     *                 waiting for a slot at capacity. Never used when the borrow can be satisfied
     *                 immediately.
     */
    default CompletionStage<S> borrowAsync(Executor executor) {
        // The honest default: there is nothing non-blocking about this pool, so the blocking call is
        // simply moved off the caller rather than pretended away.
        return CompletableFuture.supplyAsync(this::borrow, executor);
    }

    /**
     * Returns a healthy session. Implementations must validate before reuse — a session that failed
     * {@link SftpSessionLifecycle#isValid()} is closed rather than handed to the next caller.
     */
    void recycle(S session);

    /**
     * Closes and forgets a session that must not be reused. Never throws.
     */
    void discard(S session);

    /**
     * Closes every pooled session. Idempotent, never throws.
     */
    @Override
    void close();
}
