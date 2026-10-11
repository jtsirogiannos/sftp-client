package io.github.jtsirogiannos.sftp.client.api;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Non-blocking facade expressed entirely in JDK types — {@link CompletionStage} and
 * {@link Flow.Publisher}. No Mutiny, no Reactor, no Vert.x. That is the whole point: this jar must
 * be addable to a Spring, Quarkus, or plain-Java build without dragging a reactive runtime along.
 *
 * <p>Framework modules adapt at the edge:
 * <ul>
 *   <li>Quarkus/Mutiny — Mutiny 2 is natively {@code Flow}-based, so
 *       {@code Multi.createFrom().publisher(...)} and
 *       {@code Uni.createFrom().completionStage(...)} take these directly. No bridge jar.</li>
 *   <li>Spring/Reactor — {@code Flux.from(FlowAdapters.toPublisher(...))} and
 *       {@code Mono.fromCompletionStage(...)}. One adapter call.</li>
 * </ul>
 *
 * <p>Every operation lives on {@link ReactiveSftpOperations}; this interface adds what belongs to
 * owning a pool.
 */
public interface ReactiveSftpClient extends ReactiveSftpOperations, AutoCloseable {

    /**
     * Runs a unit of work over a single connection, and makes it the current session for this
     * client while the work runs.
     *
     * <pre>{@code
     * client.withSession(ops -> ops.mkdirs("/outbound/2026/10")
     *         .thenCompose(done -> ops.upload("/outbound/2026/10/report.csv", data)));
     * }</pre>
     *
     * <p>The reactive form of {@link SftpClient#withSession}: one connection for the sequence, and a
     * nested {@code withSession} joins rather than borrowing another. The returned stage completes
     * with the work's result once its session has been handed back. Not a transaction.
     *
     * <h4>The current session, and where it can be seen</h4>
     *
     * <p>Calls on <em>this client</em> join the work's session when they can see it as current, and
     * that depends on the {@link #sessionContext()}. On the Vert.x context Quarkus supplies, and
     * through the Reactor and coroutine adapters, the binding follows the work to its completion. On
     * a plain thread-local context it covers only the synchronous part of the work — the code that
     * runs before the function returns its stage — because each continuation runs on whichever
     * thread completed the step before it. The operations handed to the work always use its session,
     * whatever the context; prefer them.
     *
     * <p>{@link #download} never joins: a publisher is subscribed whenever its subscriber chooses,
     * possibly after the work has ended, and it would take the session's transfer slot from the very
     * upload it is usually feeding.
     */
    <R> CompletionStage<R> withSession(
            java.util.function.Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work);

    /**
     * Opens a session the caller manages: one connection, held until the session is closed. Never
     * current, never joined — the reactive {@link SftpClient#openSession()}. Completes once the
     * connection is ready, and fails if none can be had.
     */
    CompletionStage<ReactiveSftpClientSession> openSession();

    /** Where this client looks for, and {@link #withSession} records, the current session. */
    io.github.jtsirogiannos.sftp.client.context.SftpSessionContext sessionContext();

    /**
     * Closes the pool and every idle session in it. Idempotent, and never throws — note the narrowed
     * signature, so {@code try}-with-resources needs no catch.
     *
     * <p>This exists for the same reason {@link SftpClient#close()} does: the client owns a pool of
     * live connections. Without it a container that builds a reactive client per configured endpoint
     * has no way to release them at shutdown.
     *
     * <p>Does not shut down the {@link java.util.concurrent.Executor} the client was given — that
     * belongs to whoever created it, and is very often shared.
     */
    @Override
    void close();
}
