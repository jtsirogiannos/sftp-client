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
