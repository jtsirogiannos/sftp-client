package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.smallrye.mutiny.Uni;

/**
 * Mutiny-flavoured view of the neutral {@link ReactiveSftpClient}.
 *
 * <p>This class is the entire Quarkus reactive integration, and its thinness is the proof that
 * Phase 0 was done right. Mutiny 2 is natively {@code java.util.concurrent.Flow}-based, so no
 * adapter jar is needed — {@code Multi.createFrom().publisher(...)} takes the core's publisher
 * directly, and the only real work is converting {@link java.nio.ByteBuffer} to Vert.x
 * {@link io.vertx.core.buffer.Buffer}
 * at the edge.
 *
 * <h2>Units of work</h2>
 *
 * <p>{@link #withSession} runs a unit of work over one connection, as Hibernate Reactive Panache's
 * {@code @WithSession} does for a database session: the session is kept on the request's Vert.x
 * duplicated context until the work's {@code Uni} completes, and every operation emits back on that
 * context, so a call on this client anywhere in the pipeline joins it.
 *
 * <p>If this file ever grows past trivial conversion, framework concepts have leaked into the core.
 * Growing <em>wider</em> is expected, though: every operation the neutral client gains must appear
 * here as another one-line delegation, because this bean is the only reactive surface a Quarkus
 * application sees. {@link #unwrap()} is the safety valve for the gap between those catch-ups.
 */
public class MutinySftpClient extends MutinySftpOperations implements AutoCloseable {

    private final ReactiveSftpClient delegate;

    public MutinySftpClient(ReactiveSftpClient delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    /** For CDI's client proxy only; see {@link MutinySftpOperations#MutinySftpOperations()}. */
    protected MutinySftpClient() {
        this.delegate = null;
    }

    /**
     * The neutral client underneath, exposed because this bean is the <em>only</em> reactive bean
     * Quarkus registers: without an escape hatch, anything this wrapper lags behind on is
     * unreachable in a Quarkus application. It once lagged by thirteen operations.
     */
    public ReactiveSftpClient unwrap() {
        return delegate;
    }

    /**
     * Runs a unit of work over one connection, with that session current until the work's
     * {@code Uni} completes.
     *
     * <pre>{@code
     * client.withSession(ops -> ops.mkdirs("/outbound/2026/10")
     *         .chain(() -> ops.upload("/outbound/2026/10/report.csv", data))
     *         .chain(() -> archiver.archive()));   // calls on the client inside join this session
     * }</pre>
     *
     * <p>A nested {@code withSession} joins. The work's own failure propagates unchanged; the
     * connection goes back to the pool if it is still sound. Cancelling the returned {@code Uni}
     * cancels the work, and then the connection is handed back.
     */
    public <R> Uni<R> withSession(Function<MutinySftpOperations, Uni<R>> work) {
        return Uni.createFrom().deferred(() -> {
            AtomicReference<CompletableFuture<R>> running = new AtomicReference<>();
            Uni<R> outcome = call(() -> delegate.<R>withSession(operations -> {
                CompletableFuture<R> future = work.apply(new MutinySftpOperations(operations))
                        .subscribeAsCompletionStage();
                running.set(future);
                return future;
            }));
            return outcome.onCancellation().invoke(() -> {
                CompletableFuture<R> future = running.get();
                if (future != null) {
                    future.cancel(true); // cancels the work's subscription
                }
            });
        });
    }

    /** Opens a session the caller manages; never current, never joined. */
    public Uni<MutinySftpSession> openSession() {
        return call(delegate::openSession).map(MutinySftpSession::new);
    }

    /** Where the underlying client looks for its current session. */
    public SftpSessionContext sessionContext() {
        return delegate.sessionContext();
    }

    /**
     * Closes the underlying client and its connection pool.
     *
     * <p>This wrapper owns the delegate it was handed, so the container disposing this bean is what
     * releases the endpoint's connections at shutdown.
     */
    @Override
    public void close() {
        delegate.close();
    }
}
