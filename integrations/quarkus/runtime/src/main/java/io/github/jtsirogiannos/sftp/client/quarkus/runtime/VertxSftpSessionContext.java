package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import io.github.jtsirogiannos.sftp.client.context.SessionBindings;
import io.github.jtsirogiannos.sftp.client.context.SessionKey;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.ThreadLocalSftpSessionContext;

import io.smallrye.common.vertx.ContextLocals;
import io.smallrye.common.vertx.VertxContext;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * The current SFTP sessions, kept on the Vert.x duplicated context — the context Quarkus gives
 * every request, and the one Hibernate Reactive keeps its current session on.
 *
 * <p>A duplicated context follows a request across the event loop and the worker threads it
 * dispatches to, and a Mutiny pipeline that emits back on it stays on it. That is what makes it the
 * right home for a unit of work's session in Quarkus: unlike a thread local, it is still there after
 * the pipeline has hopped threads, so a call deep in the pipeline joins the session the work opened.
 *
 * <p>Off a duplicated context — a plain thread, a test, a {@code main} — it falls back to the
 * thread-local context, so blocking code outside a request behaves exactly as it does elsewhere.
 *
 * <h2>Binding until completion</h2>
 *
 * <p>{@link #composeWithin} keeps its binding until the work's stage completes, not merely while
 * the work is assembled: the point of the context is that continuations see it. It ends the binding
 * by key rather than by restoring a snapshot, so a second unit of work for another endpoint, begun
 * on the same request in the meantime, keeps its own.
 */
public final class VertxSftpSessionContext implements SftpSessionContext {

    private static final String KEY = "io.github.jtsirogiannos.sftp.client.sessions";

    private static final VertxSftpSessionContext INSTANCE = new VertxSftpSessionContext();

    private final SftpSessionContext fallback = ThreadLocalSftpSessionContext.instance();

    private VertxSftpSessionContext() {
    }

    public static VertxSftpSessionContext instance() {
        return INSTANCE;
    }

    @Override
    public SessionBindings capture() {
        if (VertxContext.isOnDuplicatedContext()) {
            return ContextLocals.get(KEY, SessionBindings.empty());
        }
        return fallback.capture();
    }

    @Override
    public <R> R callWith(SessionBindings bindings, Supplier<R> work) {
        Objects.requireNonNull(bindings, "bindings");
        if (!VertxContext.isOnDuplicatedContext()) {
            return fallback.callWith(bindings, work);
        }
        SessionBindings previous = ContextLocals.get(KEY, SessionBindings.empty());
        ContextLocals.put(KEY, bindings);
        try {
            return work.get();
        } finally {
            ContextLocals.put(KEY, previous);
        }
    }

    @Override
    public <S, R> CompletionStage<R> composeWithin(SessionKey<S> key, S session,
            Supplier<? extends CompletionStage<R>> work) {
        if (!VertxContext.isOnDuplicatedContext()) {
            return fallback.composeWithin(key, session, work);
        }
        io.vertx.core.Context requestContext = io.vertx.core.Vertx.currentContext();
        ContextLocals.put(KEY, capture().with(key, session));
        CompletionStage<R> stage;
        try {
            stage = work.get();
        } catch (RuntimeException e) {
            end(requestContext, key, session);
            throw e;
        }
        return stage.whenComplete((value, failure) -> end(requestContext, key, session));
    }

    /**
     * Ends one binding on the request's context — from whatever thread the work completed on, which
     * is why the context is named rather than taken as current.
     */
    private static <S> void end(io.vertx.core.Context requestContext, SessionKey<S> key, S session) {
        SessionBindings current = requestContext.getLocal(KEY);
        if (current != null && current.get(key).filter(bound -> bound == session).isPresent()) {
            requestContext.putLocal(KEY, current.without(key));
        }
    }

    @Override
    public String toString() {
        return "VertxSftpSessionContext";
    }
}
