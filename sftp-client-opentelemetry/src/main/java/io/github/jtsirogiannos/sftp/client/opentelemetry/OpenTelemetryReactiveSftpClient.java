package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpOperations;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * The reactive counterpart to {@link OpenTelemetrySftpClient}, emitting the same spans.
 *
 * <p><strong>Spans end on completion, not on return.</strong> A reactive operation hands back a
 * {@link CompletionStage} immediately, so ending the span where the method returns would record the
 * time taken to build a stage — every span a few microseconds long, and none of them describing the
 * operation. Spans are ended in a completion callback, and a download's span spans its subscription.
 *
 * <h2>Context propagation, and its limits</h2>
 *
 * <p>The span is captured as the parent {@link Context} when the operation is issued, so anything
 * the caller instruments in a dependent stage nests correctly. What this decorator deliberately does
 * <em>not</em> do is call {@code makeCurrent()} around an asynchronous call: the callback runs on
 * whatever thread the provider completes it on, so a scope opened here would be closed on a
 * different thread from the one it was opened on, which is exactly how context leaks between
 * unrelated requests.
 *
 * <h2>Units of work</h2>
 *
 * <p>A {@code withSession} block becomes one {@code sftp.session} span, from the call until the
 * work's stage completes, and a session from {@code openSession} one from open to close; the
 * session's operations are its children. Neither span is made current, for the reason above: their
 * children name them as parent explicitly.
 *
 * <p>See {@link SftpAttributes} for which attributes are standard and which are invented — there is
 * no semantic convention for file transfer.
 */
public final class OpenTelemetryReactiveSftpClient extends ForwardingReactiveSftpOperations
        implements ReactiveSftpClient {

    private final ReactiveSftpClient client;
    private final Tracer tracer;
    private final Endpoint endpoint;
    private final java.util.function.UnaryOperator<String> pathRedactor;
    private final ReactiveSftpOperations traced;

    private OpenTelemetryReactiveSftpClient(ReactiveSftpClient client, Tracer tracer,
            Endpoint endpoint, java.util.function.UnaryOperator<String> pathRedactor) {
        this.client = client;
        this.tracer = tracer;
        this.endpoint = endpoint;
        this.pathRedactor = pathRedactor;
        this.traced = new TracedReactiveSftpOperations(client, tracer, endpoint, pathRedactor, null);
    }

    /** Wraps {@code client}, taking the {@link OpenTelemetry} instance rather than the global. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, OpenTelemetry openTelemetry,
            SftpConnectionConfig config) {
        return wrap(client, openTelemetry, config, java.util.function.UnaryOperator.identity());
    }

    /**
     * As {@link #wrap(ReactiveSftpClient, OpenTelemetry, SftpConnectionConfig)}, with every remote
     * path passed through {@code pathRedactor} before it becomes the {@code file.path} attribute —
     * see the blocking client's documentation for why a deployment would want that.
     */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, OpenTelemetry openTelemetry,
            SftpConnectionConfig config, java.util.function.UnaryOperator<String> pathRedactor) {
        Endpoint endpoint = Endpoint.of(config,
            () -> Optional.ofNullable(client.capabilities())
                .map(c -> Integer.toString(c.maxSftpVersion())));
        return new OpenTelemetryReactiveSftpClient(client,
            openTelemetry.getTracer(OpenTelemetrySftpClient.SCOPE), endpoint, pathRedactor);
    }

    @Override
    protected ReactiveSftpOperations delegate() {
        return traced;
    }

    @Override
    public <R> CompletionStage<R> withSession(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
        Span span = sessionSpan();
        Context parent = Context.current().with(span);
        CompletionStage<R> outcome;
        try {
            outcome = client.withSession(operations -> work.apply(
                new TracedReactiveSftpOperations(operations, tracer, endpoint, pathRedactor, parent)));
        } catch (RuntimeException e) {
            outcome = CompletableFuture.failedFuture(e);
        }
        return outcome.whenComplete((value, failure) -> endSession(span, failure));
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        Span span = sessionSpan();
        Context parent = Context.current().with(span);
        return client.openSession()
            .whenComplete((session, failure) -> {
                if (failure != null) {
                    endSession(span, failure);
                }
            })
            .thenApply(session -> {
                ReactiveSftpOperations operations =
                    new TracedReactiveSftpOperations(session, tracer, endpoint, pathRedactor, parent);
                return new ForwardingReactiveSftpClientSession() {
                    @Override
                    protected ReactiveSftpOperations delegate() {
                        return operations;
                    }

                    @Override
                    protected ReactiveSftpClientSession session() {
                        return session;
                    }

                    @Override
                    public CompletionStage<Void> close() {
                        boolean wasOpen = session.isOpen();
                        CompletionStage<Void> closed = super.close();
                        return wasOpen ? closed.whenComplete((v, e) -> span.end()) : closed;
                    }
                };
            });
    }

    /** INTERNAL, not CLIENT: a unit of work over a connection, not a single remote call. */
    private Span sessionSpan() {
        return tracer.spanBuilder("sftp.session")
            .setParent(Context.current())
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(SftpAttributes.SERVER_ADDRESS, endpoint.host())
            .setAttribute(SftpAttributes.SERVER_PORT, (long) endpoint.port())
            .setAttribute(SftpAttributes.NETWORK_PROTOCOL_NAME, "sftp")
            .startSpan();
    }

    private static void endSession(Span span, Throwable failure) {
        if (failure != null) {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause() : failure;
            span.setStatus(StatusCode.ERROR);
            span.setAttribute(SftpAttributes.ERROR_TYPE, cause.getClass().getCanonicalName());
            span.recordException(cause);
        }
        span.end();
    }

    /** The wrapped client's: a decorator observes sessions, it does not decide where they live. */
    @Override
    public SftpSessionContext sessionContext() {
        return client.sessionContext();
    }

    @Override
    public void close() {
        client.close();
    }
}
