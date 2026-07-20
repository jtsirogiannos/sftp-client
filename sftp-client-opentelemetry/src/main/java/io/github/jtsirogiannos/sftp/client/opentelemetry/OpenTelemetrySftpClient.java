package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpOperations;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link SftpClient} that emits OpenTelemetry spans.
 *
 * <pre>{@code
 * SftpClient client = OpenTelemetrySftpClient.wrap(
 *         SftpClients.blocking("mina", config), openTelemetry, config);
 * }</pre>
 *
 * <h2>Conformance, stated plainly</h2>
 *
 * <p><strong>There is no OpenTelemetry semantic convention for SFTP or for file transfer.</strong>
 * The connection attributes here are stable and standard — {@code server.address},
 * {@code server.port}, {@code network.protocol.name}, {@code network.protocol.version},
 * {@code error.type}. Everything describing the file itself reuses the {@code file.*} registry,
 * which is experimental and which no span convention currently consumes. See {@link SftpAttributes},
 * where each name is labelled. If a convention appears later, expect the file attributes to move.
 *
 * <h2>Unlike the metrics decorator, composites are spans</h2>
 *
 * <p>{@code sftp-client-micrometer} deliberately does not time convenience methods as units, because
 * a counter should reflect what crossed the wire. Tracing wants the opposite: a
 * {@code withSession} block becomes one {@code sftp.session} span with the individual operations as
 * children, which is the shape that makes a unit of work legible in a trace view. The same data,
 * two different useful questions.
 *
 * <p>Spans are scoped with {@code makeCurrent()}, so anything the caller instruments inside a
 * {@code withSession} block nests correctly beneath it.
 */
public final class OpenTelemetrySftpClient extends ForwardingSftpOperations implements SftpClient {

    /** Instrumentation scope: identifies this library as the emitter, not the application. */
    static final String SCOPE = "io.github.jtsirogiannos.sftp-client";

    private final SftpClient client;
    private final Tracer tracer;
    private final Endpoint endpoint;
    private final SftpOperations traced;

    private final java.util.function.UnaryOperator<String> pathRedactor;

    private OpenTelemetrySftpClient(SftpClient client, Tracer tracer, Endpoint endpoint,
            java.util.function.UnaryOperator<String> pathRedactor) {
        this.client = client;
        this.tracer = tracer;
        this.endpoint = endpoint;
        this.pathRedactor = pathRedactor;
        this.traced = new TracedSftpOperations(client, tracer, endpoint, pathRedactor);
    }

    /**
     * Wraps {@code client}, taking the {@link OpenTelemetry} instance rather than reaching for the
     * global.
     *
     * <p>A library that calls {@code GlobalOpenTelemetry.get()} forces global initialisation on
     * whoever depends on it and cannot be used twice with different configurations. Pass
     * {@code GlobalOpenTelemetry.getOrNoop()} explicitly if that is genuinely what you want — the
     * point is that it is your decision and it is visible at the call site.
     */
    public static SftpClient wrap(SftpClient client, OpenTelemetry openTelemetry,
            SftpConnectionConfig config) {
        return wrap(client, openTelemetry, config, java.util.function.UnaryOperator.identity());
    }

    /**
     * As {@link #wrap(SftpClient, OpenTelemetry, SftpConnectionConfig)}, with every remote path
     * passed through {@code pathRedactor} before it becomes the {@code file.path} span attribute.
     *
     * <p>Span attributes are exported, stored and displayed, and remote filenames routinely embed
     * account numbers, customer names or tokens — the drop-folder use cases this library documents
     * are exactly the ones that name files after business entities. A deployment with that shape
     * passes a redactor ({@code path -> hash(path)}, keep-the-directory-drop-the-name, or
     * {@code path -> "[redacted]"}); everyone else pays nothing.
     */
    public static SftpClient wrap(SftpClient client, OpenTelemetry openTelemetry,
            SftpConnectionConfig config, java.util.function.UnaryOperator<String> pathRedactor) {
        Endpoint endpoint = Endpoint.of(config,
            // Lazily: the negotiated version is a property of a session, and asking the client
            // before one exists would report the provider's ceiling as though it were agreed.
            () -> Optional.ofNullable(client.capabilities())
                .map(c -> Integer.toString(c.maxSftpVersion())));
        return new OpenTelemetrySftpClient(client, openTelemetry.getTracer(SCOPE), endpoint,
            pathRedactor);
    }

    @Override
    protected SftpOperations delegate() {
        return traced;
    }

    @Override
    public <R> R withSession(Function<SftpOperations, R> work) {
        Span span = sessionSpan();
        try (Scope ignored = span.makeCurrent()) {
            return client.withSession(operations ->
                work.apply(new TracedSftpOperations(operations, tracer, endpoint, pathRedactor)));
        } catch (RuntimeException e) {
            span.setStatus(StatusCode.ERROR);
            span.setAttribute(SftpAttributes.ERROR_TYPE, e.getClass().getCanonicalName());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void withSessionWithoutResult(Consumer<SftpOperations> work) {
        withSession(operations -> {
            work.accept(operations);
            return null;
        });
    }

    /** INTERNAL, not CLIENT: the block is work over a connection, not a single remote call. */
    private Span sessionSpan() {
        return tracer.spanBuilder("sftp.session")
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(SftpAttributes.SERVER_ADDRESS, endpoint.host())
            .setAttribute(SftpAttributes.SERVER_PORT, (long) endpoint.port())
            .setAttribute(SftpAttributes.NETWORK_PROTOCOL_NAME, "sftp")
            .startSpan();
    }

    @Override
    public void close() {
        client.close();
    }
}
