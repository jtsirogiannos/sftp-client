package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.core.ByteCountingPublisher;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpClient;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

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
 * <p>See {@link SftpAttributes} for which attributes are standard and which are invented — there is
 * no semantic convention for file transfer.
 */
public final class OpenTelemetryReactiveSftpClient extends ForwardingReactiveSftpClient {

    private final ReactiveSftpClient client;
    private final Tracer tracer;
    private final Endpoint endpoint;

    private OpenTelemetryReactiveSftpClient(ReactiveSftpClient client, Tracer tracer,
            Endpoint endpoint) {
        this.client = client;
        this.tracer = tracer;
        this.endpoint = endpoint;
    }

    /** Wraps {@code client}, taking the {@link OpenTelemetry} instance rather than the global. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, OpenTelemetry openTelemetry,
            SftpConnectionConfig config) {
        Endpoint endpoint = Endpoint.of(config,
            () -> Optional.ofNullable(client.capabilities())
                .map(c -> Integer.toString(c.maxSftpVersion())));
        return new OpenTelemetryReactiveSftpClient(client,
            openTelemetry.getTracer(OpenTelemetrySftpClient.SCOPE), endpoint);
    }

    @Override
    protected ReactiveSftpClient delegate() {
        return client;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return traced("list", path, () -> client.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return traced("stat", path, () -> client.stat(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return traced("lstat", path, () -> client.lstat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return traced("exists", path, () -> client.exists(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return traced("realpath", path, () -> client.realpath(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return traced("mkdir", path, () -> client.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return traced("rename", from, () -> client.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return traced("remove", path, () -> client.remove(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return traced("symlink", linkPath, () -> client.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return traced("readlink", linkPath, () -> client.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return traced("hardlink", linkPath, () -> client.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return traced("setAttributes", path, () -> client.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return traced("statvfs", path, () -> client.statvfs(path));
    }

    /** The span covers the subscription: nothing is transferred until someone subscribes. */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath) {
        Flow.Publisher<ByteBuffer> source = client.download(remotePath);
        return subscriber -> {
            Span span = span("download", remotePath);
            new ByteCountingPublisher(source, bytes -> {
                span.setAttribute(SftpAttributes.FILE_SIZE, bytes);
                span.end();
            }).subscribe(subscriber);
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data) {
        Span span = span("upload", remotePath);
        long[] sent = {0};
        Flow.Publisher<ByteBuffer> counted =
            new ByteCountingPublisher(data, bytes -> sent[0] = bytes);
        return client.upload(remotePath, counted).whenComplete((ignored, failure) -> {
            // Set even on failure: how much landed before it broke is the interesting part.
            span.setAttribute(SftpAttributes.FILE_SIZE, sent[0]);
            finish(span, failure);
        });
    }

    private <T> CompletionStage<T> traced(String operation, String path,
            Supplier<CompletionStage<T>> work) {
        Span span = span(operation, path);
        return work.get().whenComplete((value, failure) -> finish(span, failure));
    }

    private Span span(String operation, String path) {
        SpanBuilder builder = tracer.spanBuilder("sftp." + operation)
            .setParent(Context.current())
            .setSpanKind(SpanKind.CLIENT)
            .setAttribute(SftpAttributes.SERVER_ADDRESS, endpoint.host())
            .setAttribute(SftpAttributes.SERVER_PORT, (long) endpoint.port())
            .setAttribute(SftpAttributes.NETWORK_PROTOCOL_NAME, "sftp")
            .setAttribute(SftpAttributes.FILE_PATH, path);
        endpoint.protocolVersion()
            .ifPresent(v -> builder.setAttribute(SftpAttributes.NETWORK_PROTOCOL_VERSION, v));
        return builder.startSpan();
    }

    /**
     * Ends the span, unwrapping the {@link CompletionException} the JDK adds around a failure that
     * arrived through a dependent stage — otherwise every {@code error.type} would read
     * {@code java.util.concurrent.CompletionException} and say nothing about what went wrong.
     */
    private static void finish(Span span, Throwable failure) {
        if (failure != null) {
            Throwable cause = failure instanceof CompletionException && failure.getCause() != null
                ? failure.getCause()
                : failure;
            span.setStatus(StatusCode.ERROR);
            span.setAttribute(SftpAttributes.ERROR_TYPE, cause.getClass().getCanonicalName());
            span.recordException(cause);
        }
        span.end();
    }
}
