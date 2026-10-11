package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.core.ByteCountingPublisher;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpOperations;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

/**
 * The reactive operations, traced — for a client's per-call traffic and a session's alike. The
 * reactive counterpart of {@link TracedSftpOperations}.
 *
 * <p>Spans parent on the context current when each operation is issued, unless an explicit parent
 * is given: a session's operations name the session's span, since an asynchronous unit of work has
 * no single thread to make it current on.
 */
final class TracedReactiveSftpOperations extends ForwardingReactiveSftpOperations {

    private final ReactiveSftpOperations delegate;
    private final Tracer tracer;
    private final Endpoint endpoint;
    private final java.util.function.UnaryOperator<String> pathRedactor;
    private final Context parent;

    TracedReactiveSftpOperations(ReactiveSftpOperations delegate, Tracer tracer, Endpoint endpoint,
            java.util.function.UnaryOperator<String> pathRedactor, Context parent) {
        this.delegate = delegate;
        this.tracer = tracer;
        this.endpoint = endpoint;
        this.pathRedactor = pathRedactor;
        this.parent = parent;
    }

    @Override
    protected ReactiveSftpOperations delegate() {
        return delegate;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return traced("list", path, () -> delegate.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return traced("stat", path, () -> delegate.stat(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return traced("lstat", path, () -> delegate.lstat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return traced("exists", path, () -> delegate.exists(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return traced("realpath", path, () -> delegate.realpath(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return traced("mkdir", path, () -> delegate.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return traced("rename", from, () -> delegate.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return traced("remove", path, () -> delegate.remove(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return traced("symlink", linkPath, () -> delegate.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return traced("readlink", linkPath, () -> delegate.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return traced("hardlink", linkPath, () -> delegate.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return traced("setAttributes", path, () -> delegate.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return traced("statvfs", path, () -> delegate.statvfs(path));
    }

    /** The span covers the subscription: nothing is transferred until someone subscribes. */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        Flow.Publisher<ByteBuffer> source = delegate.download(remotePath, fromOffset);
        return subscriber -> {
            Span span = span("download", remotePath);
            new ByteCountingPublisher(source, bytes -> {
                span.setAttribute(SftpAttributes.FILE_SIZE, bytes);
                span.end();
            }).subscribe(subscriber);
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
        Span span = span("upload", remotePath);
        // AtomicLong rather than a long[]: nothing in the SPI orders a third-party provider's
        // cancel against its stage completion, and the pattern must not rely on it.
        java.util.concurrent.atomic.AtomicLong sent = new java.util.concurrent.atomic.AtomicLong();
        Flow.Publisher<ByteBuffer> counted = new ByteCountingPublisher(data, sent::set);
        return delegate.upload(remotePath, counted, mode).whenComplete((ignored, failure) -> {
            // Set even on failure: how much landed before it broke is the interesting part.
            span.setAttribute(SftpAttributes.FILE_SIZE, sent.get());
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
            .setParent(parent != null ? parent : Context.current())
            .setSpanKind(SpanKind.CLIENT)
            .setAttribute(SftpAttributes.SERVER_ADDRESS, endpoint.host())
            .setAttribute(SftpAttributes.SERVER_PORT, (long) endpoint.port())
            .setAttribute(SftpAttributes.NETWORK_PROTOCOL_NAME, "sftp")
            // Redacted before it leaves the process: filenames embed business data.
            .setAttribute(SftpAttributes.FILE_PATH, pathRedactor.apply(path));
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
