package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpOperations;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * One CLIENT span per operation that crosses the wire.
 *
 * <p>Span names are the operation alone — {@code sftp.stat}, {@code sftp.upload} — never the path.
 * The trace API spec is explicit that a name identifies a class of spans rather than an instance,
 * and remote paths are unbounded cardinality. The path goes in an attribute.
 */
final class TracedSftpOperations extends ForwardingSftpOperations {

    private final SftpOperations delegate;
    private final Tracer tracer;
    private final Endpoint endpoint;
    private final java.util.function.UnaryOperator<String> pathRedactor;

    TracedSftpOperations(SftpOperations delegate, Tracer tracer, Endpoint endpoint,
            java.util.function.UnaryOperator<String> pathRedactor) {
        this.delegate = delegate;
        this.tracer = tracer;
        this.endpoint = endpoint;
        this.pathRedactor = pathRedactor;
    }

    @Override
    protected SftpOperations delegate() {
        return delegate;
    }

    @Override
    public List<RemoteFile> list(String path) {
        return traced("list", path, () -> delegate.list(path));
    }

    @Override
    public RemoteFile stat(String path) {
        return traced("stat", path, () -> delegate.stat(path));
    }

    @Override
    public RemoteFile lstat(String path) {
        return traced("lstat", path, () -> delegate.lstat(path));
    }

    @Override
    public boolean exists(String path) {
        return traced("exists", path, () -> delegate.exists(path));
    }

    @Override
    public String realpath(String path) {
        return traced("realpath", path, () -> delegate.realpath(path));
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        traced("symlink", linkPath, () -> {
            delegate.symlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        return traced("readlink", linkPath, () -> delegate.readlink(linkPath));
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        traced("hardlink", linkPath, () -> {
            delegate.hardlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public void setAttributes(String path, AttributeUpdate update) {
        traced("setAttributes", path, () -> {
            delegate.setAttributes(path, update);
            return null;
        });
    }

    @Override
    public void mkdir(String path) {
        traced("mkdir", path, () -> {
            delegate.mkdir(path);
            return null;
        });
    }

    @Override
    public void rename(String from, String to) {
        traced("rename", from, () -> {
            delegate.rename(from, to);
            return null;
        });
    }

    @Override
    public void remove(String path) {
        traced("remove", path, () -> {
            delegate.remove(path);
            return null;
        });
    }

    @Override
    public long download(String remotePath, OutputStream target) {
        return download(remotePath, target, 0);
    }

    @Override
    public long download(String remotePath, OutputStream target, long fromOffset) {
        Span span = span("download", remotePath);
        try (Scope ignored = span.makeCurrent()) {
            long bytes = delegate.download(remotePath, target, fromOffset);
            span.setAttribute(SftpAttributes.FILE_SIZE, bytes);
            return bytes;
        } catch (RuntimeException e) {
            fail(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        upload(remotePath, data, SftpSession.WriteMode.OVERWRITE);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        Span span = span("upload", remotePath);
        CountingInputStream counted = new CountingInputStream(data);
        try (Scope ignored = span.makeCurrent()) {
            delegate.upload(remotePath, counted, mode);
        } catch (RuntimeException e) {
            fail(span, e);
            throw e;
        } finally {
            // Set even on failure: how much landed before it broke is the interesting part.
            span.setAttribute(SftpAttributes.FILE_SIZE, counted.count());
            span.end();
        }
    }

    /**
     * Not traced.
     *
     * <p>This hands back a stream the caller reads afterwards, so a span here would close when the
     * file was opened and report a duration that has nothing to do with the transfer. A span that
     * measures the wrong thing is worse than no span. The two-argument forms own the whole transfer
     * and are traced.
     */
    @Override
    public InputStream download(String remotePath) {
        return delegate.download(remotePath);
    }

    private <T> T traced(String operation, String path, Supplier<T> work) {
        Span span = span(operation, path);
        try (Scope ignored = span.makeCurrent()) {
            return work.get();
        } catch (RuntimeException e) {
            fail(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    private Span span(String operation, String path) {
        SpanBuilder builder = tracer.spanBuilder("sftp." + operation)
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
     * Records the failure once, at the point it escapes to the caller.
     *
     * <p>The recording-errors guidance is explicit that the same exception should not be recorded
     * twice and that exceptions the library handles internally should not be recorded at all. This
     * decorator only ever sees what the client failed to handle, so recording here is the one place
     * it belongs.
     */
    private static void fail(Span span, RuntimeException e) {
        span.setStatus(StatusCode.ERROR);
        span.setAttribute(SftpAttributes.ERROR_TYPE, e.getClass().getCanonicalName());
        span.recordException(e);
    }
}
