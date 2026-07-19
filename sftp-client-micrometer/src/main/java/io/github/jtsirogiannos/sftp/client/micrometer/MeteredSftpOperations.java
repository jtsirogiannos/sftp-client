package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.core.ForwardingSftpOperations;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * Times every operation that crosses the wire, and counts the bytes that go with it.
 *
 * <p>Package-private because it is the same instrumentation whether it wraps a whole client or the
 * session-bound view handed to a {@code withSession} block. {@link MicrometerSftpClient} uses it for
 * both, which is what stops a unit of work from being a blind spot.
 */
final class MeteredSftpOperations extends ForwardingSftpOperations {

    /** Timer for each operation, tagged by name and outcome. */
    static final String OPERATIONS = "sftp.client.operations";
    /** Bytes moved, tagged by direction. Counted from what the operation reports, not what was asked for. */
    static final String TRANSFERRED = "sftp.client.transferred";

    private final SftpOperations delegate;
    private final MeterRegistry registry;
    private final Tags tags;

    MeteredSftpOperations(SftpOperations delegate, MeterRegistry registry, Tags tags) {
        this.delegate = delegate;
        this.registry = registry;
        this.tags = tags;
    }

    @Override
    protected SftpOperations delegate() {
        return delegate;
    }

    @Override
    public List<RemoteFile> list(String path) {
        return timed("list", () -> delegate.list(path));
    }

    @Override
    public RemoteFile stat(String path) {
        return timed("stat", () -> delegate.stat(path));
    }

    @Override
    public RemoteFile lstat(String path) {
        return timed("lstat", () -> delegate.lstat(path));
    }

    @Override
    public boolean exists(String path) {
        return timed("exists", () -> delegate.exists(path));
    }

    @Override
    public String realpath(String path) {
        return timed("realpath", () -> delegate.realpath(path));
    }

    @Override
    public void symlink(String linkPath, String targetPath) {
        timed("symlink", () -> {
            delegate.symlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public String readlink(String linkPath) {
        return timed("readlink", () -> delegate.readlink(linkPath));
    }

    @Override
    public void hardlink(String linkPath, String targetPath) {
        timed("hardlink", () -> {
            delegate.hardlink(linkPath, targetPath);
            return null;
        });
    }

    @Override
    public void setAttributes(String path,
            io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
        timed("setAttributes", () -> {
            delegate.setAttributes(path, update);
            return null;
        });
    }

    @Override
    public void mkdir(String path) {
        timed("mkdir", () -> {
            delegate.mkdir(path);
            return null;
        });
    }

    @Override
    public void rename(String from, String to) {
        timed("rename", () -> {
            delegate.rename(from, to);
            return null;
        });
    }

    @Override
    public void remove(String path) {
        timed("remove", () -> {
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
        long bytes = timed("download", () -> delegate.download(remotePath, target, fromOffset));
        countBytes("download", bytes);
        return bytes;
    }

    @Override
    public void upload(String remotePath, InputStream data) {
        upload(remotePath, data, SftpSession.WriteMode.OVERWRITE);
    }

    @Override
    public void upload(String remotePath, InputStream data, SftpSession.WriteMode mode) {
        CountingInputStream counted = new CountingInputStream(data);
        timed("upload", () -> {
            delegate.upload(remotePath, counted, mode);
            return null;
        });
        countBytes("upload", counted.count());
    }

    /**
     * Not timed, and not counted.
     *
     * <p>{@code download(String)} hands back a stream the caller reads later, so there is no
     * duration to measure here — the call returns as soon as the file is open. Timing it would
     * report how long it took to open a file and label it a transfer, which is worse than reporting
     * nothing. The two-argument forms above are the ones that own the whole transfer, and those are
     * measured.
     */
    @Override
    public InputStream download(String remotePath) {
        return delegate.download(remotePath);
    }

    private <T> T timed(String operation, Supplier<T> work) {
        Timer.Sample sample = Timer.start(registry);
        String outcome = "success";
        try {
            return work.get();
        } catch (RuntimeException e) {
            outcome = "failure";
            throw e;
        } finally {
            sample.stop(registry.timer(OPERATIONS, tags.and("operation", operation, "outcome", outcome)));
        }
    }

    private void countBytes(String direction, long bytes) {
        Counter.builder(TRANSFERRED)
            .tags(tags.and("direction", direction))
            .baseUnit("bytes")
            .register(registry)
            .increment(bytes);
    }
}
