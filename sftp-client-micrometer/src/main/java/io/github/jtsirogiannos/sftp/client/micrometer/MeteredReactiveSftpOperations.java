package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.core.ByteCountingPublisher;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpOperations;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Supplier;

/**
 * The reactive operations, metered — for a client's per-call traffic and a session's alike, which is
 * what lets {@link MicrometerReactiveSftpClient} report a unit of work's operations as well as
 * everything else. The reactive counterpart of {@link MeteredSftpOperations}.
 */
final class MeteredReactiveSftpOperations extends ForwardingReactiveSftpOperations {

    private final ReactiveSftpOperations delegate;
    private final MeterRegistry registry;
    private final Tags tags;

    MeteredReactiveSftpOperations(ReactiveSftpOperations delegate, MeterRegistry registry, Tags tags) {
        this.delegate = delegate;
        this.registry = registry;
        this.tags = tags;
    }

    @Override
    protected ReactiveSftpOperations delegate() {
        return delegate;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return timed("list", () -> delegate.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return timed("stat", () -> delegate.stat(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return timed("lstat", () -> delegate.lstat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return timed("exists", () -> delegate.exists(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return timed("realpath", () -> delegate.realpath(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return timed("mkdir", () -> delegate.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return timed("rename", () -> delegate.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return timed("remove", () -> delegate.remove(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return timed("symlink", () -> delegate.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return timed("readlink", () -> delegate.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return timed("hardlink", () -> delegate.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return timed("setAttributes", () -> delegate.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return timed("statvfs", () -> delegate.statvfs(path));
    }

    /**
     * Timed from subscribe to terminal signal, which is the only interval that means anything:
     * {@code download} returns a publisher immediately, and nothing is transferred until someone
     * subscribes — possibly much later, possibly never.
     */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        Flow.Publisher<ByteBuffer> source = delegate.download(remotePath, fromOffset);
        return subscriber -> {
            Timer.Sample sample = Timer.start(registry);
            new ByteCountingPublisher(source, bytes -> {
                sample.stop(timer("download", "success"));
                countBytes("download", bytes);
            }).subscribe(subscriber);
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode mode) {
        // AtomicLong rather than a long[]: the counting callback and this whenComplete only have a
        // happens-before edge when the provider orders its cancel against its stage completion,
        // and nothing in the SPI requires that of third parties.
        java.util.concurrent.atomic.AtomicLong sent = new java.util.concurrent.atomic.AtomicLong();
        Flow.Publisher<ByteBuffer> counted = new ByteCountingPublisher(data, sent::set);
        return timed("upload", () -> delegate.upload(remotePath, counted, mode))
            .whenComplete((ignored, failure) -> countBytes("upload", sent.get()));
    }

    private <T> CompletionStage<T> timed(String operation, Supplier<CompletionStage<T>> work) {
        Timer.Sample sample = Timer.start(registry);
        return work.get().whenComplete((value, failure) ->
            sample.stop(timer(operation, failure == null ? "success" : "failure")));
    }

    private Timer timer(String operation, String outcome) {
        return registry.timer(MeteredSftpOperations.OPERATIONS,
            tags.and("operation", operation, "outcome", outcome));
    }

    private void countBytes(String direction, long bytes) {
        Counter.builder(MeteredSftpOperations.TRANSFERRED)
            .tags(tags.and("direction", direction))
            .baseUnit("bytes")
            .register(registry)
            .increment(bytes);
    }
}
