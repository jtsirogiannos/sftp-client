package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.core.ByteCountingPublisher;
import io.github.jtsirogiannos.sftp.client.core.ForwardingReactiveSftpClient;
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
 * The reactive counterpart to {@link MicrometerSftpClient}, reporting the same meters.
 *
 * <pre>{@code
 * ReactiveSftpClient client = MicrometerReactiveSftpClient.wrap(
 *         SftpClients.reactive("mina", config, worker), registry, config);
 * }</pre>
 *
 * <p><strong>Every measurement is taken on completion, not on return.</strong> A reactive operation
 * returns a {@link CompletionStage} immediately and finishes later, so stopping a timer where the
 * method returns would time the construction of a stage and report near-zero for every operation —
 * a metric that looks healthy precisely because it measures nothing. Timers stop in a
 * {@code whenComplete} callback, and the transfer counters come from the publisher's terminal
 * signal.
 *
 * <p>There is no {@code withSession} on the reactive facade, so the unit-of-work blind spot that the
 * blocking decorator has to handle does not arise here.
 */
public final class MicrometerReactiveSftpClient extends ForwardingReactiveSftpClient {

    private final ReactiveSftpClient client;
    private final MeterRegistry registry;
    private final Tags tags;

    private MicrometerReactiveSftpClient(ReactiveSftpClient client, MeterRegistry registry,
            Tags tags) {
        this.client = client;
        this.registry = registry;
        this.tags = tags;
    }

    /** Wraps {@code client}, tagging meters with the endpoint. No credential is read. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, MeterRegistry registry,
            SftpConnectionConfig config) {
        return wrap(client, registry, Tags.of(
            "host", config.host(),
            "port", Integer.toString(config.port()),
            "user", config.username()));
    }

    /** Wraps {@code client} with tags of your choosing. */
    public static ReactiveSftpClient wrap(ReactiveSftpClient client, MeterRegistry registry,
            Tags tags) {
        return new MicrometerReactiveSftpClient(client, registry, tags);
    }

    @Override
    protected ReactiveSftpClient delegate() {
        return client;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return timed("list", () -> client.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return timed("stat", () -> client.stat(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return timed("lstat", () -> client.lstat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return timed("exists", () -> client.exists(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return timed("realpath", () -> client.realpath(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return timed("mkdir", () -> client.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return timed("rename", () -> client.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return timed("remove", () -> client.remove(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return timed("symlink", () -> client.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return timed("readlink", () -> client.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return timed("hardlink", () -> client.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return timed("setAttributes", () -> client.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return timed("statvfs", () -> client.statvfs(path));
    }

    /**
     * Timed from subscribe to terminal signal, which is the only interval that means anything:
     * {@code download} returns a publisher immediately, and nothing is transferred until someone
     * subscribes — possibly much later, possibly never.
     */
    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath) {
        Flow.Publisher<ByteBuffer> source = client.download(remotePath);
        return subscriber -> {
            Timer.Sample sample = Timer.start(registry);
            new ByteCountingPublisher(source, bytes -> {
                sample.stop(timer("download", "success"));
                countBytes("download", bytes);
            }).subscribe(subscriber);
        };
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data) {
        long[] sent = {0};
        Flow.Publisher<ByteBuffer> counted =
            new ByteCountingPublisher(data, bytes -> sent[0] = bytes);
        return timed("upload", () -> client.upload(remotePath, counted))
            .whenComplete((ignored, failure) -> countBytes("upload", sent[0]));
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
