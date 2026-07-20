package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Predicate;

/**
 * Reactor view of the neutral {@link ReactiveSftpClient}, for WebFlux applications.
 *
 * <p>Its thinness is the point, and the second proof that the core is genuinely framework-neutral —
 * the Quarkus module makes the same claim with Mutiny. {@code Mono.fromCompletionStage} takes the
 * facade's {@code CompletionStage} directly; the streaming side needs one adapter call, because
 * Reactor consumes {@code org.reactivestreams.Publisher} while the facade speaks
 * {@code java.util.concurrent.Flow.Publisher}. That bridge is {@link FlowAdapters}, and it is the
 * whole of the impedance mismatch.
 *
 * <p><strong>These transfers are not non-blocking I/O, and a WebFlux caller should know it.</strong>
 * A {@code Mono} or {@code Flux} usually promises that no thread is parked waiting on the network.
 * Here one is: every transfer occupies a thread from the client's worker executor for its duration,
 * because neither backend exposes a zero-thread SFTP path — the same reason
 * {@link SftpCapabilities#pipelinedStreaming()} is not called "async".
 *
 * <p>What that means in practice:
 * <ul>
 *   <li>Concurrent transfers are capped by the worker pool (eight threads by default), not by
 *       demand. The ninth concurrent download waits for a thread, and its {@code Mono} simply takes
 *       longer to emit.</li>
 *   <li>Nothing blocks the event loop: the work is already dispatched to that pool, and signals
 *       arrive on it. No {@code publishOn} is applied here — adding one would buy a thread hop and
 *       no safety.</li>
 *   <li>Size the executor for the transfers you expect to overlap, not for request throughput.</li>
 * </ul>
 */
public class ReactorSftpClient implements AutoCloseable {

    private final ReactiveSftpClient delegate;

    public ReactorSftpClient(ReactiveSftpClient delegate) {
        this.delegate = delegate;
    }

    /**
     * The neutral client underneath, exposed because this wrapper is maintained by hand and lags
     * behind whenever the neutral client grows — it once lagged by thirteen operations. The escape
     * hatch keeps that a nuisance rather than a wall.
     */
    public ReactiveSftpClient unwrap() {
        return delegate;
    }

    public Mono<List<RemoteFile>> list(String path) {
        return Mono.fromCompletionStage(() -> delegate.list(path));
    }

    public Mono<RemoteFile> stat(String path) {
        return Mono.fromCompletionStage(() -> delegate.stat(path));
    }

    public Mono<Boolean> exists(String path) {
        return Mono.fromCompletionStage(() -> delegate.exists(path));
    }

    public Mono<Void> mkdir(String path) {
        return Mono.fromCompletionStage(() -> delegate.mkdir(path));
    }

    public Mono<Void> rename(String from, String to) {
        return Mono.fromCompletionStage(() -> delegate.rename(from, to));
    }

    public Mono<Void> remove(String path) {
        return Mono.fromCompletionStage(() -> delegate.remove(path));
    }

    /** Attributes without following symlinks — what a tree walk must use. */
    public Mono<RemoteFile> lstat(String path) {
        return Mono.fromCompletionStage(() -> delegate.lstat(path));
    }

    public Mono<String> realpath(String path) {
        return Mono.fromCompletionStage(() -> delegate.realpath(path));
    }

    public Mono<Void> symlink(String linkPath, String targetPath) {
        return Mono.fromCompletionStage(() -> delegate.symlink(linkPath, targetPath));
    }

    public Mono<String> readlink(String linkPath) {
        return Mono.fromCompletionStage(() -> delegate.readlink(linkPath));
    }

    public Mono<Void> hardlink(String linkPath, String targetPath) {
        return Mono.fromCompletionStage(() -> delegate.hardlink(linkPath, targetPath));
    }

    public Mono<Void> setAttributes(String path, AttributeUpdate update) {
        return Mono.fromCompletionStage(() -> delegate.setAttributes(path, update));
    }

    public Mono<FilesystemStats> statvfs(String path) {
        return Mono.fromCompletionStage(() -> delegate.statvfs(path));
    }

    /** {@code mkdir -p}. An existing chain is not an error. */
    public Mono<Void> mkdirs(String path) {
        return Mono.fromCompletionStage(() -> delegate.mkdirs(path));
    }

    /** Deletes a tree, removing symlinks as links, never following them. */
    public Mono<Integer> deleteRecursively(String path) {
        return Mono.fromCompletionStage(() -> delegate.deleteRecursively(path));
    }

    public Mono<Void> chmod(String path, int mode) {
        return Mono.fromCompletionStage(() -> delegate.chmod(path, mode));
    }

    /** Directory entries matching {@code filter}, filtered client-side. */
    public Mono<List<RemoteFile>> list(String path, Predicate<RemoteFile> filter) {
        return Mono.fromCompletionStage(() -> delegate.list(path, filter));
    }

    /** The first entry matching {@code match}; empty when nothing does. */
    public Mono<RemoteFile> findFirst(String path, Predicate<RemoteFile> match) {
        return Mono.fromCompletionStage(() -> delegate.findFirst(path, match))
                .flatMap(found -> Mono.justOrEmpty(found));
    }

    /** Backpressured chunks. Cancelling the subscription abandons the transfer and its session. */
    public Flux<ByteBuffer> download(String remotePath) {
        return Flux.from(FlowAdapters.toPublisher(delegate.download(remotePath)));
    }

    /** As {@link #download(String)}, starting at {@code fromOffset} — the resume primitive. */
    public Flux<ByteBuffer> download(String remotePath, long fromOffset) {
        return Flux.from(FlowAdapters.toPublisher(delegate.download(remotePath, fromOffset)));
    }

    /** Consumes {@code data} with backpressure; completes when the upload has landed. */
    public Mono<Void> upload(String remotePath, Publisher<ByteBuffer> data) {
        return Mono.fromCompletionStage(
                () -> delegate.upload(remotePath, FlowAdapters.toFlowPublisher(data)));
    }

    /** As {@link #upload(String, Publisher)}, with explicit write semantics. */
    public Mono<Void> upload(String remotePath, Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        return Mono.fromCompletionStage(
                () -> delegate.upload(remotePath, FlowAdapters.toFlowPublisher(data), mode));
    }

    /**
     * Publishes by rename, so a reader watching the destination never sees a partial file.
     * Replacing an existing file needs SFTP v5+ rename semantics and fails on a v3 server rather
     * than degrading.
     */
    public Mono<Void> uploadAtomically(String remotePath, Publisher<ByteBuffer> data) {
        return Mono.fromCompletionStage(
                () -> delegate.uploadAtomically(remotePath, FlowAdapters.toFlowPublisher(data)));
    }

    public SftpCapabilities capabilities() {
        return delegate.capabilities();
    }

    /**
     * Closes the underlying client and its connection pool. This wrapper owns the delegate it was
     * given, so disposing the bean releases the endpoint's connections.
     */
    @Override
    public void close() {
        delegate.close();
    }
}
