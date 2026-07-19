package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.List;

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

    /** Backpressured chunks. Cancelling the subscription abandons the transfer and its session. */
    public Flux<ByteBuffer> download(String remotePath) {
        return Flux.from(FlowAdapters.toPublisher(delegate.download(remotePath)));
    }

    /** Consumes {@code data} with backpressure; completes when the upload has landed. */
    public Mono<Void> upload(String remotePath, Publisher<ByteBuffer> data) {
        return Mono.fromCompletionStage(
                () -> delegate.upload(remotePath, FlowAdapters.toFlowPublisher(data)));
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
