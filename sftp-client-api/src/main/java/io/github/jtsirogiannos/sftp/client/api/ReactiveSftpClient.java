package io.github.jtsirogiannos.sftp.client.api;

import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Non-blocking facade expressed entirely in JDK types — {@link CompletionStage} and
 * {@link Flow.Publisher}. No Mutiny, no Reactor, no Vert.x. That is the whole point: this jar must
 * be addable to a Spring, Quarkus, or plain-Java build without dragging a reactive runtime along.
 *
 * <p>Framework modules adapt at the edge:
 * <ul>
 *   <li>Quarkus/Mutiny — Mutiny 2 is natively {@code Flow}-based, so
 *       {@code Multi.createFrom().publisher(...)} and
 *       {@code Uni.createFrom().completionStage(...)} take these directly. No bridge jar.</li>
 *   <li>Spring/Reactor — {@code Flux.from(FlowAdapters.toPublisher(...))} and
 *       {@code Mono.fromCompletionStage(...)}. One adapter call.</li>
 * </ul>
 *
 * <p>Honesty note carried over from the capability model: {@link #download}/{@link #upload} are
 * demand-driven and backpressured, but the pull always happens on a worker thread. When
 * {@link #capabilities()} reports {@code pipelinedStreaming} the provider overlaps requests so that
 * worker rarely stalls — it is not zero-thread async, and this API never pretends it is.
 */
public interface ReactiveSftpClient extends AutoCloseable {

    CompletionStage<List<RemoteFile>> list(String path);

    CompletionStage<RemoteFile> stat(String path);

    CompletionStage<Boolean> exists(String path);

    CompletionStage<Void> mkdir(String path);

    CompletionStage<Void> rename(String from, String to);

    CompletionStage<Void> remove(String path);

    /**
     * Streams a remote file as backpressured chunks. Cancelling the subscription interrupts the
     * transfer and discards the underlying session rather than returning it to the pool.
     *
     * <p>Buffers are handed off to the subscriber and not reused, so they are safe to retain.
     */
    Flow.Publisher<ByteBuffer> download(String remotePath);

    /**
     * Consumes {@code data} with backpressure and writes it to the remote path, completing when the
     * upstream completes and the final flush lands.
     *
     * <p><strong>Cancelling the returned stage cancels the transfer.</strong> The upstream
     * subscription is cancelled, the remote file is closed and the session is discarded rather than
     * pooled. This is what makes a cancelled coroutine or a disposed {@code Mono} actually stop the
     * work rather than leave it running against a result nobody is waiting for.
     *
     * <p>One chunk may still land after cancelling: demand is one buffer at a time, and a write
     * already handed to the stream cannot be recalled. Nothing further is requested or written.
     */
    CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data);

    /**
     * Publishes by rename: uploads to a scratch name beside the destination and renames it into
     * place, so a reader watching the destination never sees a partially written file.
     *
     * <p>The reactive counterpart of {@link SftpOperations#uploadAtomically}, with the same promise
     * and the same refusal. A default method composed from {@link #exists}, {@link #upload},
     * {@link #rename} and {@link #remove}, so both the worker-offloaded and the thread-free client
     * inherit it and neither implements anything.
     *
     * <p>Replacing an existing file needs a rename that overwrites — {@code posix-rename}, SFTP v5 or
     * later. Against a v3 server the returned stage fails with {@link UnsupportedOperationException}
     * rather than falling back to remove-then-rename, because that gap is the window this method
     * exists to close. Publishing to a path that does not exist yet is atomic on every version.
     *
     * <p>{@code data} is subscribed at most once, so a failure is never retried behind the caller's
     * back. If the upload or the rename fails, the scratch file is removed on a best-effort basis and
     * the original failure is what the stage reports; a cleanup failure is attached to it as a
     * suppressed exception rather than replacing it.
     */
    default CompletionStage<Void> uploadAtomically(String remotePath,
            Flow.Publisher<ByteBuffer> data) {
        String temporary = AtomicPublish.temporaryPathFor(remotePath);
        return exists(remotePath).thenCompose(present -> {
            if (present && !capabilities().posixRename()) {
                return CompletableFuture.<Void>failedFuture(
                    AtomicPublish.cannotReplace(remotePath, capabilities()));
            }
            return upload(temporary, data)
                .thenCompose(uploaded -> rename(temporary, remotePath))
                // handle() rather than exceptionally(): the cleanup has to run on the failure path
                // without swallowing the success path, and it is itself asynchronous.
                .handle((published, failure) -> failure)
                .thenCompose(failure -> failure == null
                    ? CompletableFuture.<Void>completedFuture(null)
                    : cleanUpThenFail(temporary, AtomicPublish.unwrap(failure)));
        });
    }

    /** Best-effort removal of the scratch file, then re-fails with the original cause. */
    private CompletionStage<Void> cleanUpThenFail(String temporary, Throwable original) {
        return remove(temporary)
            .handle((removed, cleanupFailure) -> cleanupFailure)
            .thenCompose(cleanupFailure -> {
                if (cleanupFailure != null) {
                    // A leftover .part file is visible and harmless; losing the real cause is not.
                    original.addSuppressed(AtomicPublish.unwrap(cleanupFailure));
                }
                return CompletableFuture.<Void>failedFuture(original);
            });
    }

    SftpCapabilities capabilities();

    /**
     * Closes the pool and every idle session in it. Idempotent, and never throws — note the narrowed
     * signature, so {@code try}-with-resources needs no catch.
     *
     * <p>This exists for the same reason {@link SftpClient#close()} does: the client owns a pool of
     * live connections. Without it a container that builds a reactive client per configured endpoint
     * has no way to release them at shutdown.
     *
     * <p>Does not shut down the {@link java.util.concurrent.Executor} the client was given — that
     * belongs to whoever created it, and is very often shared.
     */
    @Override
    void close();
}
