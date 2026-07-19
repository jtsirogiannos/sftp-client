package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demand-pacing subscriber for uploads: requests exactly one buffer at a time and only asks for the
 * next after the current one has been written. That is what makes a slow remote throttle the
 * producer rather than letting an unbounded source buffer a whole file in heap.
 *
 * <p>Writes are dispatched to the worker executor, so {@code onNext} never blocks the thread that
 * produced the buffer (which may be an event loop).
 */
final class WriteSubscriber implements Flow.Subscriber<ByteBuffer> {

    private static final Logger log = LoggerFactory.getLogger(WriteSubscriber.class);

    private final OutputStream out;
    private final SftpSession session;
    private final SftpSessionPool<SftpSession> pool;
    private final Executor worker;
    private final CompletableFuture<Void> result;
    private final AtomicBoolean finished = new AtomicBoolean();
    /** Written by onSubscribe, read by the cancellation watcher on another thread. */
    private volatile Flow.Subscription subscription;

    /**
     * Serialises every signal onto one ordered sequence of worker tasks.
     *
     * <p>Without this, {@code onNext} and {@code onComplete} each submit an independent task to a
     * multi-threaded worker. Reactive Streams delivers {@code onComplete} without waiting for
     * outstanding demand, so the close would race the final write and silently truncate the upload —
     * it reported success and wrote a short file. Chaining keeps the producer thread unblocked while
     * guaranteeing writes land, in order, before the flush.
     *
     * <p>Only ever touched from {@code onNext}/{@code onError}/{@code onComplete}, which the spec
     * requires to be signalled serially; {@code volatile} covers visibility when those signals
     * arrive on different threads.
     */
    private volatile CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);

    WriteSubscriber(OutputStream out, SftpSession session, SftpSessionPool<SftpSession> pool, Executor worker,
                    CompletableFuture<Void> result) {
        this.out = out;
        this.session = session;
        this.pool = pool;
        this.worker = worker;
        this.result = result;
        // Cancelling the returned CompletionStage has to reach the transfer, or the upload keeps
        // running against a future nobody is waiting on and the session is never handed back. This
        // is the CompletionStage-side equivalent of what Subscription.cancel() does for downloads,
        // and it is how a cancelled coroutine or a disposed Mono actually stops the work.
        result.whenComplete((ignored, failure) -> {
            if (failure instanceof CancellationException) {
                cancelTransfer();
            }
        });
    }

    /** Stops the producer and releases the session; idempotent via {@link #finish}. */
    private void cancelTransfer() {
        Flow.Subscription current = subscription;
        if (current != null) {
            current.cancel();
        }
        log.debug("Upload cancelled by the caller; discarding the session");
        finish(new SftpException("upload cancelled"));
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        if (finished.get()) {
            // Cancelled before the producer ever attached; do not ask for data we will discard.
            subscription.cancel();
            return;
        }
        subscription.request(1);
    }

    @Override
    public void onNext(ByteBuffer item) {
        chain = chain.thenRunAsync(() -> {
            if (finished.get()) {
                return; // already failed or cancelled; nothing left to write
            }
            try {
                byte[] bytes = new byte[item.remaining()];
                item.get(bytes);
                out.write(bytes);
                subscription.request(1); // pace: ask for the next only once this one landed
            } catch (IOException | RuntimeException e) {
                subscription.cancel();
                // Cancelling means the publisher owes us no onError/onComplete, so this is the
                // only place the result will ever be completed. Do it here or it hangs forever -
                // and RuntimeException is caught for the same reason: an unchecked failure from a
                // provider's stream must surface as a failed upload, not a permanently pending one.
                finish(new SftpException("upload write failed", e));
            }
        }, worker);
    }

    @Override
    public void onError(Throwable throwable) {
        // Let queued writes drain first so the failure isn't reported while a write is still
        // touching the stream we are about to close.
        chain.whenComplete((ignored, alsoIgnored) -> finish(throwable));
    }

    @Override
    public void onComplete() {
        chain.thenRunAsync(() -> {
            if (finished.get()) {
                return;
            }
            try {
                out.flush();
                out.close();
                finish(null);
            } catch (IOException | RuntimeException e) {
                finish(new SftpException("upload flush failed", e));
            }
        }, worker);
    }

    /** Idempotent: a write failure and a subsequent terminal signal must not both complete it. */
    private void finish(Throwable failure) {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        if (failure != null) {
            log.debug("Upload failed; discarding the session", failure);
        }
        pool.discard(session); // mid-transfer session, never recycled
        if (failure == null) {
            result.complete(null);
        } else {
            closeQuietly();
            result.completeExceptionally(failure);
        }
    }

    private void closeQuietly() {
        try {
            out.close();
        } catch (IOException ignored) {
            // best effort
        }
    }
}
