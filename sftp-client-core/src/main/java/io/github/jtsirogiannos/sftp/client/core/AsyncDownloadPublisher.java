package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The download path for providers that implement {@link AsyncSftpSession}.
 *
 * <p>Where {@link InputStreamPublisher} parks a worker thread pulling an {@link java.io.InputStream}
 * and translating demand into reads, this simply hands the provider's own publisher downstream. The
 * bytes are produced wherever the provider produces them — an I/O thread, typically — and demand
 * flows straight through as the read window.
 *
 * <p>No type check on the borrowed session: the pool behind this is typed to {@link AsyncSftpSession},
 * so a blocking one cannot arrive here.
 *
 * <p>Two jobs remain, and they are the only reason this class exists rather than returning the
 * provider's publisher directly:
 *
 * <ul>
 *   <li><strong>Borrowing lazily.</strong> A publisher that is never subscribed must not have taken
 *       a session from the pool. The borrow happens on first subscribe, through
 *       {@code borrowAsync}, so a warm pool starts the transfer without a thread hop at all and a
 *       cold one pays for the connect off the subscriber's thread.</li>
 *   <li><strong>Returning the session exactly once.</strong> A mid-transfer session is not safe to
 *       reuse whether the stream completed, failed or was cancelled, so every terminal route ends at
 *       the same hook and a latch makes sure it fires once.</li>
 * </ul>
 */
final class AsyncDownloadPublisher implements Flow.Publisher<ByteBuffer> {

    private final String remotePath;
    private final long offset;
    private final Supplier<CompletionStage<AsyncSftpSession>> borrow;
    private final Consumer<AsyncSftpSession> onTerminate;

    AsyncDownloadPublisher(String remotePath, long offset,
            Supplier<CompletionStage<AsyncSftpSession>> borrow,
            Consumer<AsyncSftpSession> onTerminate) {
        this.remotePath = remotePath;
        this.offset = offset;
        this.borrow = borrow;
        this.onTerminate = onTerminate;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        borrow.get().whenComplete((session, failure) -> {
            if (failure != null) {
                failBeforeStarting(subscriber, failure);
                return;
            }
            session.readAsync(remotePath, offset)
                    .subscribe(new ReleaseOnceSubscriber(subscriber, () -> onTerminate.accept(session)));
        });
    }

    /**
     * Rule 1.9: a subscriber that has been handed to {@code subscribe} must receive
     * {@code onSubscribe} before anything else, including a failure that happened before there was
     * ever a subscription to speak of.
     */
    private static void failBeforeStarting(Flow.Subscriber<? super ByteBuffer> subscriber,
            Throwable failure) {
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                // Already terminal; demand is meaningless and must not signal anything further.
            }

            @Override
            public void cancel() {
            }
        });
        subscriber.onError(failure);
    }

    /** Passes signals through untouched, and returns the session on whichever terminal comes first. */
    private static final class ReleaseOnceSubscriber implements Flow.Subscriber<ByteBuffer> {

        private final Flow.Subscriber<? super ByteBuffer> downstream;
        private final Runnable release;
        private final AtomicBoolean released = new AtomicBoolean();

        ReleaseOnceSubscriber(Flow.Subscriber<? super ByteBuffer> downstream, Runnable release) {
            this.downstream = downstream;
            this.release = release;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    subscription.request(n);
                }

                @Override
                public void cancel() {
                    // Cancel first: the provider stops the transfer, and only then is the session
                    // settled. Releasing before cancelling would hand a still-streaming session back.
                    subscription.cancel();
                    releaseOnce();
                }
            });
        }

        @Override
        public void onNext(ByteBuffer item) {
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            releaseOnce();
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            releaseOnce();
            downstream.onComplete();
        }

        private void releaseOnce() {
            if (released.compareAndSet(false, true)) {
                release.run();
            }
        }
    }
}
