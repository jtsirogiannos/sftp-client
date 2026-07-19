package io.github.jtsirogiannos.sftp.client.core;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * Wraps a byte publisher and reports how much flowed through it, once, when it finishes.
 *
 * <p>Shared by the observability modules so metrics and tracing count the same thing the same way.
 * It lives in the core because it is pure {@code Flow} plumbing that knows nothing about meters or
 * spans, which is the same reason {@link ForwardingSftpOperations} does.
 *
 * <p>The total is taken from {@link ByteBuffer#remaining()} as each buffer passes, which is what the
 * subscriber will actually see. Reading it is not destructive, so the buffer goes downstream
 * untouched.
 *
 * <p>The callback fires exactly once, on whichever terminal arrives first — completion, failure, or
 * cancellation. Cancellation counts too: a transfer abandoned half way still moved those bytes, and
 * reporting nothing for it would quietly under-count every cancelled download.
 */
public final class ByteCountingPublisher implements Flow.Publisher<ByteBuffer> {

    private final Flow.Publisher<ByteBuffer> delegate;
    private final LongConsumer onFinished;

    public ByteCountingPublisher(Flow.Publisher<ByteBuffer> delegate, LongConsumer onFinished) {
        this.delegate = delegate;
        this.onFinished = onFinished;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        delegate.subscribe(new CountingSubscriber(subscriber, onFinished));
    }

    private static final class CountingSubscriber implements Flow.Subscriber<ByteBuffer> {

        private final Flow.Subscriber<? super ByteBuffer> downstream;
        private final LongConsumer onFinished;
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicBoolean reported = new AtomicBoolean();

        CountingSubscriber(Flow.Subscriber<? super ByteBuffer> downstream, LongConsumer onFinished) {
            this.downstream = downstream;
            this.onFinished = onFinished;
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
                    subscription.cancel();
                    report();
                }
            });
        }

        @Override
        public void onNext(ByteBuffer item) {
            bytes.addAndGet(item.remaining());
            downstream.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            report();
            downstream.onError(throwable);
        }

        @Override
        public void onComplete() {
            report();
            downstream.onComplete();
        }

        private void report() {
            if (reported.compareAndSet(false, true)) {
                onFinished.accept(bytes.get());
            }
        }
    }
}
