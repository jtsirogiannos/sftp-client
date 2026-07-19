package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Emits a byte array in fixed-size chunks, honouring demand.
 *
 * <p>Small, but not trivial: this is a Reactive Streams publisher, and the rules that bite are the
 * ones about concurrency rather than the ones about data. Demand can be signalled from any thread
 * and re-entrantly from inside {@code onNext}, so emission runs through a drain loop with a demand
 * counter — exactly one thread is ever inside the loop, which is what rule 1.3 asks for. A
 * re-entrant {@code request} therefore adds demand and returns rather than nesting.
 */
final class ByteArrayPublisher implements Flow.Publisher<ByteBuffer> {

    private final byte[] content;
    private final int chunkSize;

    ByteArrayPublisher(byte[] content, int chunkSize) {
        this.content = content;
        this.chunkSize = chunkSize;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber, "subscriber");
        subscriber.onSubscribe(new ChunkSubscription(subscriber));
    }

    private final class ChunkSubscription implements Flow.Subscription {

        private final Flow.Subscriber<? super ByteBuffer> subscriber;
        private final AtomicLong demand = new AtomicLong();
        private int position;
        private volatile boolean cancelled;
        private boolean terminated;

        ChunkSubscription(Flow.Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                // Rule 3.9. A cancelled subscription is silent, so check that first.
                if (!cancelled) {
                    cancelled = true;
                    subscriber.onError(new IllegalArgumentException(
                            "non-positive subscription request: " + n));
                }
                return;
            }
            if (cancelled) {
                return;
            }
            // Unbounded demand saturates rather than overflowing, per rule 3.17.
            demand.accumulateAndGet(n, (current, added) -> {
                long sum = current + added;
                return sum < 0 ? Long.MAX_VALUE : sum;
            });
            drain();
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        private void drain() {
            do {
                while (demand.get() > 0 && !cancelled && !terminated) {
                    if (position >= content.length) {
                        terminated = true;
                        subscriber.onComplete();
                        return;
                    }
                    int length = Math.min(chunkSize, content.length - position);
                    // A read-only slice: a subscriber that flips or compacts what it is handed must
                    // not be able to corrupt the stored file, which is shared with every session.
                    ByteBuffer chunk = ByteBuffer
                            .wrap(content, position, length)
                            .slice()
                            .asReadOnlyBuffer();
                    position += length;
                    demand.decrementAndGet();
                    subscriber.onNext(chunk);
                }
            } while (demand.get() > 0 && !cancelled && !terminated);
        }
    }
}
