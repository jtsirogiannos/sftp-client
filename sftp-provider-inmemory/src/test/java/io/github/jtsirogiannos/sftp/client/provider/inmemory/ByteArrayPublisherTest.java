package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The drain loop's concurrency contract, which the conformance suite structurally cannot check —
 * it drains with one up-front {@code request(Long.MAX_VALUE)}, so re-entrancy and contention never
 * arise there.
 */
class ByteArrayPublisherTest {

    /**
     * The canonical one-at-a-time subscriber: {@code request(1)} from inside {@code onNext}. An
     * unserialised drain recurses once per chunk and overflows the stack; a serialised one runs the
     * whole stream in a flat loop, and never nests {@code onNext} inside itself.
     */
    @Test
    void aRequestOneAtATimeSubscriberNeitherRecursesNorSeesNestedSignals() {
        byte[] content = new byte[60_000];
        ByteArrayPublisher publisher = new ByteArrayPublisher(content, 1); // 60k chunks

        AtomicInteger depth = new AtomicInteger();
        AtomicInteger maxDepth = new AtomicInteger();
        AtomicLong received = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription s) {
                subscription = s;
                s.request(1);
            }

            @Override
            public void onNext(ByteBuffer item) {
                int now = depth.incrementAndGet();
                maxDepth.accumulateAndGet(now, Math::max);
                received.addAndGet(item.remaining());
                subscription.request(1);   // re-entrant on an unserialised drain
                depth.decrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
                done.countDown();
            }

            @Override
            public void onComplete() {
                done.countDown();
            }
        });

        assertThat(done.getCount()).describedAs("the whole stream drains synchronously").isZero();
        assertThat(failure.get()).isNull();
        assertThat(received.get()).isEqualTo(60_000);
        assertThat(maxDepth.get())
            .describedAs("onNext delivered inside onNext is the rule-1.3 violation")
            .isEqualTo(1);
    }

    /** Concurrent demand from many threads must neither duplicate nor skip a single byte. */
    @Test
    void concurrentRequestsDeliverEveryChunkExactlyOnce() throws Exception {
        byte[] content = new byte[8_192];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) i;
        }
        ByteArrayPublisher publisher = new ByteArrayPublisher(content, 8);

        AtomicLong received = new AtomicLong();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger inOnNext = new AtomicInteger();
        AtomicInteger overlaps = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Flow.Subscription> ref = new AtomicReference<>();

        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription s) {
                ref.set(s);
            }

            @Override
            public void onNext(ByteBuffer item) {
                if (inOnNext.getAndIncrement() != 0) {
                    overlaps.incrementAndGet();
                }
                received.addAndGet(item.remaining());
                inOnNext.decrementAndGet();
            }

            @Override
            public void onError(Throwable t) {
                done.countDown();
            }

            @Override
            public void onComplete() {
                completions.incrementAndGet();
                done.countDown();
            }
        });

        ExecutorService threads = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch go = new CountDownLatch(1);
            for (int t = 0; t < 4; t++) {
                threads.submit(() -> {
                    go.await();
                    for (int i = 0; i < 300; i++) {
                        ref.get().request(1);
                    }
                    return null;
                });
            }
            go.countDown();
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            threads.shutdownNow();
        }

        assertThat(received.get())
            .describedAs("duplicated or skipped chunks corrupt the byte count")
            .isEqualTo(8_192);
        assertThat(completions.get()).describedAs("exactly one terminal signal").isEqualTo(1);
        assertThat(overlaps.get()).describedAs("concurrent onNext is a rule-1.3 violation").isZero();
    }
}
