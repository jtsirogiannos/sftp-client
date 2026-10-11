package io.github.jtsirogiannos.sftp.client.core;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Small publishers and subscribers for driving the reactive facade in tests. */
final class ReactiveFlows {

    private ReactiveFlows() {
    }

    /** One buffer, then complete. */
    static Flow.Publisher<ByteBuffer> text(String content) {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private final AtomicBoolean sent = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (sent.compareAndSet(false, true)) {
                    subscriber.onNext(ByteBuffer.wrap(bytes));
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                sent.set(true);
            }
        });
    }

    /** Fails as soon as anything is asked of it. */
    static Flow.Publisher<ByteBuffer> failing(RuntimeException failure) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private final AtomicBoolean done = new AtomicBoolean();

            @Override
            public void request(long n) {
                if (done.compareAndSet(false, true)) {
                    subscriber.onError(failure);
                }
            }

            @Override
            public void cancel() {
                done.set(true);
            }
        });
    }

    /** Everything the publisher emits, as text, once it completes. */
    static CompletableFuture<String> collect(Flow.Publisher<ByteBuffer> publisher) {
        CompletableFuture<String> result = new CompletableFuture<>();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                bytes.writeBytes(chunk);
            }

            @Override
            public void onError(Throwable throwable) {
                result.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                result.complete(bytes.toString(StandardCharsets.UTF_8));
            }
        });
        return result;
    }

    /** A subscriber that takes nothing until told to, so a download stays open. */
    static final class Holding implements Flow.Subscriber<ByteBuffer> {

        final CompletableFuture<Flow.Subscription> subscribed = new CompletableFuture<>();
        final CompletableFuture<Throwable> ended = new CompletableFuture<>();

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscribed.complete(subscription);
        }

        @Override
        public void onNext(ByteBuffer item) {
        }

        @Override
        public void onError(Throwable throwable) {
            ended.complete(throwable);
        }

        @Override
        public void onComplete() {
            ended.complete(null);
        }
    }
}
