package io.github.jtsirogiannos.sftp.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Turns a blocking {@link InputStream} into a demand-driven {@link Flow.Publisher} of chunks.
 *
 * <p>This is the one genuinely fiddly class in the library, and the reason the whole design works:
 * write correct backpressure once here, and every framework module is a type-conversion shim.
 *
 * <p>Contract points that matter (verify with {@code reactive-streams-tck-flow} at test scope):
 * <ul>
 *   <li>Single subscriber only; a second gets {@code onError}.</li>
 *   <li>{@code request(n <= 0)} signals {@code IllegalArgumentException}.</li>
 *   <li>Never two concurrent {@code onNext} calls — serialized by the {@code wip} drain loop, so
 *       a re-entrant {@code request()} from inside {@code onNext} increments demand rather than
 *       recursing.</li>
 *   <li>Reads happen on the supplied {@link Executor}, never on the caller's thread, so an
 *       event-loop caller is never blocked.</li>
 *   <li>Exactly one terminal signal, and {@code onTerminate} runs exactly once — that hook is how
 *       the pool learns to discard a mid-transfer session.</li>
 * </ul>
 */
public final class InputStreamPublisher implements Flow.Publisher<ByteBuffer> {

    private static final Logger log = LoggerFactory.getLogger(InputStreamPublisher.class);

    private final Supplier<InputStream> streamSupplier;
    private final int chunkSize;
    private final Executor executor;
    private final Consumer<Throwable> onTerminate;
    private final AtomicBoolean subscribed = new AtomicBoolean();

    /**
     * @param streamSupplier opens the stream; invoked lazily on the executor at first request
     * @param onTerminate    called exactly once with the failure, or {@code null} on
     *                       completion/cancellation — used for session cleanup
     */
    public InputStreamPublisher(Supplier<InputStream> streamSupplier, int chunkSize,
                                Executor executor, Consumer<Throwable> onTerminate) {
        this.streamSupplier = Objects.requireNonNull(streamSupplier);
        this.chunkSize = chunkSize;
        this.executor = Objects.requireNonNull(executor);
        this.onTerminate = Objects.requireNonNull(onTerminate);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
        Objects.requireNonNull(subscriber);
        if (!subscribed.compareAndSet(false, true)) {
            subscriber.onSubscribe(NoopSubscription.INSTANCE);
            subscriber.onError(new IllegalStateException("InputStreamPublisher supports one subscriber"));
            return;
        }
        StreamSubscription sub = new StreamSubscription(subscriber);
        subscriber.onSubscribe(sub);
        // Rule 1.3 (signals are serial). Calling request() from inside onSubscribe is legal and
        // common, but it used to start the drain loop immediately - on another thread - so onNext
        // could overtake an onSubscribe that had not returned yet. Demand raised during onSubscribe
        // is recorded and only drained from here, once onSubscribe is definitively finished.
        sub.activate();
    }

    private enum NoopSubscription implements Flow.Subscription {
        INSTANCE;

        @Override
        public void request(long n) {
            // no-op
        }

        @Override
        public void cancel() {
            // no-op
        }
    }

    /**
     * Internal marker so the core can wrap IO failures without depending on the api module's type.
     */
    static final class SftpStreamException extends RuntimeException {
        SftpStreamException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final class StreamSubscription implements Flow.Subscription {

        private final Flow.Subscriber<? super ByteBuffer> subscriber;
        private final AtomicLong demand = new AtomicLong();
        private final AtomicLong wip = new AtomicLong();
        private final AtomicBoolean terminated = new AtomicBoolean();
        private volatile boolean cancelled;
        /** False until onSubscribe has returned; see the note in {@link #subscribe}. */
        private volatile boolean active;
        private InputStream in; // touched only inside the drain loop

        StreamSubscription(Flow.Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                fail(new IllegalArgumentException("request must be positive (rule 3.9), was " + n));
                return;
            }
            addDemand(n);
            schedule();
        }

        @Override
        public void cancel() {
            cancelled = true;
            // A cancelled transfer discards its session. Without this line, a caller wondering why
            // the pool keeps opening connections has nothing at all to look at.
            log.debug("Transfer cancelled by the subscriber; the session will be discarded");
            // No terminal signal on cancel, but resources still have to go back.
            if (terminated.compareAndSet(false, true)) {
                executor.execute(() -> cleanup(null));
            }
        }

        private void addDemand(long n) {
            demand.updateAndGet(current -> {
                long next = current + n;
                return next < 0 ? Long.MAX_VALUE : next; // saturate, don't overflow
            });
        }

        /**
         * Only the thread that flips wip 0 -> 1 drains; others just record work and return.
         */
        private void schedule() {
            if (!active) {
                // Demand is already recorded; activate() starts the drain once onSubscribe returns.
                return;
            }
            if (wip.getAndIncrement() == 0) {
                executor.execute(this::drain);
            }
        }

        /** Unlocks draining after onSubscribe has returned, honouring demand raised inside it. */
        void activate() {
            active = true;
            if (demand.get() > 0 && !cancelled) {
                schedule();
            }
        }

        private void drain() {
            long missed = 1;
            for (; ; ) {
                while (demand.get() > 0 && !cancelled && !terminated.get()) {
                    if (!emitOne()) {
                        break;
                    }
                    demand.decrementAndGet();
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }

        /**
         * @return true if a chunk was emitted and the stream may have more
         */
        private boolean emitOne() {
            try {
                if (in == null) {
                    in = streamSupplier.get();
                }
                byte[] scratch = new byte[chunkSize];
                int read = in.read(scratch);
                if (read < 0) {
                    complete();
                    return false;
                }
                if (read == 0) {
                    return true; // nothing available yet; keep the demand and retry
                }
                // Fresh array per chunk: buffers escape to the subscriber and must not be reused.
                subscriber.onNext(ByteBuffer.wrap(scratch, 0, read).asReadOnlyBuffer());
                return true;
            } catch (IOException e) {
                fail(new SftpStreamException("read failed", e));
                return false;
            } catch (RuntimeException e) {
                fail(e);
                return false;
            }
        }

        private void complete() {
            if (terminated.compareAndSet(false, true)) {
                cleanup(null);
                if (!cancelled) {
                    subscriber.onComplete();
                }
            }
        }

        private void fail(Throwable t) {
            if (terminated.compareAndSet(false, true)) {
                cleanup(t);
                if (!cancelled) {
                    subscriber.onError(t);
                }
            }
        }

        private void cleanup(Throwable t) {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException ignored) {
                    // best effort
                }
            }
            onTerminate.accept(t);
        }
    }
}
