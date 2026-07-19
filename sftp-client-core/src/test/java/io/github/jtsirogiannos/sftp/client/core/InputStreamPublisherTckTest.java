package io.github.jtsirogiannos.sftp.client.core;

import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;

/**
 * Runs the official Reactive Streams TCK against {@link InputStreamPublisher}.
 *
 * <p>The README calls that class the one genuinely fiddly piece of the library, and this is the
 * verification it asks for. It is worth the TestNG dependency: the suite drives demand accounting,
 * re-entrant {@code request()}, cancellation, signal ordering and overflow — roughly thirty rules
 * that a hand-written test will not think to check, and that a byte-comparison round-trip cannot
 * detect. Its sibling {@code WriteSubscriber} shipped with a race that corrupted every reactive
 * upload precisely because nothing exercised it.
 *
 * <p>One chunk per element: {@code chunkSize = 1} over a byte array of length {@code elements}
 * yields exactly {@code elements} onNext signals followed by onComplete, which is the contract the
 * TCK expects of {@code createFlowPublisher}.
 */
public class InputStreamPublisherTckTest extends FlowPublisherVerification<ByteBuffer> {

    private ExecutorService executor;

    public InputStreamPublisherTckTest() {
        super(new TestEnvironment());
    }

    @BeforeClass
    public void startExecutor() {
        executor = Executors.newFixedThreadPool(4);
    }

    @AfterClass
    public void stopExecutor() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public Flow.Publisher<ByteBuffer> createFlowPublisher(long elements) {
        return new InputStreamPublisher(
                () -> bytes(elements),
                1,
                executor,
                failure -> {
                });
    }

    /**
     * Yields exactly {@code count} bytes without allocating them.
     *
     * <p>An array-backed stream would cap the publisher at {@code Integer.MAX_VALUE} elements, and
     * declaring that ceiling makes the TCK skip {@code required_spec317_*} — the demand-overflow
     * rules. Those are worth keeping: the subscription's {@code addDemand} deliberately saturates at
     * {@code Long.MAX_VALUE} rather than wrapping negative, and this is the only thing that checks
     * the branch. Generating on the fly lets {@code count} be {@code Long.MAX_VALUE}.
     */
    private static InputStream bytes(long count) {
        if (count <= 4096) {
            return new ByteArrayInputStream(new byte[(int) count]);
        }
        return new InputStream() {
            private long remaining = count;

            @Override
            public int read() {
                return remaining-- > 0 ? 0 : -1;
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (remaining <= 0) {
                    return -1;
                }
                int toRead = (int) Math.min(len, remaining);
                Arrays.fill(b, off, off + toRead, (byte) 0);
                remaining -= toRead;
                return toRead;
            }
        };
    }

    /**
     * Null skips the "fails immediately after onSubscribe" tests, and that is the honest answer
     * rather than a workaround: this publisher opens its stream lazily inside the drain loop, so it
     * cannot fail before the first {@code request()}. That laziness is deliberate — it is what keeps
     * an unsubscribed publisher from borrowing a pooled session it never uses.
     */
    @Override
    public Flow.Publisher<ByteBuffer> createFailedFlowPublisher() {
        return null;
    }
}
