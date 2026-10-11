package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression cover for the upload path.
 *
 * <p>This exists because {@code WriteSubscriber} shipped with a bug that corrupted <em>every</em>
 * reactive upload — it reported success and wrote a short file — and nothing caught it: the blocking
 * path does not go through this class, so it had never executed. The cause is subtle enough to come
 * back. Reactive Streams delivers {@code onComplete} without waiting for outstanding demand, so if
 * each {@code onNext} merely queues its write on a multi-threaded executor, the close scheduled by
 * {@code onComplete} can overtake the final write.
 *
 * <p>Deliberately no SSH server: a recording {@link OutputStream} observes far more than a byte
 * comparison could — writes arriving after close, and two writes overlapping in time.
 */
public class WriteSubscriberTest {

    private static final int CHUNKS = 64;
    private static final int CHUNK_SIZE = 512;
    private static final int ATTEMPTS = 25;

    /**
     * Repeated because the failure is a race: a single run can pass by luck. Against the original
     * implementation this fails on essentially every attempt.
     */
    @Test
    public void writesEveryChunkBeforeClosingTheStream() throws Exception {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            ExecutorService worker = Executors.newFixedThreadPool(4);
            try {
                RecordingStream out = new RecordingStream();
                CompletableFuture<Void> result = new CompletableFuture<>();
                StubPool pool = new StubPool();
                StubSession session = new StubSession();

                WriteSubscriber subscriber =
                        new WriteSubscriber(out, () -> pool.discard(session), worker, result);

                SubmissionPublisher<ByteBuffer> publisher = new SubmissionPublisher<>(worker, 16);
                publisher.subscribe(subscriber);
                for (int i = 0; i < CHUNKS; i++) {
                    publisher.submit(ByteBuffer.wrap(new byte[CHUNK_SIZE]));
                }
                publisher.close(); // -> onComplete, which must not overtake the queued writes

                result.get(30, TimeUnit.SECONDS);

                assertThat(out.bytesWritten.get())
                        .describedAs("attempt %s: every chunk must land before close", attempt)
                        .isEqualTo((long) CHUNKS * CHUNK_SIZE);
                assertThat(out.writesAfterClose.get())
                        .describedAs("attempt %s: write after close", attempt)
                        .isZero();
                assertThat(out.overlappingWrites.get())
                        .describedAs("attempt %s: concurrent writes to one stream", attempt)
                        .isZero();
                assertThat(out.closed).describedAs("attempt %s: stream must be closed", attempt).isTrue();
                assertThat(pool.discarded)
                        .describedAs("attempt %s: mid-transfer session is discarded, never recycled", attempt)
                        .isTrue();
                assertThat(pool.recycled).isFalse();
            } finally {
                worker.shutdownNow();
            }
        }
    }

    /**
     * A write that fails must complete the result exceptionally rather than leave it pending.
     *
     * <p>Both flavours matter. On failure the subscriber cancels, which means the publisher owes it
     * no further signal — so the subscriber itself is the only thing that can ever complete the
     * result. An unchecked exception escaping the write task used to slip past the {@code
     * IOException}-only catch and hang the upload forever; that is why {@code RuntimeException} is
     * covered here too.
     */
    @Test(dataProvider = "writeFailures", timeOut = 60_000)
    public void failedWriteCompletesTheResultExceptionally(boolean unchecked) throws Exception {
        ExecutorService worker = Executors.newFixedThreadPool(2);
        try {
            RecordingStream out = new RecordingStream();
            out.failFrom = 4;
            out.failUnchecked = unchecked;
            CompletableFuture<Void> result = new CompletableFuture<>();

            WriteSubscriber subscriber =
                    new WriteSubscriber(out, () -> { }, worker, result);

            // Capacity above CHUNKS on purpose: submit() blocks when the buffer fills, and after a
            // failure no more demand arrives - a smaller buffer deadlocks the test rather than the
            // code under test.
            SubmissionPublisher<ByteBuffer> publisher = new SubmissionPublisher<>(worker, CHUNKS * 2);
            publisher.subscribe(subscriber);
            for (int i = 0; i < CHUNKS; i++) {
                publisher.submit(ByteBuffer.wrap(new byte[CHUNK_SIZE]));
            }
            publisher.close();

            // Specific on purpose. A weaker "it fails somehow" assertion passes even when the
            // upload silently reports success after dropping writes, which is the failure mode
            // actually worth catching here.
            assertThat(result)
                    .failsWithin(30, TimeUnit.SECONDS)
                    .withThrowableOfType(java.util.concurrent.ExecutionException.class)
                    .havingCause()
                    .isInstanceOf(io.github.jtsirogiannos.sftp.client.api.SftpException.class);
            assertThat(out.bytesWritten.get())
                    .describedAs("a failed upload must not claim to have written everything")
                    .isLessThan((long) CHUNKS * CHUNK_SIZE);
        } finally {
            worker.shutdownNow();
        }
    }

    @org.testng.annotations.DataProvider(name = "writeFailures")
    public Object[][] writeFailures() {
        return new Object[][] {{false}, {true}};
    }

    /** Records what a real remote stream could not tell us: ordering and overlap. */
    private static final class RecordingStream extends OutputStream {

        final AtomicLong bytesWritten = new AtomicLong();
        final AtomicInteger writesAfterClose = new AtomicInteger();
        final AtomicInteger overlappingWrites = new AtomicInteger();
        final AtomicInteger inFlight = new AtomicInteger();
        volatile boolean closed;
        volatile int failFrom = -1;
        volatile boolean failUnchecked;
        private final AtomicInteger writeCount = new AtomicInteger();

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed) {
                writesAfterClose.incrementAndGet();
            }
            if (inFlight.incrementAndGet() > 1) {
                overlappingWrites.incrementAndGet();
            }
            try {
                if (failFrom >= 0 && writeCount.incrementAndGet() >= failFrom) {
                    if (failUnchecked) {
                        throw new IllegalStateException("simulated unchecked failure");
                    }
                    throw new IOException("simulated failure");
                }
                // Widen the window: a real remote write is slow, and the race needs time to lose.
                Thread.sleep(0, 200_000);
                bytesWritten.addAndGet(len);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class StubPool implements SftpSessionPool<SftpSession> {
        volatile boolean discarded;
        volatile boolean recycled;

        @Override
        public SftpSession borrow() {
            throw new UnsupportedOperationException("the subscriber is handed a session, never borrows");
        }

        @Override
        public void recycle(SftpSession session) {
            recycled = true;
        }

        @Override
        public void discard(SftpSession session) {
            discarded = true;
        }

        @Override
        public void close() {
        }
    }

    /** Never touched by the subscriber beyond being handed back to the pool. */
    private static final class StubSession implements SftpSession {

        @Override
        public void list(String path, java.util.function.Predicate<RemoteFile> visitor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteFile lstat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void symlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String readlink(String linkPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RemoteFile stat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mkdir(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rename(String from, String to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream openRead(String path, long offset) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream openWrite(String path, WriteMode mode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).build();
        }

        @Override
        public String realpath(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAttributes(String path,
                io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void hardlink(String linkPath, String targetPath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public io.github.jtsirogiannos.sftp.client.api.FilesystemStats statvfs(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}
