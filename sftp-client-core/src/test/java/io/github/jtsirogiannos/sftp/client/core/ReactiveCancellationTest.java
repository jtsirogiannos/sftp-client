package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The half of ROADMAP Phase 2 that was still a TODO: what happens when a transfer is cancelled, and
 * whether a slow consumer can be made to buffer without bound.
 *
 * <p>Cancellation is the dangerous one. A cancelled download must <em>discard</em> its session
 * rather than return it to the pool — the bytes still in flight would corrupt the next borrower's
 * read. If the discard is missed, nothing fails loudly: the pool simply loses a slot per cancelled
 * transfer until {@code borrow()} blocks forever and the application appears to hang. Nothing had
 * ever called {@code cancel()} on this publisher before these tests.
 *
 * <p>Fakes rather than a real server, deliberately: the assertions are about pool accounting and
 * demand, and a fake stream can be made slow and observable in ways a real one cannot.
 */
public class ReactiveCancellationTest {

    private static final int PAYLOAD = 4 * 1024 * 1024;
    private static final Duration LONG_IDLE = Duration.ofMinutes(5);
    private static final Duration ACQUIRE = Duration.ofSeconds(5);

    /** A cancelled download must give the session back as discarded, freeing the pool slot. */
    @Test(timeOut = 30_000)
    public void cancellingADownloadDiscardsTheSessionAndFreesTheSlot() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newFixedThreadPool(4);
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                provider, config(), 1, LONG_IDLE, ACQUIRE)) {

            DefaultReactiveSftpClient client = new DefaultReactiveSftpClient(provider, pool, worker);

            CountDownLatch firstChunk = new CountDownLatch(1);
            AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
            AtomicInteger delivered = new AtomicInteger();

            client.download("/upload/big.bin").subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription s) {
                    subscription.set(s);
                    s.request(1);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    delivered.incrementAndGet();
                    firstChunk.countDown();
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            });

            assertThat(firstChunk.await(10, TimeUnit.SECONDS))
                    .describedAs("the publisher should deliver the first requested chunk")
                    .isTrue();

            // Cancel far from EOF: the session is mid-transfer and must not be recycled.
            subscription.get().cancel();

            awaitTrue(() -> pool.liveSessions() == 0,
                    "cancelling must release the session; the pool still reports "
                            + pool.liveSessions() + " live");

            assertThat(pool.idleSessions())
                    .describedAs("a mid-transfer session must be discarded, never parked for reuse")
                    .isZero();
            assertThat(provider.lastSession().closed())
                    .describedAs("the discarded session must actually be closed")
                    .isTrue();
            awaitTrue(() -> provider.lastSession().streamClosed(),
                    "the underlying stream must be closed when the transfer is abandoned");

            // The real symptom of a leak: the next borrow blocks until the acquire timeout.
            SftpSession borrowed = pool.borrow();
            assertThat(borrowed).describedAs("a freed slot must be reusable").isNotNull();
            pool.discard(borrowed);
        } finally {
            worker.shutdownNow();
        }
    }

    /** Repeated cancellation is where a one-slot-per-cancel leak becomes obvious. */
    @Test(timeOut = 60_000)
    public void repeatedCancellationDoesNotExhaustThePool() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newFixedThreadPool(4);
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                provider, config(), 2, LONG_IDLE, Duration.ofSeconds(3))) {

            DefaultReactiveSftpClient client = new DefaultReactiveSftpClient(provider, pool, worker);

            for (int attempt = 0; attempt < 10; attempt++) {
                CountDownLatch firstChunk = new CountDownLatch(1);
                AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

                client.download("/upload/big.bin").subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription s) {
                        subscription.set(s);
                        s.request(1);
                    }

                    @Override
                    public void onNext(ByteBuffer item) {
                        firstChunk.countDown();
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onComplete() {
                    }
                });

                assertThat(firstChunk.await(10, TimeUnit.SECONDS)).isTrue();
                subscription.get().cancel();
                awaitTrue(() -> pool.liveSessions() == 0,
                        "attempt " + attempt + ": pool did not recover, live="
                                + pool.liveSessions());
            }
        } finally {
            worker.shutdownNow();
        }
    }

    /**
     * Backpressure: the publisher must never run ahead of demand.
     *
     * <p>That is the property that keeps a slow consumer from OOMing — not a memory measurement,
     * which would be flaky, but the invariant underneath it. If it emits only what was requested,
     * an unread remainder stays in the remote file rather than in the heap.
     */
    @Test(timeOut = 30_000)
    public void neverEmitsMoreThanWasRequested() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newFixedThreadPool(4);
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                provider, config(), 2, LONG_IDLE, ACQUIRE)) {

            DefaultReactiveSftpClient client = new DefaultReactiveSftpClient(provider, pool, worker);

            AtomicInteger delivered = new AtomicInteger();
            AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();
            CountDownLatch twoChunks = new CountDownLatch(2);

            client.download("/upload/big.bin").subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription s) {
                    subscription.set(s);
                    s.request(2); // and never again
                }

                @Override
                public void onNext(ByteBuffer item) {
                    delivered.incrementAndGet();
                    twoChunks.countDown();
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            });

            assertThat(twoChunks.await(10, TimeUnit.SECONDS)).isTrue();
            // A publisher that prefetches would keep going here; give it every chance to misbehave.
            Thread.sleep(500);

            assertThat(delivered.get())
                    .describedAs("emitted more than the 2 chunks requested — demand is not respected")
                    .isEqualTo(2);
            assertThat(provider.lastSession().bytesRead())
                    .describedAs("read far beyond demand, so the remainder is buffered somewhere")
                    .isLessThanOrEqualTo(3L * 32 * 1024);

            subscription.get().cancel();
            awaitTrue(() -> pool.liveSessions() == 0, "cancel after backpressure should release");
        } finally {
            worker.shutdownNow();
        }
    }

    /**
     * Cancelling the returned CompletionStage must stop the upload and release the session.
     *
     * <p>This is the path a cancelled coroutine or a disposed {@code Mono} takes. Before it was
     * wired up the future completed as cancelled while the transfer carried on writing to a session
     * that was never handed back — a pool slot lost per cancellation, and bytes still landing on a
     * file the caller had given up on.
     */
    @Test(timeOut = 30_000)
    public void cancellingTheUploadFutureStopsTheTransferAndFreesTheSession() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newFixedThreadPool(4);
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                provider, config(), 1, LONG_IDLE, ACQUIRE)) {

            DefaultReactiveSftpClient client = new DefaultReactiveSftpClient(provider, pool, worker);
            SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>(worker, 64);
            CompletableFuture<Void> upload =
                    client.upload("/upload/cancelled.bin", source).toCompletableFuture();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!source.hasSubscribers() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(source.hasSubscribers()).isTrue();

            for (int i = 0; i < 200; i++) {
                source.submit(ByteBuffer.wrap(new byte[1024]));
            }
            // Let a little of it through, then give up on the transfer.
            Thread.sleep(50);
            assertThat(upload.cancel(true)).isTrue();

            awaitTrue(() -> pool.liveSessions() == 0,
                    "a cancelled upload must release its session; pool still reports "
                            + pool.liveSessions() + " live");
            awaitTrue(() -> provider.lastSession().writeStreamClosed.get(),
                    "the remote file must be closed when the upload is cancelled");

            int writtenAtCancel = provider.lastSession().bytesWritten.get();
            Thread.sleep(200); // anything still moving would land in this window
            int writtenAfter = provider.lastSession().bytesWritten.get();

            // Not "zero more bytes": demand is one chunk at a time, so a write already handed to
            // the stream when cancel fired will finish — those bytes cannot be recalled. What must
            // not happen is the transfer carrying on.
            assertThat(writtenAfter - writtenAtCancel)
                    .describedAs("at most the single in-flight chunk may still land")
                    .isLessThanOrEqualTo(1024);
            assertThat(writtenAfter)
                    .describedAs("the upload ran to completion despite being cancelled")
                    .isLessThan(200 * 1024);

            source.close();
        } finally {
            worker.shutdownNow();
        }
    }

    /** Cancelling before the worker task runs must not borrow a session at all. */
    @Test(timeOut = 30_000)
    public void cancellingImmediatelyNeverBorrowsASession() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                provider, config(), 1, LONG_IDLE, ACQUIRE)) {

            DefaultReactiveSftpClient client = new DefaultReactiveSftpClient(provider, pool, worker);
            // Occupy the single worker thread so the upload task cannot start yet.
            CountDownLatch release = new CountDownLatch(1);
            worker.execute(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>();
            CompletableFuture<Void> upload =
                    client.upload("/upload/never-started.bin", source).toCompletableFuture();
            assertThat(upload.cancel(true)).isTrue();
            release.countDown();

            Thread.sleep(200); // give the queued task its chance to misbehave
            assertThat(pool.liveSessions())
                    .describedAs("a cancelled upload should never have opened a session")
                    .isZero();
            source.close();
        } finally {
            worker.shutdownNow();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError(message);
    }

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host("localhost")
                .username("test")
                .password("secret".toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }

    private static final class CountingProvider implements SftpProvider {

        private final AtomicReference<FakeSession> last = new AtomicReference<>();

        FakeSession lastSession() {
            return last.get();
        }

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public SftpCapabilities capabilities() {
            // pipelinedStreaming=false gives the 32 KiB chunk size the assertions above assume.
            return SftpCapabilities.builder().maxSftpVersion(3).build();
        }

        @Override
        public SftpSession connect(SftpConnectionConfig config) {
            FakeSession session = new FakeSession();
            last.set(session);
            return session;
        }
    }

    /** Serves a large, slow stream and records exactly how much of it was consumed. */
    private static final class FakeSession implements SftpSession {

        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean streamClosed = new AtomicBoolean();
        private final AtomicInteger read = new AtomicInteger();

        boolean closed() {
            return closed.get();
        }

        boolean streamClosed() {
            return streamClosed.get();
        }

        long bytesRead() {
            return read.get();
        }

        @Override
        public InputStream openRead(String path, long offset) {
            return new InputStream() {
                private int remaining = PAYLOAD;

                @Override
                public int read() {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) < 0 ? -1 : one[0];
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    if (remaining <= 0) {
                        return -1;
                    }
                    // Slow enough that a prefetching publisher would visibly run ahead.
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return -1;
                    }
                    int n = Math.min(len, remaining);
                    remaining -= n;
                    ReactiveCancellationTest.FakeSession.this.read.addAndGet(n);
                    return n;
                }

                @Override
                public void close() {
                    streamClosed.set(true);
                }
            };
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
            return !closed.get();
        }

        @Override
        public void close() {
            closed.set(true);
        }

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

        final AtomicInteger bytesWritten = new AtomicInteger();
        final AtomicBoolean writeStreamClosed = new AtomicBoolean();

        @Override
        public OutputStream openWrite(String path, WriteMode mode) {
            return new OutputStream() {
                @Override
                public void write(int b) {
                    write(new byte[] {(byte) b}, 0, 1);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    try {
                        Thread.sleep(5); // slow enough to cancel mid-flight
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    bytesWritten.addAndGet(len);
                }

                @Override
                public void close() {
                    writeStreamClosed.set(true);
                }
            };
        }
    }
}
