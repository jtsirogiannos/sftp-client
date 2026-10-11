package io.github.jtsirogiannos.sftp.client.pool;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behaviour tests for the pool, against a counting fake provider rather than a real server: the
 * things worth asserting here are how many connections get opened and when, which a real SSH server
 * makes slower to observe and no clearer.
 */
public class BoundedSftpSessionPoolTest {

    private static final Duration LONG_IDLE = Duration.ofMinutes(5);
    private static final Duration SHORT_WAIT = Duration.ofMillis(200);

    /** The whole point: N operations must not mean N handshakes. */
    @Test
    public void reusesOneSessionAcrossSequentialBorrows() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, LONG_IDLE, SHORT_WAIT)) {
            for (int i = 0; i < 20; i++) {
                SftpSession session = pool.borrow();
                pool.recycle(session);
            }
            assertThat(provider.connects.get())
                    .describedAs("20 sequential operations should open exactly one session")
                    .isEqualTo(1);
            assertThat(pool.idleSessions()).isEqualTo(1);
            assertThat(pool.liveSessions()).isEqualTo(1);
        }
    }

    /**
     * A session can die while parked — server-side idle timeout, dropped NAT mapping — and nothing
     * notifies the client. Handing that corpse to the next caller is the classic pool bug.
     */
    @Test
    public void replacesSessionThatDiedWhileParked() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, LONG_IDLE, SHORT_WAIT)) {
            FakeSession first = (FakeSession) pool.borrow();
            pool.recycle(first);

            first.valid = false; // the server hung up while it sat in the pool

            SftpSession second = pool.borrow();
            assertThat(second).isNotSameAs(first);
            assertThat(first.closed).describedAs("the dead session must be closed, not leaked").isTrue();
            assertThat(provider.connects.get()).isEqualTo(2);
            assertThat(pool.liveSessions()).isEqualTo(1);
        }
    }

    /** Never park a session the caller has already reported as broken. */
    @Test
    public void doesNotParkAnInvalidSessionOnRecycle() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, LONG_IDLE, SHORT_WAIT)) {
            FakeSession session = (FakeSession) pool.borrow();
            session.valid = false;
            pool.recycle(session);

            assertThat(pool.idleSessions()).isZero();
            assertThat(pool.liveSessions()).isZero();
            assertThat(session.closed).isTrue();
        }
    }

    /** Mid-transfer sessions are discarded, and that must free capacity rather than strand it. */
    @Test
    public void discardFreesCapacity() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 1, LONG_IDLE, SHORT_WAIT)) {
            SftpSession first = pool.borrow();
            pool.discard(first);
            assertThat(pool.liveSessions()).isZero();

            SftpSession second = pool.borrow(); // would block forever if the slot leaked
            assertThat(second).isNotNull();
            pool.recycle(second);
        }
    }

    /** Discarding twice must not corrupt the count and permanently shrink the pool. */
    @Test
    public void doubleDiscardDoesNotStrandCapacity() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 2, LONG_IDLE, SHORT_WAIT)) {
            SftpSession session = pool.borrow();
            pool.discard(session);
            pool.discard(session);

            assertThat(pool.liveSessions()).isZero();
            SftpSession a = pool.borrow();
            SftpSession b = pool.borrow();
            assertThat(a).isNotNull();
            assertThat(b).isNotNull();
        }
    }

    /** At the ceiling a borrow waits, then fails with a message that says what to do about it. */
    @Test
    public void failsWithAnActionableMessageWhenExhausted() {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 1, LONG_IDLE, SHORT_WAIT)) {
            SftpSession held = pool.borrow();

            assertThatThrownBy(pool::borrow)
                    .isInstanceOf(SftpException.class)
                    .hasMessageContaining("timed out")
                    .hasMessageContaining("never returned it");

            pool.recycle(held);
        }
    }

    /** A waiting borrower must be woken by a recycle, not left to time out. */
    @Test(timeOut = 30_000)
    public void waitingBorrowerIsHandedTheRecycledSession() throws Exception {
        CountingProvider provider = new CountingProvider();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (BoundedSftpSessionPool<SftpSession> pool =
                     pool(provider, 1, LONG_IDLE, Duration.ofSeconds(10))) {
            SftpSession held = pool.borrow();
            CountDownLatch borrowing = new CountDownLatch(1);

            var waiter = worker.submit(() -> {
                borrowing.countDown();
                return pool.borrow();
            });

            assertThat(borrowing.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(100); // let it reach the wait
            pool.recycle(held);

            SftpSession handed = (SftpSession) waiter.get(10, TimeUnit.SECONDS);
            assertThat(handed).isSameAs(held);
            assertThat(provider.connects.get())
                    .describedAs("the waiter should get the freed session, not a new handshake")
                    .isEqualTo(1);
        } finally {
            worker.shutdownNow();
        }
    }

    /** Sessions parked longer than maxIdle are closed rather than reused. */
    @Test
    public void evictsSessionsIdleForTooLong() throws Exception {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool =
                     pool(provider, 4, Duration.ofMillis(50), SHORT_WAIT)) {
            FakeSession first = (FakeSession) pool.borrow();
            pool.recycle(first);
            assertThat(pool.idleSessions()).isEqualTo(1);

            Thread.sleep(150); // outlive maxIdle

            SftpSession second = pool.borrow();
            assertThat(second).isNotSameAs(first);
            assertThat(first.closed).describedAs("the stale session must be closed").isTrue();
            assertThat(provider.connects.get()).isEqualTo(2);
            assertThat(pool.liveSessions())
                    .describedAs("evicting must not leave a phantom slot behind")
                    .isEqualTo(1);
        }
    }

    /**
     * The same eviction on request, for an owner whose pools see no traffic of their own — and only
     * of what is parked: a session in use is never closed out from under its borrower.
     */
    @Test
    public void evictsExpiredSessionsOnRequestWithoutWaitingForTraffic() throws Exception {
        CountingProvider provider = new CountingProvider();
        try (BoundedSftpSessionPool<SftpSession> pool =
                     pool(provider, 4, Duration.ofMillis(50), SHORT_WAIT)) {
            FakeSession parked = (FakeSession) pool.borrow();
            FakeSession inUse = (FakeSession) pool.borrow();
            pool.recycle(parked);

            assertThat(pool.evictExpired()).describedAs("nothing is stale yet").isZero();
            Thread.sleep(150); // outlive maxIdle

            assertThat(pool.evictExpired()).isEqualTo(1);
            assertThat(parked.closed).isTrue();
            assertThat(inUse.closed).describedAs("checked out, so not the pool's to close").isFalse();
            assertThat(pool.idleSessions()).isZero();
            assertThat(pool.liveSessions()).describedAs("only the borrowed one remains").isEqualTo(1);

            pool.recycle(inUse);
            assertThat(pool.borrow()).describedAs("still usable afterwards").isSameAs(inUse);
        }
    }

    /** Closing the pool closes what it holds; a borrow afterwards fails rather than reconnecting. */
    @Test
    public void closeReleasesPooledSessionsAndRefusesFurtherBorrows() {
        CountingProvider provider = new CountingProvider();
        BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, LONG_IDLE, SHORT_WAIT);
        FakeSession a = (FakeSession) pool.borrow();
        FakeSession b = (FakeSession) pool.borrow();
        pool.recycle(a);
        pool.recycle(b);

        pool.close();

        assertThat(a.closed).isTrue();
        assertThat(b.closed).isTrue();
        assertThat(pool.idleSessions()).isZero();
        assertThatThrownBy(pool::borrow).isInstanceOf(IllegalStateException.class);
        pool.close(); // idempotent
    }

    /** A failed connect must release the slot it reserved, or the pool shrinks with every outage. */
    @Test
    public void failedConnectDoesNotLeakASlot() {
        CountingProvider provider = new CountingProvider();
        provider.failNext = true;
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 1, LONG_IDLE, SHORT_WAIT)) {
            assertThatThrownBy(pool::borrow).isInstanceOf(SftpException.class);
            assertThat(pool.liveSessions()).isZero();

            SftpSession recovered = pool.borrow(); // the pool must still work afterwards
            assertThat(recovered).isNotNull();
            pool.recycle(recovered);
        }
    }

    /** Under concurrency the ceiling still holds: never more live sessions than maxSize. */
    @Test(timeOut = 60_000)
    public void neverExceedsMaxSizeUnderConcurrency() throws Exception {
        int maxSize = 4;
        int threads = 16;
        CountingProvider provider = new CountingProvider();
        ExecutorService workers = Executors.newFixedThreadPool(threads);
        try (BoundedSftpSessionPool<SftpSession> pool =
                     pool(provider, maxSize, LONG_IDLE, Duration.ofSeconds(20))) {
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                workers.execute(() -> {
                    try {
                        start.await();
                        for (int op = 0; op < 25; op++) {
                            SftpSession session = pool.borrow();
                            try {
                                assertThat(provider.checkedOut.incrementAndGet())
                                        .isLessThanOrEqualTo(maxSize);
                                Thread.sleep(1);
                            } finally {
                                provider.checkedOut.decrementAndGet();
                                pool.recycle(session);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertThat(done.await(50, TimeUnit.SECONDS)).isTrue();

            assertThat(provider.connects.get())
                    .describedAs("400 operations must not open more than maxSize sessions")
                    .isLessThanOrEqualTo(maxSize);
            assertThat(pool.liveSessions()).isLessThanOrEqualTo(maxSize);
        } finally {
            workers.shutdownNow();
        }
    }

    // --- borrowAsync ---

    /**
     * The point of the fast path: a warm pool hands back a session with no scheduling hop at all.
     * The recording executor is what makes that observable — the returned stage is already complete
     * either way, because the fallback runs inline here too.
     */
    @Test
    public void borrowAsyncReusesAnIdleSessionWithoutTouchingTheExecutor() {
        CountingProvider provider = new CountingProvider();
        RecordingExecutor executor = new RecordingExecutor();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, Duration.ofMinutes(1),
                Duration.ofSeconds(1))) {
            pool.recycle(pool.borrow()); // park one

            CompletableFuture<SftpSession> borrowed =
                    pool.borrowAsync(executor).toCompletableFuture();

            assertThat(borrowed).as("a parked session should be handed over immediately").isDone();
            assertThat(borrowed.join()).isNotNull();
            assertThat(executor.tasks.get())
                    .as("reusing an idle session must not go through the executor").isZero();
            assertThat(provider.connects.get()).as("no new connection was needed").isEqualTo(1);
        }
    }

    @Test
    public void borrowAsyncOpensThroughTheExecutorWhenNothingIsIdle() {
        CountingProvider provider = new CountingProvider();
        RecordingExecutor executor = new RecordingExecutor();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, Duration.ofMinutes(1),
                Duration.ofSeconds(1))) {
            SftpSession session = pool.borrowAsync(executor).toCompletableFuture().join();

            assertThat(session).isNotNull();
            assertThat(executor.tasks.get())
                    .as("opening a connection blocks and must be offloaded").isEqualTo(1);
            assertThat(provider.connects.get()).isEqualTo(1);
        }
    }

    /** A session that died while parked must not be handed out, and the retry must not block here. */
    @Test
    public void borrowAsyncReplacesAnIdleSessionThatFailedValidation() {
        CountingProvider provider = new CountingProvider();
        RecordingExecutor executor = new RecordingExecutor();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, Duration.ofMinutes(1),
                Duration.ofSeconds(1))) {
            FakeSession first = (FakeSession) pool.borrow();
            pool.recycle(first);
            first.valid = false;

            SftpSession replacement = pool.borrowAsync(executor).toCompletableFuture().join();

            assertThat(replacement).isNotNull();
            assertThat(replacement).as("the dead session must not be handed out").isNotSameAs(first);
            assertThat(executor.tasks.get())
                    .as("the replacement connect must be offloaded").isEqualTo(1);
            assertThat(provider.connects.get()).isEqualTo(2);
        }
    }

    @Test
    public void borrowAsyncFailsRatherThanThrowsWhenThePoolIsClosed() {
        CountingProvider provider = new CountingProvider();
        RecordingExecutor executor = new RecordingExecutor();
        BoundedSftpSessionPool<SftpSession> pool = pool(provider, 4, Duration.ofMinutes(1), Duration.ofSeconds(1));
        pool.close();

        CompletableFuture<SftpSession> borrowed = pool.borrowAsync(executor).toCompletableFuture();

        assertThat(borrowed)
                .as("a closed pool must fail the stage, not throw at the call site")
                .isCompletedExceptionally();
        assertThat(executor.tasks.get()).isZero();
    }

    /** A pool that overrides nothing still gets a working borrowAsync, offloaded to the executor. */
    @Test
    public void theDefaultBorrowAsyncOffloadsTheBlockingBorrow() {
        RecordingExecutor executor = new RecordingExecutor();
        FakeSession only = new FakeSession();
        SftpSessionPool<SftpSession> minimal = new SftpSessionPool<>() {
            @Override
            public SftpSession borrow() {
                return only;
            }

            @Override
            public void recycle(SftpSession session) {
            }

            @Override
            public void discard(SftpSession session) {
            }

            @Override
            public void close() {
            }
        };

        SftpSession session = minimal.borrowAsync(executor).toCompletableFuture().join();

        assertThat(session).isSameAs(only);
        assertThat(executor.tasks.get()).isEqualTo(1);
    }

    /** Runs inline so tests stay deterministic; the count is the only thing under assertion. */
    private static final class RecordingExecutor implements java.util.concurrent.Executor {

        final AtomicInteger tasks = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            tasks.incrementAndGet();
            command.run();
        }
    }

    private static BoundedSftpSessionPool<SftpSession> pool(SftpProvider provider, int maxSize, Duration maxIdle,
                                               Duration acquireTimeout) {
        return BoundedSftpSessionPool.blocking(provider, config(), maxSize, maxIdle, acquireTimeout);
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

        final AtomicInteger connects = new AtomicInteger();
        final AtomicInteger checkedOut = new AtomicInteger();
        volatile boolean failNext;

        @Override
        public String name() {
            return "counting";
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).build();
        }

        @Override
        public SftpSession connect(SftpConnectionConfig config) {
            if (failNext) {
                failNext = false;
                throw new SftpException("simulated connect failure");
            }
            connects.incrementAndGet();
            return new FakeSession();
        }
    }

    private static final class FakeSession implements SftpSession {

        volatile boolean valid = true;
        volatile boolean closed;

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
            return valid && !closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
