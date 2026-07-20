package io.github.jtsirogiannos.sftp.client.pool;

import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every event provoked by real pool behaviour rather than by invoking the listener, because the
 * claim under test is "the pool reports what happened", not "the interface has methods".
 */
public class PoolListenerTest {

    private static final Duration LONG_IDLE = Duration.ofMinutes(5);

    @Test
    public void reportsAWaitWithItsDurationWhenThePoolIsAtCapacity() throws Exception {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool =
                pool(1, LONG_IDLE, Duration.ofSeconds(5))) {
            pool.addListener(events);
            SftpSession held = pool.borrow();

            // A second borrower queues behind the single slot; release it after a measurable delay.
            ExecutorService other = Executors.newSingleThreadExecutor();
            try {
                CountDownLatch borrowing = new CountDownLatch(1);
                var second = other.submit(() -> {
                    borrowing.countDown();
                    SftpSession s = pool.borrow();
                    pool.recycle(s);
                    return s;
                });
                borrowing.await();
                Thread.sleep(80);
                pool.recycle(held);
                second.get(5, TimeUnit.SECONDS);
            } finally {
                other.shutdownNow();
            }

            assertThat(events.waits).hasSize(1);
            assertThat(events.waits.get(0).toMillis())
                .describedAs("the wait began ~80ms before the slot freed")
                .isGreaterThan(40);
            assertThat(events.timeouts.get()).isZero();
        }
    }

    @Test
    public void reportsATimeoutWhenNoSlotEverFrees() {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool =
                pool(1, LONG_IDLE, Duration.ofMillis(60))) {
            pool.addListener(events);
            SftpSession held = pool.borrow();

            assertThatThrownBy(pool::borrow).isInstanceOf(SftpException.class);

            assertThat(events.timeouts.get()).isEqualTo(1);
            assertThat(events.waits)
                .describedAs("the time spent waiting is reported even when it ends in a timeout")
                .hasSize(1);
            pool.recycle(held);
        }
    }

    @Test
    public void reportsADiscardOnceAndNotForTheNullConvenience() {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(2, LONG_IDLE, Duration.ofSeconds(1))) {
            pool.addListener(events);
            pool.discard(pool.borrow());
            pool.discard(null);

            assertThat(events.discards.get())
                .describedAs("a null discard is bookkeeping, not churn")
                .isEqualTo(1);
        }
    }

    @Test
    public void reportsAnEvictionPerExpiredSession() throws Exception {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool =
                pool(3, Duration.ofMillis(40), Duration.ofSeconds(1))) {
            pool.addListener(events);
            SftpSession a = pool.borrow();
            SftpSession b = pool.borrow();
            pool.recycle(a);
            pool.recycle(b);
            Thread.sleep(90);

            pool.recycle(pool.borrow()); // traffic triggers the sweep

            assertThat(events.evictions.get())
                .describedAs("both parked sessions were past the idle limit")
                .isEqualTo(2);
        }
    }

    @Test
    public void reportsAValidationFailureWhenAParkedSessionDies() {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(2, LONG_IDLE, Duration.ofSeconds(1))) {
            pool.addListener(events);
            FakeSession session = (FakeSession) pool.borrow();
            pool.recycle(session);
            session.valid = false;

            SftpSession replacement = pool.borrow();

            assertThat(replacement).isNotSameAs(session);
            assertThat(events.validationFailures.get()).isEqualTo(1);
            assertThat(events.discards.get())
                .describedAs("a death while parked is not a caller's discard")
                .isZero();
        }
    }

    @Test
    public void aThrowingListenerDoesNotBreakTheOperationOrSilenceOthers() {
        RecordingListener events = new RecordingListener();
        try (BoundedSftpSessionPool<SftpSession> pool = pool(2, LONG_IDLE, Duration.ofSeconds(1))) {
            pool.addListener(new SftpSessionPoolListener() {
                @Override
                public void sessionDiscarded() {
                    throw new IllegalStateException("a buggy listener");
                }
            });
            pool.addListener(events);

            pool.discard(pool.borrow()); // must not throw

            assertThat(events.discards.get())
                .describedAs("a listener that throws must not silence the ones after it")
                .isEqualTo(1);
        }
    }

    /**
     * The deadlock the contract exists to prevent. The timeout listener takes a lock of its own; a
     * second thread holds that lock and calls into the pool. If the pool notified while holding its
     * internal lock, the two would deadlock — a lock-order inversion on the exhaustion path, where
     * listeners are most likely to page someone.
     */
    @Test
    public void timeoutListenersFireOutsideThePoolLock() throws Exception {
        Object listenerLock = new Object();
        CountDownLatch inListener = new CountDownLatch(1);
        CountDownLatch poolTouched = new CountDownLatch(1);

        try (BoundedSftpSessionPool<SftpSession> pool =
                pool(1, LONG_IDLE, Duration.ofMillis(80))) {
            pool.addListener(new SftpSessionPoolListener() {
                @Override
                public void borrowTimedOut() {
                    inListener.countDown();
                    synchronized (listenerLock) {
                        // Reached only once the other thread has exited listenerLock — and that
                        // thread first calls into the pool. Deadlock if we hold the pool lock here.
                    }
                }
            });
            SftpSession held = pool.borrow();

            ExecutorService other = Executors.newSingleThreadExecutor();
            try {
                other.submit(() -> {
                    inListener.await();
                    synchronized (listenerLock) {
                        pool.liveSessions();   // takes the pool lock
                        poolTouched.countDown();
                    }
                    return null;
                });

                // Runs the timeout path on this thread. Holding the pool lock through the listener
                // would deadlock against the submit above, and the await below would expire.
                ExecutorService borrower = Executors.newSingleThreadExecutor();
                try {
                    var timedOut = borrower.submit(() -> {
                        try {
                            pool.borrow();
                            return false;
                        } catch (SftpException expected) {
                            return true;
                        }
                    });
                    assertThat(poolTouched.await(5, TimeUnit.SECONDS))
                        .describedAs("a listener holding the pool lock deadlocks this")
                        .isTrue();
                    assertThat(timedOut.get(5, TimeUnit.SECONDS)).isTrue();
                } finally {
                    borrower.shutdownNow();
                }
            } finally {
                other.shutdownNow();
            }
            pool.recycle(held);
        }
    }

    // --- fixtures ---

    private static BoundedSftpSessionPool<SftpSession> pool(int maxSize, Duration maxIdle,
            Duration acquireTimeout) {
        SftpProvider provider = new SftpProvider() {
            @Override
            public String name() {
                return "listener-test";
            }

            @Override
            public SftpCapabilities capabilities() {
                return SftpCapabilities.builder().maxSftpVersion(3).build();
            }

            @Override
            public SftpSession connect(SftpConnectionConfig config) {
                return new FakeSession();
            }
        };
        SftpConnectionConfig config = SftpConnectionConfig.builder()
            .host("localhost").username("test").password("secret".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY).build();
        return BoundedSftpSessionPool.blocking(provider, config, maxSize, maxIdle, acquireTimeout);
    }

    private static final class RecordingListener implements SftpSessionPoolListener {
        final List<Duration> waits = new CopyOnWriteArrayList<>();
        final java.util.concurrent.atomic.AtomicInteger timeouts = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger discards = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger evictions = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger validationFailures = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public void borrowWaited(Duration waited) {
            waits.add(waited);
        }

        @Override
        public void borrowTimedOut() {
            timeouts.incrementAndGet();
        }

        @Override
        public void sessionDiscarded() {
            discards.incrementAndGet();
        }

        @Override
        public void sessionEvicted() {
            evictions.incrementAndGet();
        }

        @Override
        public void sessionValidationFailed() {
            validationFailures.incrementAndGet();
        }
    }

    private static final class FakeSession implements SftpSession {
        volatile boolean valid = true;

        @Override public void list(String path, Predicate<RemoteFile> visitor) { }
        @Override public RemoteFile stat(String path) { throw new UnsupportedOperationException(); }
        @Override public RemoteFile lstat(String path) { throw new UnsupportedOperationException(); }
        @Override public boolean exists(String path) { return false; }
        @Override public String realpath(String path) { return path; }
        @Override public void symlink(String l, String t) { }
        @Override public String readlink(String l) { return l; }
        @Override public void hardlink(String l, String t) { }
        @Override public void setAttributes(String p, AttributeUpdate u) { }
        @Override public FilesystemStats statvfs(String p) { throw new UnsupportedOperationException(); }
        @Override public void mkdir(String path) { }
        @Override public void rename(String from, String to) { }
        @Override public void remove(String path) { }
        @Override public InputStream openRead(String path, long offset) { throw new UnsupportedOperationException(); }
        @Override public OutputStream openWrite(String path, WriteMode mode) { throw new UnsupportedOperationException(); }
        @Override public SftpCapabilities capabilities() { return SftpCapabilities.builder().maxSftpVersion(3).build(); }
        @Override public boolean isValid() { return valid; }
        @Override public void close() { }
    }
}
