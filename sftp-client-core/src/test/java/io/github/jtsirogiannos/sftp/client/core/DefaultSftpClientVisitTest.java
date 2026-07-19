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
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import io.github.jtsirogiannos.sftp.client.api.SftpException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a streamed listing costs the pool.
 *
 * <p>Walking to the end returns the session; stopping early cannot, because entries the server has
 * already queued are still on the wire and the next borrower would read them as its own. That is the
 * same trade an abandoned download makes, and it is invisible from the outside — which is why it is
 * asserted here rather than left to the conformance suite, where pool state is not observable.
 */
public class DefaultSftpClientVisitTest {

    private static final Duration LONG_IDLE = Duration.ofMinutes(5);
    private static final Duration ACQUIRE = Duration.ofSeconds(5);

    @Test
    public void walkingToTheEndReturnsTheSessionToThePool() {
        ListingProvider provider = new ListingProvider(10);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            AtomicInteger seen = new AtomicInteger();
            client.visit("/upload", entry -> {
                seen.incrementAndGet();
                return true;
            });

            assertThat(seen.get()).isEqualTo(10);
            assertThat(pool.idleSessions())
                    .describedAs("a completed walk leaves nothing on the wire, so the session is reusable")
                    .isEqualTo(1);
            assertThat(pool.liveSessions()).isEqualTo(1);
        }
    }

    @Test
    public void stoppingEarlyDiscardsTheSessionRatherThanPoolingIt() {
        ListingProvider provider = new ListingProvider(1000);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            AtomicInteger seen = new AtomicInteger();
            client.visit("/upload", entry -> {
                seen.incrementAndGet();
                return false;
            });

            assertThat(seen.get()).describedAs("the visitor asked to stop after one entry").isEqualTo(1);
            assertThat(pool.idleSessions())
                    .describedAs("a half-read listing must never be parked for the next caller")
                    .isZero();
            assertThat(pool.liveSessions()).isZero();
            assertThat(provider.lastSession.closed).isTrue();
        }
    }

    /** The whole point of the API: don't page a huge directory to find one file. */
    @Test
    public void findFirstStopsAtTheMatchInsteadOfWalkingOn() {
        ListingProvider provider = new ListingProvider(1000);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            var match = client.findFirst("/upload", entry -> entry.name().equals("file-2.txt"));

            assertThat(match).isPresent();
            assertThat(provider.lastSession.entriesVisited.get())
                    .describedAs("walked past the match instead of stopping")
                    .isEqualTo(3);
        }
    }

    /** Filtering keeps only matches, rather than materialising the directory and filtering after. */
    @Test
    public void filteredListCollectsOnlyMatches() {
        ListingProvider provider = new ListingProvider(100);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            List<RemoteFile> matches = client.list("/upload", f -> f.name().endsWith("-7.txt"));

            assertThat(matches).extracting(RemoteFile::name).containsExactly("file-7.txt");
            assertThat(provider.lastSession.entriesVisited.get())
                    .describedAs("a filter has to see every entry, but should keep only the matches")
                    .isEqualTo(100);
        }
    }

    // --- unit of work ---

    /**
     * The measured reason this exists: the same sequence of calls costs one borrow inside a block
     * and one per operation outside it.
     */
    @Test
    public void withSessionBorrowsOnceForTheWholeBlock() {
        ListingProvider provider = new ListingProvider(10);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            CountingPool counting = new CountingPool(pool);
            DefaultSftpClient client = new DefaultSftpClient(provider, counting);

            counting.borrows.set(0);
            client.visit("/upload", entry -> true);
            client.visit("/upload", entry -> true);
            client.visit("/upload", entry -> true);
            int separately = counting.borrows.get();

            counting.borrows.set(0);
            client.withSessionWithoutResult(ops -> {
                ops.list("/upload");
                ops.list("/upload");
                ops.list("/upload");
            });

            assertThat(separately).describedAs("one borrow per call outside a block").isEqualTo(3);
            assertThat(counting.borrows.get())
                    .describedAs("a block must borrow exactly once however many calls it makes")
                    .isEqualTo(1);
        }
    }

    @Test
    public void withSessionReturnsAValueAndRecyclesTheSession() {
        ListingProvider provider = new ListingProvider(5);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            int count = client.withSession(ops -> ops.list("/upload").size());

            assertThat(count).isEqualTo(5);
            assertThat(pool.idleSessions())
                    .describedAs("a block that completes should hand its session back")
                    .isEqualTo(1);
        }
    }

    /** A block that throws leaves the session in an unknown state, so it must not be pooled. */
    @Test
    public void aFailingBlockDiscardsTheSession() {
        ListingProvider provider = new ListingProvider(5);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            assertThatThrownBy(() -> client.withSessionWithoutResult(ops -> {
                ops.list("/upload");
                throw new IllegalStateException("boom");
            })).isInstanceOf(SftpException.class);

            assertThat(pool.idleSessions())
                    .describedAs("a session whose last operation blew up must never be reused")
                    .isZero();
            assertThat(pool.liveSessions()).isZero();
        }
    }

    /** Nothing is atomic: work done before a failure stays done. */
    @Test
    public void withSessionIsNotATransaction() {
        ListingProvider provider = new ListingProvider(0);
        try (BoundedSftpSessionPool<SftpSession> pool = pool(provider)) {
            DefaultSftpClient client = new DefaultSftpClient(provider, pool);

            assertThatThrownBy(() -> client.withSessionWithoutResult(ops -> {
                ops.mkdir("/upload/created");
                throw new IllegalStateException("failed after the mkdir");
            })).isInstanceOf(SftpException.class);

            assertThat(provider.lastSession.directoriesCreated)
                    .describedAs("the earlier operation is not rolled back — SFTP has no transactions")
                    .containsExactly("/upload/created");
        }
    }

    /** Counts borrows so the block's saving is measured rather than assumed. */
    private static final class CountingPool implements io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool<SftpSession> {
        final AtomicInteger borrows = new AtomicInteger();
        private final io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool<SftpSession> delegate;

        CountingPool(io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool<SftpSession> delegate) {
            this.delegate = delegate;
        }

        @Override
        public SftpSession borrow() {
            borrows.incrementAndGet();
            return delegate.borrow();
        }

        @Override
        public void recycle(SftpSession session) {
            delegate.recycle(session);
        }

        @Override
        public void discard(SftpSession session) {
            delegate.discard(session);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    private static BoundedSftpSessionPool<SftpSession> pool(SftpProvider provider) {
        return BoundedSftpSessionPool.blocking(provider, config(), 2, LONG_IDLE, ACQUIRE);
    }

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
                .host("localhost")
                .username("test")
                .password("secret".toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
    }

    private static final class ListingProvider implements SftpProvider {

        private final int entryCount;
        volatile ListingSession lastSession;

        ListingProvider(int entryCount) {
            this.entryCount = entryCount;
        }

        @Override
        public String name() {
            return "listing-fake";
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).build();
        }

        @Override
        public SftpSession connect(SftpConnectionConfig config) {
            lastSession = new ListingSession(entryCount);
            return lastSession;
        }
    }

    /** Counts how far the walk actually got, which is the thing under test. */
    private static final class ListingSession implements SftpSession {

        private final int entryCount;
        final AtomicInteger entriesVisited = new AtomicInteger();
        volatile boolean closed;

        ListingSession(int entryCount) {
            this.entryCount = entryCount;
        }

        @Override
        public void list(String path, Predicate<RemoteFile> visitor) {
            for (int i = 0; i < entryCount; i++) {
                entriesVisited.incrementAndGet();
                RemoteFile entry = new RemoteFile("file-" + i + ".txt", path + "/file-" + i + ".txt",
                        1, false, false, Instant.EPOCH, 0644);
                if (!visitor.test(entry)) {
                    return; // provider closes its handle here
                }
            }
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
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public RemoteFile stat(String path) {
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
        public boolean exists(String path) {
            throw new UnsupportedOperationException();
        }

        final List<String> directoriesCreated = new java.util.ArrayList<>();

        @Override
        public void mkdir(String path) {
            directoriesCreated.add(path);
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
    }
}
