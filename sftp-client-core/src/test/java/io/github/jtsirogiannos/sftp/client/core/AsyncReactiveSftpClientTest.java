package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * The reactive client over a thread-free provider.
 *
 * <p>Which API the client reaches for is not asserted here, because it cannot vary: the pool is
 * typed to {@link AsyncSftpSession} and the blocking half does not exist on it, so the compiler
 * settles the question and these tests are free to be about behaviour.
 */
public class AsyncReactiveSftpClientTest {

    private static final byte[] CONTENT = "async payload".getBytes(StandardCharsets.UTF_8);

    @Test
    public void metadataOperationsGoStraightToTheProvider() throws Exception {
        RecordingAsyncSession session = new RecordingAsyncSession();
        try (AsyncReactiveSftpClient client = clientFor(session)) {
            RemoteFile stat = client.stat("/f").toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertEquals(stat.name(), "f");
            assertEquals(List.copyOf(session.calls), List.of("statAsync"));
        }
    }

    @Test
    public void downloadStreamsTheProvidersOwnPublisher() throws Exception {
        RecordingAsyncSession session = new RecordingAsyncSession();
        try (AsyncReactiveSftpClient client = clientFor(session)) {
            byte[] received = drain(client.download("/f"));

            assertEquals(new String(received, StandardCharsets.UTF_8),
                new String(CONTENT, StandardCharsets.UTF_8));
            assertTrue(session.calls.contains("readAsync"));
        }
    }

    @Test
    public void uploadHandsThePublisherToTheProvider() throws Exception {
        RecordingAsyncSession session = new RecordingAsyncSession();
        try (AsyncReactiveSftpClient client = clientFor(session)) {
            client.upload("/f", publisherOf(CONTENT)).toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertTrue(session.calls.contains("writeAsync"));
            assertEquals(new String(session.written.toByteArray(), StandardCharsets.UTF_8),
                new String(CONTENT, StandardCharsets.UTF_8));
        }
    }

    /**
     * The whole point of the async path: with a warm pool nothing is offloaded, so the operation
     * settles on the thread that asked for it and no worker is involved at all.
     */
    @Test
    public void aWarmPoolCompletesWithoutTouchingTheWorker() throws Exception {
        RecordingAsyncSession session = new RecordingAsyncSession();
        java.util.concurrent.atomic.AtomicInteger offloaded =
            new java.util.concurrent.atomic.AtomicInteger();
        Executor countingWorker = command -> {
            offloaded.incrementAndGet();
            command.run();
        };
        try (AsyncReactiveSftpClient client = new AsyncReactiveSftpClient(
                new FixedAsyncProvider(), new InlinePool(session), countingWorker)) {
            client.stat("/f").toCompletableFuture().get(5, TimeUnit.SECONDS);

            assertEquals(offloaded.get(), 0,
                "an async operation over a warm pool must not occupy a worker thread");
        }
    }

    /**
     * A provider's failure must arrive as {@link SftpException} whatever it actually threw, so the
     * choice of provider does not leak through the caller's catch block. The provider fails with an
     * {@link IllegalStateException} on purpose — an SftpException would arrive as one regardless and
     * prove nothing.
     */
    @Test
    public void failuresAreNormalisedToSftpException() {
        RecordingAsyncSession session = new RecordingAsyncSession();
        session.failWith = new IllegalStateException("backend blew up");
        try (AsyncReactiveSftpClient client = clientFor(session)) {
            Exception thrown = expectThrows(Exception.class,
                () -> client.stat("/f").toCompletableFuture().get(5, TimeUnit.SECONDS));

            assertEquals(thrown.getCause().getClass(), SftpException.class);
            assertEquals(thrown.getCause().getCause().getClass(), IllegalStateException.class);
        }
    }

    /** A failed borrow must fail the operation rather than stall it or leak a session. */
    @Test
    public void aFailedBorrowFailsTheOperation() {
        try (AsyncReactiveSftpClient client = new AsyncReactiveSftpClient(new FixedAsyncProvider(),
                new FailingPool(), Runnable::run)) {
            Exception thrown = expectThrows(Exception.class,
                () -> client.stat("/f").toCompletableFuture().get(5, TimeUnit.SECONDS));

            assertEquals(thrown.getCause().getClass(), SftpException.class);
        }
    }

    // --- fixtures ---

    private static AsyncReactiveSftpClient clientFor(AsyncSftpSession session) {
        return new AsyncReactiveSftpClient(new FixedAsyncProvider(), new InlinePool(session),
            Runnable::run);
    }

    private static byte[] drain(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<byte[]> done = new CompletableFuture<>();
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                sink.writeBytes(chunk);
            }

            @Override
            public void onError(Throwable throwable) {
                done.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                done.complete(sink.toByteArray());
            }
        });
        return done.get(5, TimeUnit.SECONDS);
    }

    private static Flow.Publisher<ByteBuffer> publisherOf(byte[] bytes) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private boolean sent;

            @Override
            public void request(long n) {
                if (!sent && n > 0) {
                    sent = true;
                    subscriber.onNext(ByteBuffer.wrap(bytes));
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
            }
        });
    }

    private static final class FixedAsyncProvider implements AsyncSftpProvider {
        @Override
        public String name() {
            return "fixed-async";
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).asyncStreaming(true).build();
        }

        @Override
        public SftpSession connect(SftpConnectionConfig config) {
            throw new UnsupportedOperationException("the pool in this test is pre-loaded");
        }

        @Override
        public AsyncSftpSession connectAsync(SftpConnectionConfig config) {
            throw new UnsupportedOperationException("the pool in this test is pre-loaded");
        }
    }

    /** Models a warm pool: the borrow completes inline, on the caller's thread. */
    private record InlinePool(AsyncSftpSession session) implements SftpSessionPool<AsyncSftpSession> {
        @Override
        public AsyncSftpSession borrow() {
            return session;
        }

        @Override
        public CompletionStage<AsyncSftpSession> borrowAsync(Executor executor) {
            return CompletableFuture.completedFuture(session);
        }

        @Override
        public void recycle(AsyncSftpSession s) {
        }

        @Override
        public void discard(AsyncSftpSession s) {
        }

        @Override
        public void close() {
        }
    }

    private static final class FailingPool implements SftpSessionPool<AsyncSftpSession> {
        @Override
        public AsyncSftpSession borrow() {
            throw new SftpException("pool exhausted");
        }

        @Override
        public void recycle(AsyncSftpSession s) {
        }

        @Override
        public void discard(AsyncSftpSession s) {
        }

        @Override
        public void close() {
        }
    }

    /** The whole async contract, which is all a fake needs to implement. */
    private static final class RecordingAsyncSession implements AsyncSftpSession {

        final List<String> calls = new CopyOnWriteArrayList<>();
        final ByteArrayOutputStream written = new ByteArrayOutputStream();
        RuntimeException failWith;

        static RemoteFile file(String path) {
            String name = path.substring(path.lastIndexOf('/') + 1);
            return new RemoteFile(name, path, CONTENT.length, false, false, Instant.EPOCH, 0644);
        }

        @Override
        public CompletionStage<List<RemoteFile>> listAsync(String path) {
            calls.add("listAsync");
            return CompletableFuture.completedFuture(List.of(file(path)));
        }

        @Override
        public CompletionStage<RemoteFile> statAsync(String path) {
            calls.add("statAsync");
            if (failWith == null) {
                return CompletableFuture.completedFuture(file(path));
            }
            // Failed through a dependent stage, so the failure arrives wrapped in a
            // CompletionException exactly as a real composing provider's would.
            return CompletableFuture.<RemoteFile>failedFuture(failWith).thenApply(value -> value);
        }

        @Override
        public CompletionStage<Boolean> existsAsync(String path) {
            calls.add("existsAsync");
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public CompletionStage<Void> mkdirAsync(String path) {
            calls.add("mkdirAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> renameAsync(String from, String to) {
            calls.add("renameAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> removeAsync(String path) {
            calls.add("removeAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<RemoteFile> lstatAsync(String path) {
            calls.add("lstatAsync");
            return CompletableFuture.completedFuture(file(path));
        }

        @Override
        public CompletionStage<String> realpathAsync(String path) {
            calls.add("realpathAsync");
            return CompletableFuture.completedFuture(path);
        }

        @Override
        public CompletionStage<Void> symlinkAsync(String linkPath, String targetPath) {
            calls.add("symlinkAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<String> readlinkAsync(String linkPath) {
            calls.add("readlinkAsync");
            return CompletableFuture.completedFuture(linkPath);
        }

        @Override
        public CompletionStage<Void> hardlinkAsync(String linkPath, String targetPath) {
            calls.add("hardlinkAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<Void> setAttributesAsync(String path,
                io.github.jtsirogiannos.sftp.client.api.AttributeUpdate update) {
            calls.add("setAttributesAsync");
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<io.github.jtsirogiannos.sftp.client.api.FilesystemStats> statvfsAsync(
                String path) {
            calls.add("statvfsAsync");
            return CompletableFuture.failedFuture(new UnsupportedOperationException("no statvfs"));
        }

        @Override
        public Flow.Publisher<ByteBuffer> readAsync(String path, long offset) {
            calls.add("readAsync");
            return publisherOf(CONTENT);
        }

        @Override
        public CompletionStage<Void> writeAsync(String path, Flow.Publisher<ByteBuffer> data,
                SftpSession.WriteMode mode) {
            calls.add("writeAsync");
            CompletableFuture<Void> result = new CompletableFuture<>();
            data.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    written.writeBytes(chunk);
                }

                @Override
                public void onError(Throwable throwable) {
                    result.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    result.complete(null);
                }
            });
            return result;
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(3).asyncStreaming(true).build();
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
