package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * How {@code ReactiveSftpClient.uploadAtomically} composes failures, driven against a stub rather
 * than a server because the interesting cases are hard to provoke over a real connection.
 *
 * <p>Specifically: what a caller sees when the upload fails <em>and</em> the scratch-file cleanup
 * fails too. The integration suite covers the happy paths and the ordinary failure, but it cannot
 * easily make {@code remove} throw, and that is the one path where the {@link
 * java.util.concurrent.CompletionException} unwrapping is observable — attach a suppressed exception
 * to the wrapper instead of the cause and it never reaches the caller, because {@code get()} reports
 * the cause.
 */
public class ReactiveAtomicUploadFailureTest {

    @Test
    public void surfacesTheOriginalFailureNotTheCleanupFailure() {
        StubReactiveClient client = new StubReactiveClient();
        client.uploadFailure = new IllegalStateException("the upload broke");
        client.removeFailure = new SftpException("could not delete the scratch file");

        Exception thrown = expectThrows(Exception.class, () -> client
            .uploadAtomically("/upload/f.bin", empty())
            .toCompletableFuture().get(5, TimeUnit.SECONDS));

        Throwable surfaced = thrown.getCause();
        assertEquals(surfaced.getClass(), IllegalStateException.class,
            "the caller must get the failure that actually broke the upload");
        assertEquals(surfaced.getMessage(), "the upload broke");
        assertEquals(surfaced.getSuppressed().length, 1,
            "the cleanup failure must be attached to the exception the caller receives, not to a "
                + "CompletionException wrapper that get() strips off");
        assertEquals(surfaced.getSuppressed()[0].getClass(), SftpException.class);
    }

    @Test
    public void removesTheScratchFileAndNotTheDestination() {
        StubReactiveClient client = new StubReactiveClient();
        client.uploadFailure = new IllegalStateException("the upload broke");

        expectThrows(Exception.class, () -> client.uploadAtomically("/upload/f.bin", empty())
            .toCompletableFuture().get(5, TimeUnit.SECONDS));

        assertEquals(client.removed.size(), 1);
        String removed = client.removed.get(0);
        assertTrue(removed.startsWith("/upload/.f.bin."), "unexpected scratch path: " + removed);
        assertTrue(removed.endsWith(".part"), "unexpected scratch path: " + removed);
    }

    private static Flow.Publisher<ByteBuffer> empty() {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
            }
        });
    }

    /** Only the four operations the default method composes actually do anything. */
    private static final class StubReactiveClient implements ReactiveSftpClient {

        final List<String> removed = new CopyOnWriteArrayList<>();
        RuntimeException uploadFailure;
        RuntimeException removeFailure;

        @Override
        public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data) {
            return uploadFailure == null
                ? CompletableFuture.completedFuture(null)
                // Through a dependent stage, so the failure arrives wrapped in a
                // CompletionException exactly as a real client's would.
                : CompletableFuture.<Void>failedFuture(uploadFailure).thenApply(v -> v);
        }

        @Override
        public CompletionStage<Void> remove(String path) {
            removed.add(path);
            return removeFailure == null
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.<Void>failedFuture(removeFailure).thenApply(v -> v);
        }

        @Override
        public CompletionStage<Boolean> exists(String path) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletionStage<Void> rename(String from, String to) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public SftpCapabilities capabilities() {
            return SftpCapabilities.builder().maxSftpVersion(6).posixRename(true).build();
        }

        @Override
        public CompletionStage<List<RemoteFile>> list(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<RemoteFile> stat(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> mkdir(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Flow.Publisher<ByteBuffer> download(String remotePath) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
