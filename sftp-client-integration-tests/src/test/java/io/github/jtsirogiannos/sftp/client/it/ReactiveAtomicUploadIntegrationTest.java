package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Publish-by-rename through the reactive facade.
 *
 * <p>The same shape as the blocking suite, including the control test — {@link
 * #plainReactiveUploadIsVisibleWhileStillPartial} exists so that {@link
 * #aReaderNeverSeesAPartialFile} means something. An "is the file correct afterwards" assertion
 * passes for both methods and would test nothing.
 *
 * <p>Observation goes through a <em>separate blocking client</em> with its own pool. Peeking with the
 * reactive client under test would contend for the very worker executor driving the transfer, and a
 * test that deadlocks under load is worse than no test.
 */
class ReactiveAtomicUploadIntegrationTest {

    private static EmbeddedSftpServer server;
    private static ExecutorService worker;
    private static final AtomicInteger UNIQUE = new AtomicInteger();

    private static final byte[] OLD = "the previous version\n".getBytes(StandardCharsets.UTF_8);
    private static final int CHUNK = 8 * 1024;
    private static final int CHUNKS = 64;
    private static final byte[] NEW = newContent();

    @BeforeAll
    static void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        worker = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void stopServer() {
        worker.shutdownNow();
        server.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void publishesToAPathThatDoesNotExistYet(String provider) throws Exception {
        String path = uniquePath("rx-create");
        try (ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
             SftpClient observer = SftpClients.blocking(provider, server.config())) {

            transfer(client, path, null, true);

            assertThat(read(observer, path)).isEqualTo(NEW);
            assertThat(partFiles(observer)).describedAs("the scratch file must not survive").isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina"})
    void aReaderNeverSeesAPartialFile(String provider) throws Exception {
        String path = uniquePath("rx-replace");
        try (ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
             SftpClient observer = SftpClients.blocking(provider, server.config())) {
            observer.upload(path, new ByteArrayInputStream(OLD));
            AtomicReference<byte[]> midTransfer = new AtomicReference<>();

            transfer(client, path, () -> midTransfer.set(read(observer, path)), true);

            assertThat(midTransfer.get())
                .describedAs("mid-upload the destination must still hold the previous version")
                .isEqualTo(OLD);
            assertThat(read(observer, path)).isEqualTo(NEW);
        }
    }

    /** The control: the same observation against ordinary upload must catch a truncated file. */
    @ParameterizedTest
    @ValueSource(strings = {"mina"})
    void plainReactiveUploadIsVisibleWhileStillPartial(String provider) throws Exception {
        String path = uniquePath("rx-nonatomic");
        try (ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
             SftpClient observer = SftpClients.blocking(provider, server.config())) {
            observer.upload(path, new ByteArrayInputStream(OLD));
            AtomicReference<byte[]> midTransfer = new AtomicReference<>();

            transfer(client, path, () -> midTransfer.set(read(observer, path)), false);

            assertThat(midTransfer.get())
                .describedAs("if this ever equals OLD or NEW the observation is not landing "
                    + "mid-transfer, and the atomic test above proves nothing")
                .isNotEqualTo(OLD)
                .isNotEqualTo(NEW);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"jsch"})
    void refusesToReplaceWhenTheServerCannotRenameAtomically(String provider) {
        String path = uniquePath("rx-v3replace");
        try (ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
             SftpClient observer = SftpClients.blocking(provider, server.config())) {
            observer.upload(path, new ByteArrayInputStream(OLD));
            assertThat(client.capabilities().posixRename()).isFalse();

            try (SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>(worker, 4)) {
                CompletableFuture<Void> published =
                    client.uploadAtomically(path, source).toCompletableFuture();

                assertThatThrownBy(() -> published.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .cause().isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("posixRename");
            }

            assertThat(read(observer, path))
                .describedAs("a refused publish must leave the destination untouched").isEqualTo(OLD);
            assertThat(partFiles(observer)).isZero();
        }
    }

    /** A source that errors part-way must clean up the scratch file and publish nothing. */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void cleansUpTheScratchFileWhenTheSourceFails(String provider) {
        String path = uniquePath("rx-failure");
        try (ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
             SftpClient observer = SftpClients.blocking(provider, server.config())) {

            try (SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>(worker, 4)) {
                CompletableFuture<Void> published =
                    client.uploadAtomically(path, source).toCompletableFuture();
                awaitSubscriber(source);
                source.submit(ByteBuffer.wrap(new byte[CHUNK]));
                source.closeExceptionally(new IllegalStateException("simulated source failure"));

                assertThatThrownBy(() -> published.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
            }

            assertThat(observer.exists(path)).describedAs("nothing should have been published")
                .isFalse();
            assertThat(partFiles(observer))
                .describedAs("the .part file must be cleaned up after a failed upload").isZero();
        }
    }

    // --- helpers ---

    /**
     * Feeds {@link #NEW} through a demand-paced publisher, running {@code midway} once the transfer
     * is half fed. {@code SubmissionPublisher.submit} blocks when its buffer is full, so the halfway
     * point really is halfway through the wire transfer rather than halfway through a memory copy.
     */
    private static void transfer(ReactiveSftpClient client, String path, Runnable midway,
            boolean atomic) throws Exception {
        try (SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>(worker, 4)) {
            CompletableFuture<Void> published = (atomic
                ? client.uploadAtomically(path, source)
                : client.upload(path, source)).toCompletableFuture();

            awaitSubscriber(source);
            for (int i = 0; i < CHUNKS; i++) {
                source.submit(ByteBuffer.wrap(NEW, i * CHUNK, CHUNK).slice());
                if (midway != null && i == CHUNKS / 2) {
                    midway.run();
                }
            }
            source.close();
            published.get(30, TimeUnit.SECONDS);
        }
    }

    /** SubmissionPublisher discards anything submitted before a subscriber attaches. */
    private static void awaitSubscriber(SubmissionPublisher<ByteBuffer> source) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!source.hasSubscribers() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(source.hasSubscribers()).describedAs("upload never subscribed").isTrue();
    }

    private static byte[] read(SftpClient client, String path) {
        if (!client.exists(path)) {
            return new byte[0];
        }
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        client.download(path, sink);
        return sink.toByteArray();
    }

    private static long partFiles(SftpClient client) {
        return client.list("/upload").stream().filter(f -> f.name().endsWith(".part")).count();
    }

    private static String uniquePath(String tag) {
        return "/upload/atomic-" + tag + "-" + UNIQUE.incrementAndGet() + ".bin";
    }

    private static byte[] newContent() {
        byte[] content = new byte[CHUNK * CHUNKS];
        Arrays.fill(content, (byte) 'B');
        return content;
    }
}
