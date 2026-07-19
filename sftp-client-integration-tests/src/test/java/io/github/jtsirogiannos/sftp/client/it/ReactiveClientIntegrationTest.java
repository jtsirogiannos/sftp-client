package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive facade against a real server — the path with the worst track record in this project.
 *
 * <p>{@code WriteSubscriber} shipped a race that truncated every upload while reporting success, and
 * {@code InputStreamPublisher} violated the Reactive Streams serialisation rule. Both were found by
 * driving real bytes through, not by reading the code. The core's unit tests now cover demand and
 * pool accounting with fakes; what only a real transfer shows is whether the bytes survive.
 */
class ReactiveClientIntegrationTest {

    private static EmbeddedSftpServer server;
    private static ExecutorService worker;
    private static final AtomicInteger UNIQUE = new AtomicInteger();

    @BeforeAll
    static void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        worker = Executors.newFixedThreadPool(4);
    }

    @AfterAll
    static void stopServer() {
        worker.shutdownNow();
        server.close();
    }

    /**
     * A megabyte through the demand-paced subscriber and back through the publisher, compared byte
     * for byte. The truncation bug reported success and wrote a short file, so only the comparison
     * catches it.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void roundTripsAMegabyteThroughTheReactivePath(String provider) throws Exception {
        String path = "/upload/rx-" + provider + "-" + UNIQUE.incrementAndGet() + ".bin";
        ReactiveSftpClient client = SftpClients.reactive(provider, server.config(), worker);
        try {
            byte[] payload = new byte[1024 * 1024];
            new Random(7).nextBytes(payload);

            SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>(worker, 16);
            CompletableFuture<Void> uploaded = client.upload(path, source).toCompletableFuture();

            // SubmissionPublisher drops anything submitted before a subscriber attaches, and
            // upload() subscribes on a worker thread — so wait, or the test proves nothing.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!source.hasSubscribers() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(source.hasSubscribers()).describedAs("upload() never subscribed").isTrue();

            for (int offset = 0; offset < payload.length; offset += 64 * 1024) {
                int length = Math.min(64 * 1024, payload.length - offset);
                source.submit(ByteBuffer.wrap(Arrays.copyOfRange(payload, offset, offset + length)));
            }
            source.close();
            uploaded.get(60, TimeUnit.SECONDS);

            assertThat(client.stat(path).toCompletableFuture().get(30, TimeUnit.SECONDS).size())
                    .describedAs("the upload reported success; the remote size must agree")
                    .isEqualTo(payload.length);

            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            CompletableFuture<Void> downloaded = new CompletableFuture<>();
            client.download(path).subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription subscription;

                @Override
                public void onSubscribe(Flow.Subscription s) {
                    subscription = s;
                    s.request(1);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    byte[] chunk = new byte[item.remaining()];
                    item.get(chunk);
                    sink.writeBytes(chunk);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable throwable) {
                    downloaded.completeExceptionally(throwable);
                }

                @Override
                public void onComplete() {
                    downloaded.complete(null);
                }
            });
            downloaded.get(60, TimeUnit.SECONDS);

            assertThat(sink.toByteArray())
                    .describedAs("reassembled bytes must match what was sent")
                    .isEqualTo(payload);
        } finally {
            client.close();
        }
    }
}
