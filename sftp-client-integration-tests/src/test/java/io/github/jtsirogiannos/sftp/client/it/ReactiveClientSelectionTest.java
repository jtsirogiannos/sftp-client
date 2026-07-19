package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.core.AsyncReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.core.DefaultReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code SftpClients} must pick the reactive client that matches what the provider can actually do.
 *
 * <p>This is the one place the choice is made, and it is made once per client rather than per
 * operation — {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession} and
 * {@link io.github.jtsirogiannos.sftp.client.spi.SftpSession} are sibling contracts, so a client is
 * built over one or the other and the pool is typed to match. Getting this wrong would silently cost
 * every caller of an async provider the thread-free path, with nothing else failing.
 */
class ReactiveClientSelectionTest {

    private static final EmbeddedSftpServer SERVER = new EmbeddedSftpServer();
    private static ExecutorService worker;

    @BeforeAll
    static void start() {
        SERVER.start();
        worker = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void stop() {
        worker.shutdownNow();
        SERVER.close();
    }

    @Test
    void anAsyncProviderGetsTheThreadFreeClient() {
        try (ReactiveSftpClient client = SftpClients.reactive("inmemory", inMemoryConfig(), worker)) {
            assertInstanceOf(AsyncReactiveSftpClient.class, client,
                "the in-memory provider implements AsyncSftpProvider, so it must get the async client");
            assertEquals(true, client.capabilities().asyncStreaming());
        }
    }

    @Test
    void aBlockingProviderGetsTheWorkerOffloadedClient() {
        try (ReactiveSftpClient client = SftpClients.reactive("mina", SERVER.config(), worker)) {
            assertInstanceOf(DefaultReactiveSftpClient.class, client,
                "MINA has no async SFTP client API, so it must not claim the thread-free path");
            assertEquals(false, client.capabilities().asyncStreaming());
        }
    }

    /** End to end over the async client: the separation has to actually move bytes. */
    @Test
    void theAsyncClientRoundTripsAFile() throws Exception {
        byte[] payload = "round trip through the async path\n".getBytes(StandardCharsets.UTF_8);
        try (ReactiveSftpClient client = SftpClients.reactive("inmemory", inMemoryConfig(), worker)) {
            try (SubmissionPublisher<ByteBuffer> source = new SubmissionPublisher<>()) {
                CompletableFuture<Void> upload =
                    client.upload("/upload/async.txt", source).toCompletableFuture();

                // SubmissionPublisher discards anything submitted before a subscriber attaches, and
                // the borrow that precedes subscribing can go through the worker on a cold pool.
                // Without this wait the upload reports success having written an empty file.
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!source.hasSubscribers() && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                assertTrue(source.hasSubscribers(), "upload() never subscribed");

                source.submit(ByteBuffer.wrap(payload));
                source.close();
                upload.get(10, TimeUnit.SECONDS);
            }

            assertEquals(new String(payload, StandardCharsets.UTF_8),
                new String(drain(client.download("/upload/async.txt")), StandardCharsets.UTF_8));
        }
    }

    private static SftpConnectionConfig inMemoryConfig() {
        // No socket is involved; the in-memory provider ignores everything but the shape.
        return SftpConnectionConfig.builder()
            .host("in-memory")
            .username("tester")
            .password("unused".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
            .build();
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
        return done.get(10, TimeUnit.SECONDS);
    }
}
