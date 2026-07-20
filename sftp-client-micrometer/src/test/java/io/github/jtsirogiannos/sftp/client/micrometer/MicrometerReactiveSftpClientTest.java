package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reactive decorator. The recurring hazard is measuring the return rather than the completion. */
class MicrometerReactiveSftpClientTest {

    private SimpleMeterRegistry registry;
    private ReactiveSftpClient client;
    private ExecutorService worker;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        worker = Executors.newCachedThreadPool();
        client = MicrometerReactiveSftpClient.wrap(
            SftpClients.reactive("inmemory", config(), worker), registry, config());
    }

    @AfterEach
    void tearDown() {
        worker.shutdownNow();
    }

    @Test
    void timesOperationsAndTagsTheirOutcome() throws Exception {
        await(client.mkdir("/upload/rx"));
        assertThatThrownBy(() -> await(client.stat("/upload/nope"))).isInstanceOf(Exception.class);

        assertThat(timerCount("mkdir", "success")).isEqualTo(1);
        assertThat(timerCount("stat", "failure"))
            .describedAs("a failed stage must be tagged failure, not lost")
            .isEqualTo(1);
    }

    /**
     * The trap this whole class exists to avoid. A reactive call returns a stage immediately, so a
     * timer stopped at the return would record the cost of building it — near zero, and healthy
     * looking. The delegate here takes a known minimum time; the recorded duration must reflect it.
     */
    @Test
    void measuresTheCompletionRatherThanTheReturn() throws Exception {
        ReactiveSftpClient slow = MicrometerReactiveSftpClient.wrap(
            new SlowClient(SftpClients.reactive("inmemory", config(), worker), worker),
            registry, config());

        await(slow.mkdir("/upload/slow"));

        assertThat(registry.get(MeteredSftpOperations.OPERATIONS)
            .tag("operation", "mkdir").timer().totalTime(TimeUnit.MILLISECONDS))
            .describedAs("timing the return instead of the completion reports ~0")
            .isGreaterThan(40);
    }

    @Test
    void countsBytesThroughThePublisherInBothDirections() throws Exception {
        byte[] payload = new byte[64];
        await(client.upload("/upload/rx.bin", publisherOf(payload)));
        drain(client.download("/upload/rx.bin"));

        assertThat(counterTotal("upload")).isEqualTo(64);
        assertThat(counterTotal("download")).isEqualTo(64);
    }

    /** A download publisher nobody subscribes to has transferred nothing and must report nothing. */
    @Test
    void doesNotRecordADownloadThatWasNeverSubscribed() {
        client.download("/upload/never.bin");

        assertThat(registry.find(MeteredSftpOperations.OPERATIONS)
            .tag("operation", "download").timers()).isEmpty();
    }

    // --- helpers ---

    /** Delays completion so a duration exists to measure. */
    private record SlowClient(ReactiveSftpClient delegate, ExecutorService worker)
            implements ReactiveSftpClient {
        @Override
        public CompletionStage<Void> mkdir(String path) {
            return delegate.mkdir(path).thenCompose(ignored -> {
                CompletableFuture<Void> slow = new CompletableFuture<>();
                worker.submit(() -> {
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    slow.complete(null);
                });
                return slow;
            });
        }

        @Override public CompletionStage<java.util.List<io.github.jtsirogiannos.sftp.client.api.RemoteFile>> list(String p) { return delegate.list(p); }
        @Override public CompletionStage<io.github.jtsirogiannos.sftp.client.api.RemoteFile> stat(String p) { return delegate.stat(p); }
        @Override public CompletionStage<io.github.jtsirogiannos.sftp.client.api.RemoteFile> lstat(String p) { return delegate.lstat(p); }
        @Override public CompletionStage<Boolean> exists(String p) { return delegate.exists(p); }
        @Override public CompletionStage<String> realpath(String p) { return delegate.realpath(p); }
        @Override public CompletionStage<Void> rename(String f, String t) { return delegate.rename(f, t); }
        @Override public CompletionStage<Void> remove(String p) { return delegate.remove(p); }
        @Override public CompletionStage<Void> symlink(String l, String t) { return delegate.symlink(l, t); }
        @Override public CompletionStage<String> readlink(String l) { return delegate.readlink(l); }
        @Override public CompletionStage<Void> hardlink(String l, String t) { return delegate.hardlink(l, t); }
        @Override public CompletionStage<Void> setAttributes(String p, io.github.jtsirogiannos.sftp.client.api.AttributeUpdate u) { return delegate.setAttributes(p, u); }
        @Override public CompletionStage<io.github.jtsirogiannos.sftp.client.api.FilesystemStats> statvfs(String p) { return delegate.statvfs(p); }
        @Override public Flow.Publisher<ByteBuffer> download(String p, long o) { return delegate.download(p, o); }
        @Override public CompletionStage<Void> upload(String p, Flow.Publisher<ByteBuffer> d, io.github.jtsirogiannos.sftp.client.spi.SftpSession.WriteMode m) { return delegate.upload(p, d, m); }
        @Override public io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities capabilities() { return delegate.capabilities(); }
        @Override public void close() { delegate.close(); }
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(20, TimeUnit.SECONDS);
    }

    private static void drain(Flow.Publisher<ByteBuffer> publisher) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription s) { s.request(Long.MAX_VALUE); }
            @Override public void onNext(ByteBuffer item) { }
            @Override public void onError(Throwable t) { done.completeExceptionally(t); }
            @Override public void onComplete() { done.complete(null); }
        });
        done.get(20, TimeUnit.SECONDS);
    }

    private static Flow.Publisher<ByteBuffer> publisherOf(byte[] bytes) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            private boolean sent;
            @Override public void request(long n) {
                if (!sent && n > 0) {
                    sent = true;
                    subscriber.onNext(ByteBuffer.wrap(bytes));
                    subscriber.onComplete();
                }
            }
            @Override public void cancel() { }
        });
    }

    private long timerCount(String operation, String outcome) {
        return registry.find(MeteredSftpOperations.OPERATIONS)
            .tag("operation", operation).tag("outcome", outcome).timers()
            .stream().mapToLong(t -> t.count()).sum();
    }

    private double counterTotal(String direction) {
        return registry.find(MeteredSftpOperations.TRANSFERRED)
            .tag("direction", direction).counters()
            .stream().mapToDouble(c -> c.count()).sum();
    }

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
            .host("in-memory").username("tester").password("unused".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY).build();
    }
}
