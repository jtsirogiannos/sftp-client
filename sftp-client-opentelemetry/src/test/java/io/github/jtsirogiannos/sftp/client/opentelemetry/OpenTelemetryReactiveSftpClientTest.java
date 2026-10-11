package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The reactive tracing decorator. */
class OpenTelemetryReactiveSftpClientTest {

    private InMemorySpanExporter spans;
    private ReactiveSftpClient client;
    private ExecutorService worker;

    @BeforeEach
    void setUp() {
        spans = InMemorySpanExporter.create();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
            .build();
        worker = Executors.newCachedThreadPool();
        client = OpenTelemetryReactiveSftpClient.wrap(
            SftpClients.reactive("inmemory", config(), worker), sdk, config());
    }

    @AfterEach
    void tearDown() {
        worker.shutdownNow();
    }

    @Test
    void emitsAClientSpanPerOperationWithTheStandardAttributes() throws Exception {
        await(client.mkdir("/upload/rx-traced"));

        SpanData span = only("sftp.mkdir");
        assertThat(span.getKind().name()).isEqualTo("CLIENT");
        assertThat(span.getAttributes().get(SftpAttributes.SERVER_ADDRESS)).isEqualTo("in-memory");
        assertThat(span.getAttributes().get(SftpAttributes.FILE_PATH)).isEqualTo("/upload/rx-traced");
        assertThat(span.getName()).doesNotContain("/upload");
    }

    /**
     * A span ended where the method returns would have a duration measuring stage construction and
     * would already be exported before the operation finished. Nothing should be exported until the
     * stage completes.
     */
    @Test
    void endsTheSpanOnCompletionRatherThanOnReturn() throws Exception {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        ReactiveSftpClient gated = OpenTelemetryReactiveSftpClient.wrap(
            new GatedClient(SftpClients.reactive("inmemory", config(), worker), gate),
            OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build()).build(),
            config());

        CompletionStage<Void> inFlight = gated.mkdir("/upload/gated");
        assertThat(spans.getFinishedSpanItems())
            .describedAs("the operation has not finished, so no span should have been exported")
            .isEmpty();

        gate.complete(null);
        await(inFlight);
        assertThat(spans.getFinishedSpanItems()).hasSize(1);
    }

    @Test
    void marksAFailedStageWithStatusAndErrorType() {
        assertThatThrownBy(() -> await(client.stat("/upload/missing"))).isInstanceOf(Exception.class);

        SpanData span = only("sftp.stat");
        assertThat(span.getStatus().getStatusCode().name()).isEqualTo("ERROR");
        assertThat(span.getAttributes().get(SftpAttributes.ERROR_TYPE))
            .isEqualTo(SftpException.class.getCanonicalName());
    }

    /**
     * A failure arriving through a <em>dependent</em> stage is wrapped in a
     * {@link java.util.concurrent.CompletionException}, and recording that as {@code error.type}
     * would label every failure with a class that says nothing about what went wrong.
     *
     * <p>The client under test in the other cases never produces a wrapped failure — the core
     * unwraps before completing — so this needs a delegate that fails through composition. Without
     * it the unwrapping is dead code that no test would notice being deleted.
     */
    @Test
    void unwrapsCompletionExceptionSoErrorTypeNamesTheRealFailure() {
        CompletableFuture<Void> gate = new CompletableFuture<>();
        ReactiveSftpClient composed = OpenTelemetryReactiveSftpClient.wrap(
            new GatedClient(SftpClients.reactive("inmemory", config(), worker), gate),
            OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build()).build(),
            config());

        CompletionStage<Void> inFlight = composed.mkdir("/upload/composed");
        gate.completeExceptionally(new SftpException("the delegate failed"));
        assertThatThrownBy(() -> await(inFlight)).isInstanceOf(Exception.class);

        assertThat(only("sftp.mkdir").getAttributes().get(SftpAttributes.ERROR_TYPE))
            .describedAs("CompletionException as the error type would say nothing about the failure")
            .isEqualTo(SftpException.class.getCanonicalName());
    }

    @Test
    void recordsTransferSizeFromWhatFlowed() throws Exception {
        await(client.upload("/upload/rx.bin", publisherOf(new byte[48])));
        drain(client.download("/upload/rx.bin"));

        assertThat(only("sftp.upload").getAttributes().get(SftpAttributes.FILE_SIZE)).isEqualTo(48L);
        assertThat(only("sftp.download").getAttributes().get(SftpAttributes.FILE_SIZE)).isEqualTo(48L);
    }

    /** A reactive unit of work becomes one session span, its operations beneath it. */
    @Test
    void nestsAReactiveUnitOfWorkUnderOneSessionSpan() throws Exception {
        await(client.withSession(ops -> ops.mkdir("/upload/rx-unit")
            .thenCompose(v -> ops.exists("/upload/rx-unit"))));

        SpanData session = only("sftp.session");
        assertThat(session.getKind().name()).isEqualTo("INTERNAL");
        assertThat(spans.getFinishedSpanItems().stream()
            .filter(s -> s.getParentSpanId().equals(session.getSpanId())).map(SpanData::getName))
            .containsExactlyInAnyOrder("sftp.mkdir", "sftp.exists");
    }

    @Test
    void anOpenReactiveSessionHasOneSpanFromOpenToClose() throws Exception {
        var opened = await(client.openSession());
        await(opened.mkdir("/upload/rx-opened"));
        assertThat(spans.getFinishedSpanItems()).extracting(SpanData::getName)
            .describedAs("still open, so its span has not ended")
            .doesNotContain("sftp.session");
        await(opened.close());
        await(opened.close());

        SpanData session = only("sftp.session");
        assertThat(only("sftp.mkdir").getParentSpanId()).isEqualTo(session.getSpanId());
    }

    @Test
    void doesNotEmitASpanForADownloadNobodySubscribedTo() {
        client.download("/upload/never.bin");

        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    // --- helpers ---

    /** Holds the operation open until the gate completes, so "returned" and "finished" differ. */
    private record GatedClient(ReactiveSftpClient delegate, CompletableFuture<Void> gate)
            implements ReactiveSftpClient {
        @Override
        public CompletionStage<Void> mkdir(String path) {
            return delegate.mkdir(path).thenCompose(ignored -> gate);
        }

        @Override public CompletionStage<List<io.github.jtsirogiannos.sftp.client.api.RemoteFile>> list(String p) { return delegate.list(p); }
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
        @Override public <R> CompletionStage<R> withSession(java.util.function.Function<io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations, ? extends CompletionStage<R>> w) { return delegate.withSession(w); }
        @Override public CompletionStage<io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession> openSession() { return delegate.openSession(); }
        @Override public io.github.jtsirogiannos.sftp.client.context.SftpSessionContext sessionContext() { return delegate.sessionContext(); }
        @Override public void close() { delegate.close(); }
    }

    private SpanData only(String name) {
        List<SpanData> matching = spans.getFinishedSpanItems().stream()
            .filter(s -> s.getName().equals(name)).toList();
        assertThat(matching).describedAs("expected exactly one %s span", name).hasSize(1);
        return matching.get(0);
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

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
            .host("in-memory").username("tester").password("unused".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY).build();
    }
}
