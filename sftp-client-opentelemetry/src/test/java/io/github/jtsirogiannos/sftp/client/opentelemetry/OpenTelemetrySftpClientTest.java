package io.github.jtsirogiannos.sftp.client.opentelemetry;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The tracing decorator, driven through a real client and asserted against exported spans. */
class OpenTelemetrySftpClientTest {

    private InMemorySpanExporter spans;
    private SftpClient client;

    @BeforeEach
    void setUp() {
        spans = InMemorySpanExporter.create();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
            .build();
        client = OpenTelemetrySftpClient.wrap(
            SftpClients.blocking("inmemory", config()), sdk, config());
    }

    @Test
    void namesSpansByOperationAndKeepsThePathOutOfTheName() {
        client.mkdir("/upload/traced");

        SpanData span = only("sftp.mkdir");
        assertThat(span.getName())
            .describedAs("a remote path in a span name is unbounded cardinality")
            .doesNotContain("/upload");
        assertThat(span.getAttributes().get(SftpAttributes.FILE_PATH)).isEqualTo("/upload/traced");
    }

    @Test
    void carriesTheStableConnectionAttributes() {
        client.exists("/upload");

        SpanData span = only("sftp.exists");
        assertThat(span.getAttributes().get(SftpAttributes.SERVER_ADDRESS)).isEqualTo("in-memory");
        assertThat(span.getAttributes().get(SftpAttributes.SERVER_PORT)).isEqualTo(22L);
        assertThat(span.getAttributes().get(SftpAttributes.NETWORK_PROTOCOL_NAME)).isEqualTo("sftp");
        assertThat(span.getKind().name()).isEqualTo("CLIENT");
    }

    @Test
    void reportsTheNegotiatedProtocolVersionRatherThanAGuess() {
        client.exists("/upload");

        assertThat(only("sftp.exists").getAttributes()
            .get(SftpAttributes.NETWORK_PROTOCOL_VERSION))
            .describedAs("the in-memory provider negotiates v6")
            .isEqualTo("6");
    }

    @Test
    void marksAFailureWithStatusAndErrorType() {
        assertThatThrownBy(() -> client.stat("/upload/missing")).isInstanceOf(SftpException.class);

        SpanData span = only("sftp.stat");
        assertThat(span.getStatus().getStatusCode().name()).isEqualTo("ERROR");
        assertThat(span.getAttributes().get(SftpAttributes.ERROR_TYPE))
            .isEqualTo(SftpException.class.getCanonicalName());
        assertThat(span.getEvents())
            .describedAs("recorded once, where it escaped to the caller")
            .hasSize(1);
    }

    @Test
    void recordsBytesActuallyTransferred() {
        byte[] payload = "seventeen bytes!!".getBytes(StandardCharsets.UTF_8);
        client.upload("/upload/t.bin", new ByteArrayInputStream(payload));
        client.download("/upload/t.bin", new ByteArrayOutputStream());

        assertThat(only("sftp.upload").getAttributes().get(SftpAttributes.FILE_SIZE))
            .isEqualTo((long) payload.length);
        assertThat(only("sftp.download").getAttributes().get(SftpAttributes.FILE_SIZE))
            .isEqualTo((long) payload.length);
    }

    /**
     * The shape that makes a unit of work legible: one span for the block, the operations beneath
     * it. Deliberately the opposite of the metrics decorator, which does not aggregate composites.
     */
    @Test
    void nestsAUnitOfWorkUnderOneSessionSpan() {
        client.withSessionWithoutResult(work -> {
            work.mkdir("/upload/unit");
            work.upload("/upload/unit/f.bin", new ByteArrayInputStream(new byte[8]));
        });

        SpanData session = only("sftp.session");
        List<SpanData> children = spans.getFinishedSpanItems().stream()
            .filter(s -> s.getParentSpanId().equals(session.getSpanId())).toList();

        assertThat(children).extracting(SpanData::getName)
            .describedAs("operations inside withSession must be children, not orphans")
            .containsExactlyInAnyOrder("sftp.mkdir", "sftp.upload");
        assertThat(session.getKind().name()).isEqualTo("INTERNAL");
    }

    // --- helpers ---

    private SpanData only(String name) {
        List<SpanData> matching = spans.getFinishedSpanItems().stream()
            .filter(s -> s.getName().equals(name)).toList();
        assertThat(matching).describedAs("expected exactly one %s span", name).hasSize(1);
        return matching.get(0);
    }

    private static SftpConnectionConfig config() {
        return SftpConnectionConfig.builder()
            .host("in-memory").username("tester").password("unused".toCharArray())
            .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY).build();
    }
}
