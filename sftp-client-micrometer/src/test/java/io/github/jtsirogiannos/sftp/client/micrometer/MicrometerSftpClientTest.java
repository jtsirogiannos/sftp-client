package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The decorator, driven through a real client against the in-memory provider.
 *
 * <p>A fake client would prove the decorator forwards; it would not prove the numbers mean anything.
 * Running against a provider that actually moves bytes is what makes the transfer counters
 * checkable.
 */
class MicrometerSftpClientTest {

    private SimpleMeterRegistry registry;
    private SftpClient client;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        client = MicrometerSftpClient.wrap(
            SftpClients.blocking("inmemory", config()), registry, config());
    }

    @Test
    void timesEachOperationWithItsNameAndOutcome() {
        client.mkdir("/upload/metered");
        client.stat("/upload/metered");

        assertThat(timerCount("mkdir", "success")).isEqualTo(1);
        assertThat(timerCount("stat", "success")).isEqualTo(1);
    }

    @Test
    void tagsAFailedOperationAsSuchRatherThanLosingIt() {
        assertThatThrownBy(() -> client.stat("/upload/does-not-exist"))
            .isInstanceOf(SftpException.class);

        assertThat(timerCount("stat", "failure"))
            .describedAs("a failure that is not recorded is worse than no metric at all")
            .isEqualTo(1);
        assertThat(timerCount("stat", "success")).isZero();
    }

    @Test
    void countsBytesInBothDirections() {
        byte[] payload = "twenty-four bytes here!!".getBytes(StandardCharsets.UTF_8);
        client.upload("/upload/bytes.bin", new ByteArrayInputStream(payload));
        client.download("/upload/bytes.bin", new ByteArrayOutputStream());

        assertThat(counterTotal("upload")).isEqualTo(payload.length);
        assertThat(counterTotal("download")).isEqualTo(payload.length);
    }

    /**
     * Convenience methods are composed from primitives, and the primitives are what crossed the
     * wire. Reporting {@code mkdirs} as one opaque call would hide that a four-deep path cost eight
     * round trips.
     */
    @Test
    void reportsWhatCrossedTheWireRatherThanTheConvenienceCall() {
        client.mkdirs("/upload/a/b/c");

        assertThat(timerCount("mkdir", "success")).isEqualTo(3);
        assertThat(timerCount("exists", "success")).isPositive();
        assertThat(registry.find(MeteredSftpOperations.OPERATIONS).tag("operation", "mkdirs").timers())
            .describedAs("there is no wire operation called mkdirs")
            .isEmpty();
    }

    /**
     * The blind spot this decorator was built to avoid: {@code withSession} hands the block a
     * session-bound view that would otherwise skip the decorator completely — and a unit of work is
     * exactly where multi-step traffic lives.
     */
    @Test
    void measuresOperationsInsideAUnitOfWork() {
        client.withSessionWithoutResult(work -> {
            work.mkdir("/upload/unit");
            work.upload("/upload/unit/f.bin", new ByteArrayInputStream(new byte[16]));
        });

        assertThat(timerCount("mkdir", "success"))
            .describedAs("operations inside withSession must not vanish from the metrics")
            .isEqualTo(1);
        assertThat(counterTotal("upload")).isEqualTo(16);
    }

    @Test
    void bindsPoolGauges() {
        SftpConnectionConfig config = config();
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                SftpClients.provider("inmemory"), config)) {
            SftpPoolMetrics.bind(pool, registry, Tags.of("host", config.host()));

            assertThat(registry.get(SftpPoolMetrics.LIVE).gauge().value()).isZero();
            pool.recycle(pool.borrow());
            assertThat(registry.get(SftpPoolMetrics.LIVE).gauge().value()).isEqualTo(1);
            assertThat(registry.get(SftpPoolMetrics.IDLE).gauge().value()).isEqualTo(1);
        }
    }

    /** One bind call wires the event meters too, driven here by real pool behaviour. */
    @Test
    void bindWiresTheEventCountersAndTheWaitTimer() {
        SftpConnectionConfig config = config();
        try (BoundedSftpSessionPool<SftpSession> pool = BoundedSftpSessionPool.blocking(
                SftpClients.provider("inmemory"), config, 2,
                java.time.Duration.ofMillis(40), java.time.Duration.ofSeconds(1))) {
            SftpPoolMetrics.bind(pool, registry, Tags.of("host", config.host()));

            pool.discard(pool.borrow());                   // churn
            pool.recycle(pool.borrow());                   // park one...
            try {
                Thread.sleep(90);                          // ...past the idle limit
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            pool.recycle(pool.borrow());                   // traffic triggers the sweep

            assertThat(registry.get(SftpPoolMetrics.DISCARDED).counter().count()).isEqualTo(1);
            assertThat(registry.get(SftpPoolMetrics.EVICTED).counter().count()).isEqualTo(1);
            assertThat(registry.get(SftpPoolMetrics.BORROW_WAIT).timer().count())
                .describedAs("nothing ever waited, and an uncontended pool must say so")
                .isZero();
        }
    }

    @Test
    void tagsCarryTheEndpointSoSeveralAreDistinguishable() {
        client.exists("/upload");

        assertThat(registry.get(MeteredSftpOperations.OPERATIONS)
            .tag("operation", "exists").tag("host", "in-memory").timer().count()).isEqualTo(1);
    }

    // --- helpers ---

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
