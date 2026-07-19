package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

/**
 * Binds pool health to a registry.
 *
 * <p>Separate from {@link MicrometerSftpClient} because pool state belongs to the pool, not to any
 * one client of it, and because it is a gauge rather than something that happens per call. Several
 * clients can share a pool; the gauges should be bound once.
 *
 * <p>This is the signal worth having first. Pool exhaustion is the classic production failure for a
 * pooled client — every session checked out, every caller queued behind the acquire timeout, and an
 * application that looks hung rather than broken. {@code BoundedSftpSessionPool} already logs a WARN
 * when it happens; {@code sftp.client.pool.live} sitting at its ceiling while work backs up is how
 * you see it coming beforehand.
 */
public final class SftpPoolMetrics {

    /** Sessions the pool has open: parked plus checked out. */
    public static final String LIVE = "sftp.client.pool.live";
    /** Sessions parked and reusable. Live minus idle is the number checked out. */
    public static final String IDLE = "sftp.client.pool.idle";

    private SftpPoolMetrics() {
    }

    /**
     * Registers gauges for {@code pool}.
     *
     * <p>Micrometer holds gauge targets weakly, so the caller must keep the pool referenced for the
     * gauges to keep reporting — which is normal, since a pool nobody holds is a pool nobody uses.
     */
    public static void bind(BoundedSftpSessionPool<?> pool, MeterRegistry registry, Tags tags) {
        Gauge.builder(LIVE, pool, BoundedSftpSessionPool::liveSessions)
            .description("SFTP sessions open: parked plus checked out")
            .tags(tags)
            .register(registry);
        Gauge.builder(IDLE, pool, BoundedSftpSessionPool::idleSessions)
            .description("SFTP sessions parked and available for reuse")
            .tags(tags)
            .register(registry);
    }
}
