package io.github.jtsirogiannos.sftp.client.micrometer;

import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPoolListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

/**
 * Binds pool health to a registry.
 *
 * <p>Separate from {@link MicrometerSftpClient} because pool state belongs to the pool, not to any
 * one client of it, and because it is a gauge rather than something that happens per call. Several
 * clients can share a pool; the gauges should be bound once.
 *
 * <p>Binding also attaches an event listener for the counters and the borrow-wait timer, so one
 * {@code bind} call wires everything the pool can report.
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
    /** Timer of borrows that had to wait for a slot. Rising rate or duration: pool undersized. */
    public static final String BORROW_WAIT = "sftp.client.pool.borrow.wait";
    /** Borrows that gave up after the acquire timeout — the failure callers actually see. */
    public static final String BORROW_TIMEOUTS = "sftp.client.pool.borrow.timeouts";
    /** Sessions closed as suspect instead of parked. Churn: each one costs a future reconnect. */
    public static final String DISCARDED = "sftp.client.pool.discarded";
    /** Sessions closed after sitting idle past the limit. */
    public static final String EVICTED = "sftp.client.pool.evicted";
    /** Parked sessions that died before reuse — usually a server idle timeout beating maxIdle. */
    public static final String VALIDATION_FAILURES = "sftp.client.pool.validation.failures";

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

        // The gauges above answer "how many, right now"; the meters below answer "what has been
        // happening", which is where the pre-failure signals live - waits climb before timeouts,
        // and churn shows up long before a gauge looks unhealthy.
        Timer waits = Timer.builder(BORROW_WAIT)
            .description("Time borrows spent waiting for a session when the pool was at capacity")
            .tags(tags)
            .register(registry);
        Counter timeouts = counter(registry, BORROW_TIMEOUTS,
            "Borrows that timed out with every session in use", tags);
        Counter discarded = counter(registry, DISCARDED,
            "Sessions closed as suspect instead of returned to the pool", tags);
        Counter evicted = counter(registry, EVICTED,
            "Sessions closed after exceeding the idle limit", tags);
        Counter validationFailures = counter(registry, VALIDATION_FAILURES,
            "Parked sessions that failed validation on borrow and were replaced", tags);

        pool.addListener(new SftpSessionPoolListener() {
            @Override
            public void borrowWaited(Duration waited) {
                waits.record(waited);
            }

            @Override
            public void borrowTimedOut() {
                timeouts.increment();
            }

            @Override
            public void sessionDiscarded() {
                discarded.increment();
            }

            @Override
            public void sessionEvicted() {
                evicted.increment();
            }

            @Override
            public void sessionValidationFailed() {
                validationFailures.increment();
            }
        });
    }

    private static Counter counter(MeterRegistry registry, String name, String description,
            Tags tags) {
        return Counter.builder(name).description(description).tags(tags).register(registry);
    }
}
