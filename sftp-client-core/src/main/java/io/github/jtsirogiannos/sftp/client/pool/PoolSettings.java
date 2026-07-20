package io.github.jtsirogiannos.sftp.client.pool;

import java.time.Duration;
import java.util.Objects;

/**
 * The pool's tuning knobs, as a value callers can actually reach.
 *
 * <p>{@link BoundedSftpSessionPool} has taken these since it existed, but no public entry point —
 * {@code SftpClients}, the Spring properties, the Quarkus config — ever exposed them, so every
 * consumer was silently pinned to the defaults. Under load that is the ninth caller blocking for
 * thirty seconds and then failing, discovered in production rather than in a config file.
 *
 * <p>Deliberately a separate type rather than components on
 * {@link io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig}: that record is the SPI-facing
 * description of a <em>connection</em>, handed to provider authors who have no business seeing pool
 * policy — pooling is the core's concern, which is also why this lives in the pool package and not
 * the api module.
 *
 * @param maxSize        most sessions the pool will hold, parked plus checked out. The ceiling on
 *                       concurrent operations per endpoint
 * @param maxIdle        how long a parked session may sit before it is closed instead of reused.
 *                       Align with the server's idle timeout if parked sessions keep dying
 * @param acquireTimeout how long a borrow waits at capacity before failing loudly
 */
public record PoolSettings(int maxSize, Duration maxIdle, Duration acquireTimeout) {

    public PoolSettings {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1, got: " + maxSize);
        }
        Objects.requireNonNull(maxIdle, "maxIdle");
        Objects.requireNonNull(acquireTimeout, "acquireTimeout");
        if (maxIdle.isNegative() || maxIdle.isZero()) {
            throw new IllegalArgumentException("maxIdle must be positive, got: " + maxIdle);
        }
        if (acquireTimeout.isNegative() || acquireTimeout.isZero()) {
            throw new IllegalArgumentException(
                "acquireTimeout must be positive, got: " + acquireTimeout);
        }
    }

    /** Eight sessions, five minutes idle, thirty seconds to acquire. */
    public static PoolSettings defaults() {
        return new PoolSettings(BoundedSftpSessionPool.DEFAULT_MAX_SIZE,
            BoundedSftpSessionPool.DEFAULT_MAX_IDLE,
            BoundedSftpSessionPool.DEFAULT_ACQUIRE_TIMEOUT);
    }

    public PoolSettings withMaxSize(int maxSize) {
        return new PoolSettings(maxSize, maxIdle, acquireTimeout);
    }

    public PoolSettings withMaxIdle(Duration maxIdle) {
        return new PoolSettings(maxSize, maxIdle, acquireTimeout);
    }

    public PoolSettings withAcquireTimeout(Duration acquireTimeout) {
        return new PoolSettings(maxSize, maxIdle, acquireTimeout);
    }
}
