package io.github.jtsirogiannos.sftp.client.pool;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpSessionLifecycle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * A bounded pool of live SFTP sessions — the reason an operation costs one round-trip instead of a
 * TCP connect plus an SSH key exchange plus an auth exchange.
 *
 * <p>Connecting per operation pays a TCP handshake, an SSH key exchange and an authentication
 * exchange before any useful byte moves; reusing a session pays none of them. Even against an
 * in-process server on loopback, where a handshake is as cheap as it will ever get, that dominates
 * the cost of a {@code list()}. Over a real network each extra round-trip also pays the link
 * latency, so the gap widens rather than narrows.
 *
 * <p>Four behaviours, each of which exists because a session is a long-lived remote resource:
 *
 * <ul>
 *   <li><strong>Validation on borrow.</strong> A pooled session can die while parked — the server
 *       times it out, a NAT drops the mapping — and nothing tells the client. Every session is
 *       checked with {@link SftpSession#isValid()} on the way out, and a dead one is closed and
 *       replaced rather than handed over. This is the single most important thing a pool does.</li>
 *   <li><strong>LIFO reuse.</strong> Sessions are taken most-recently-parked first, so a burst of
 *       traffic keeps reusing a small hot set and the rest age out, rather than round-robin keeping
 *       every session marginally alive.</li>
 *   <li><strong>Idle eviction.</strong> A session parked longer than {@code maxIdle} is closed
 *       instead of reused, so the pool does not hold connections open indefinitely.</li>
 *   <li><strong>A hard ceiling with a bounded wait.</strong> At {@code maxSize} live sessions a
 *       borrow waits up to {@code acquireTimeout} and then fails loudly. Unbounded growth would just
 *       move the failure to the server, which has its own limit — OpenSSH refuses beyond
 *       {@code MaxStartups}, and the resulting error is far harder to read than this one.</li>
 * </ul>
 *
 * <p><strong>Eviction happens on pool traffic, not on a timer.</strong> There is no background
 * sweeper thread, so an idle pool holds its sessions until something calls {@link #borrow()} or
 * {@link #recycle} again. That is a deliberate trade: a thread per pool needs its own lifecycle, and
 * validation-on-borrow already covers the correctness case of a session the server has since
 * dropped. What it does not cover is a process that goes quiet for hours and keeps sockets open — if
 * that matters for your deployment, this is the place to add a scheduled sweep.
 *
 * <p>Generic in the session type so that one implementation serves both the blocking
 * {@link SftpSession} and the thread-free {@link AsyncSftpSession}. Nothing below depends on which:
 * a pool touches {@link SftpSessionLifecycle#isValid()} and {@link SftpSessionLifecycle#close()} and
 * nothing else, because pooling is about the lifetime of a connection rather than what travels over
 * it. Use {@link #blocking} or {@link #async} to build one.
 *
 * <p>Thread-safe. Sessions themselves are single-owner and are never shared between borrowers.
 */
public final class BoundedSftpSessionPool<S extends SftpSessionLifecycle>
        implements SftpSessionPool<S> {

    private static final Logger log = LoggerFactory.getLogger(BoundedSftpSessionPool.class);

    public static final int DEFAULT_MAX_SIZE = 8;
    public static final Duration DEFAULT_MAX_IDLE = Duration.ofMinutes(5);
    public static final Duration DEFAULT_ACQUIRE_TIMEOUT = Duration.ofSeconds(30);

    private final String providerName;
    private final Supplier<S> opener;
    private final SftpConnectionConfig config;
    private final int maxSize;
    private final long maxIdleNanos;
    private final long acquireTimeoutNanos;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition slotFreed = lock.newCondition();
    private final Deque<Parked<S>> idle = new ArrayDeque<>();

    /** Live sessions: parked plus checked out. Guarded by {@link #lock}. */
    private int live;
    private boolean closed;

    /**
     * Listeners, notified outside the lock. Copy-on-write because reads outnumber writes by the
     * operation count: registration happens at wiring time, iteration on every borrow.
     */
    private final java.util.concurrent.CopyOnWriteArrayList<SftpSessionPoolListener> listeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    /** A pool of blocking sessions — what every provider can supply. */
    public static BoundedSftpSessionPool<SftpSession> blocking(SftpProvider provider,
            SftpConnectionConfig config) {
        return blocking(provider, config, DEFAULT_MAX_SIZE, DEFAULT_MAX_IDLE,
                DEFAULT_ACQUIRE_TIMEOUT);
    }

    public static BoundedSftpSessionPool<SftpSession> blocking(SftpProvider provider,
            SftpConnectionConfig config, int maxSize, Duration maxIdle, Duration acquireTimeout) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(config, "config");
        return new BoundedSftpSessionPool<>(provider.name(), () -> provider.connect(config), config,
                maxSize, maxIdle, acquireTimeout);
    }

    /** A pool of thread-free sessions, for the providers that can open them. */
    public static BoundedSftpSessionPool<AsyncSftpSession> async(AsyncSftpProvider provider,
            SftpConnectionConfig config) {
        return async(provider, config, DEFAULT_MAX_SIZE, DEFAULT_MAX_IDLE, DEFAULT_ACQUIRE_TIMEOUT);
    }

    public static BoundedSftpSessionPool<AsyncSftpSession> async(AsyncSftpProvider provider,
            SftpConnectionConfig config, int maxSize, Duration maxIdle, Duration acquireTimeout) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(config, "config");
        return new BoundedSftpSessionPool<>(provider.name(), () -> provider.connectAsync(config),
                config, maxSize, maxIdle, acquireTimeout);
    }

    private BoundedSftpSessionPool(String providerName, Supplier<S> opener,
            SftpConnectionConfig config, int maxSize, Duration maxIdle, Duration acquireTimeout) {
        this.providerName = providerName;
        this.opener = opener;
        this.config = Objects.requireNonNull(config, "config");
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1, got: " + maxSize);
        }
        this.maxSize = maxSize;
        this.maxIdleNanos = maxIdle.toNanos();
        this.acquireTimeoutNanos = acquireTimeout.toNanos();
    }

    /**
     * Registers a listener for this pool's events. See {@link SftpSessionPoolListener} for the
     * contract: callbacks run on pool-using threads and must be quick and non-throwing.
     */
    public void addListener(SftpSessionPoolListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * A listener failure must never become an operation failure — observation does not get to break
     * the thing observed. The throw is logged rather than swallowed silently, because a listener
     * that throws on every event is a bug someone needs to see.
     */
    private void notifyListeners(java.util.function.Consumer<SftpSessionPoolListener> event) {
        for (SftpSessionPoolListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException e) {
                log.warn("A pool listener threw; ignoring so the operation is unaffected", e);
            }
        }
    }

    @Override
    public S borrow() {
        long deadline = System.nanoTime() + acquireTimeoutNanos;
        // Set on first finding the pool at capacity; the borrow reports its total wait on exit.
        long waitStartedNanos = 0;
        for (; ; ) {
            S pooled = null;
            boolean reservedSlot = false;
            List<S> expired = List.of();
            SftpException failure = null;
            boolean timedOut = false;

            lock.lock();
            try {
                if (closed) {
                    throw new IllegalStateException("pool is closed");
                }
                expired = takeExpiredLocked();
                Parked<S> candidate = idle.pollLast(); // LIFO: reuse the hottest session
                if (candidate != null) {
                    pooled = candidate.session();
                } else if (live < maxSize) {
                    live++; // reserve before releasing the lock so nobody else takes the slot
                    reservedSlot = true;
                } else {
                    if (waitStartedNanos == 0) {
                        waitStartedNanos = System.nanoTime();
                    }
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        log.warn("Session pool for {}@{}:{} exhausted: all {} sessions in use after "
                                        + "waiting {}ms. Either raise maxSize or find the operation "
                                        + "that borrowed a session and never returned it.",
                                config.username(), config.host(), config.port(), maxSize,
                                Duration.ofNanos(acquireTimeoutNanos).toMillis());
                        // The throw happens OUTSIDE this locked region — see below. Notifying under
                        // the lock is a lock-order inversion with any listener that takes a lock of
                        // its own, on exactly the path (exhaustion) where listeners page people and
                        // every other borrower is already queued behind this lock.
                        failure = new SftpException("timed out after "
                                + Duration.ofNanos(acquireTimeoutNanos).toMillis()
                                + "ms waiting for a session; all " + maxSize
                                + " are in use. Raise maxSize, or look for an operation that "
                                + "borrowed a session and never returned it.");
                        timedOut = true;
                    } else {
                        try {
                            slotFreed.awaitNanos(remaining);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            failure = new SftpException("interrupted while waiting for a session", e);
                        }
                        if (failure == null) {
                            continue;
                        }
                    }
                }
            } finally {
                lock.unlock();
            }

            // Closing happens outside the lock: it can block, and no one else needs to wait for
            // it. Deliberately BEFORE the failure check: sessions already removed from idle in this
            // iteration would otherwise never be closed when the borrow ends in a timeout.
            if (!expired.isEmpty()) {
                log.debug("Evicting {} session(s) idle beyond {}ms", expired.size(),
                        Duration.ofNanos(maxIdleNanos).toMillis());
                expired.forEach(session -> notifyListeners(SftpSessionPoolListener::sessionEvicted));
            }
            expired.forEach(BoundedSftpSessionPool::closeQuietly);

            if (failure != null) {
                reportWait(waitStartedNanos);
                if (timedOut) {
                    notifyListeners(SftpSessionPoolListener::borrowTimedOut);
                }
                throw failure;
            }

            if (pooled != null) {
                if (pooled.isValid()) {
                    reportWait(waitStartedNanos);
                    return pooled;
                }
                // Died while parked. Drop it and go round again rather than handing over a corpse.
                log.debug("Discarding a pooled session that failed validation for {}@{}:{}; opening a "
                        + "replacement", config.username(), config.host(), config.port());
                notifyListeners(SftpSessionPoolListener::sessionValidationFailed);
                closeQuietly(pooled);
                releaseSlot();
                continue;
            }

            try {
                log.debug("Opening a new session to {}@{}:{} via provider '{}' ({} live, max {})",
                        config.username(), config.host(), config.port(), providerName, live,
                        maxSize);
                reportWait(waitStartedNanos);
                return opener.get();
            } catch (RuntimeException e) {
                if (reservedSlot) {
                    releaseSlot(); // never leak the reservation on a failed connect
                }
                throw e;
            }
        }
    }

    /**
     * Satisfies the borrow inline when a validated idle session is waiting, and only falls back to
     * {@code executor} when something would genuinely block.
     *
     * <p>The fast path is the common one — a warm pool under steady load hands back the session it
     * most recently took in — and it costs nothing: {@link SftpSession#isValid()} is contractually
     * forbidden from making a round trip, and the LIFO pop happens under a lock held for a few
     * instructions. Bouncing that through an executor would add a scheduling hop to the operation
     * this method exists to keep off a thread.
     *
     * <p>Everything that can block is left to {@code executor}: opening a connection, waiting for a
     * slot at capacity, and closing evicted sessions. When the fast path misses, this degrades
     * exactly to the default — one hop, then {@link #borrow()}.
     */
    @Override
    public CompletionStage<S> borrowAsync(Executor executor) {
        S ready = null;
        List<S> expired = List.of();

        lock.lock();
        try {
            if (closed) {
                return CompletableFuture.failedFuture(new IllegalStateException("pool is closed"));
            }
            expired = takeExpiredLocked();
            Parked<S> candidate = idle.pollLast(); // LIFO, same as borrow()
            if (candidate != null) {
                ready = candidate.session();
            }
        } finally {
            lock.unlock();
        }

        // Closing blocks, so it never happens on this thread even when the borrow itself succeeded.
        if (!expired.isEmpty()) {
            List<S> toClose = expired;
            log.debug("Evicting {} session(s) idle beyond {}ms", toClose.size(),
                    Duration.ofNanos(maxIdleNanos).toMillis());
            toClose.forEach(session -> notifyListeners(SftpSessionPoolListener::sessionEvicted));
            executor.execute(() -> toClose.forEach(BoundedSftpSessionPool::closeQuietly));
        }

        if (ready != null) {
            if (ready.isValid()) {
                return CompletableFuture.completedFuture(ready);
            }
            // Died while parked. Releasing the slot and closing it both belong off this thread; the
            // retry goes the slow way rather than spinning here.
            S dead = ready;
            log.debug("Discarding a pooled session that failed validation for {}@{}:{}; opening a "
                    + "replacement", config.username(), config.host(), config.port());
            notifyListeners(SftpSessionPoolListener::sessionValidationFailed);
            return CompletableFuture.supplyAsync(() -> {
                closeQuietly(dead);
                releaseSlot();
                return borrow();
            }, executor);
        }

        // Nothing idle: opening a session or waiting for a slot both block.
        return CompletableFuture.supplyAsync(this::borrow, executor);
    }

    @Override
    public void recycle(S session) {
        if (session == null) {
            return;
        }
        boolean parked = false;
        lock.lock();
        try {
            // isValid() is contractually a local check with no round-trip, so it is safe here.
            if (!closed && session.isValid() && idle.size() < maxSize) {
                idle.addLast(new Parked<>(session, System.nanoTime()));
                parked = true;
                slotFreed.signal();
            }
        } finally {
            lock.unlock();
        }
        if (!parked) {
            discard(session);
        }
    }

    @Override
    public void discard(S session) {
        if (session != null) {
            closeQuietly(session);
            // Only when there really was a session: recycle() funnels its park-refusals here with
            // one, and counting a null discard would inflate churn with bookkeeping noise.
            notifyListeners(SftpSessionPoolListener::sessionDiscarded);
        }
        releaseSlot();
    }

    @Override
    public void close() {
        List<S> toClose;
        lock.lock();
        try {
            closed = true;
            toClose = drainIdleLocked();
            slotFreed.signalAll(); // wake anyone waiting so they fail fast instead of timing out
        } finally {
            lock.unlock();
        }
        log.debug("Closing pool for {}@{}:{}; releasing {} idle session(s)", config.username(),
                config.host(), config.port(), toClose.size());
        toClose.forEach(BoundedSftpSessionPool::closeQuietly);
    }

    /**
     * Closes parked sessions idle beyond {@code maxIdle} now, instead of on this pool's next borrow
     * or recycle.
     *
     * <p>Eviction otherwise runs only on this pool's own traffic, which is right for one pool per
     * endpoint and wrong for an owner of many: a process holding a pool per tenant has pools that see
     * nothing for hours, each keeping its parked sockets open the whole time. Such an owner calls this
     * on its own aggregate traffic. Sessions checked out are untouched, and the pool stays usable.
     *
     * @return how many sessions were closed
     */
    public int evictExpired() {
        List<S> expired;
        lock.lock();
        try {
            if (closed) {
                return 0;
            }
            // No waiter needs waking: a borrower only waits when nothing is parked, and everything
            // evicted here was parked.
            expired = takeExpiredLocked();
        } finally {
            lock.unlock();
        }
        if (!expired.isEmpty()) {
            log.debug("Evicting {} session(s) idle beyond {}ms on request", expired.size(),
                    Duration.ofNanos(maxIdleNanos).toMillis());
            expired.forEach(session -> notifyListeners(SftpSessionPoolListener::sessionEvicted));
            expired.forEach(BoundedSftpSessionPool::closeQuietly);
        }
        return expired.size();
    }

    /** Live sessions, parked plus checked out. For tests and diagnostics. */
    public int liveSessions() {
        lock.lock();
        try {
            return live;
        } finally {
            lock.unlock();
        }
    }

    /** Sessions currently parked and reusable. For tests and diagnostics. */
    public int idleSessions() {
        lock.lock();
        try {
            return idle.size();
        } finally {
            lock.unlock();
        }
    }

    /** Reports a completed wait, once, if this borrow ever waited at all. */
    private void reportWait(long waitStartedNanos) {
        if (waitStartedNanos != 0) {
            Duration waited = Duration.ofNanos(System.nanoTime() - waitStartedNanos);
            notifyListeners(listener -> listener.borrowWaited(waited));
        }
    }

    private void releaseSlot() {
        lock.lock();
        try {
            // Clamped: a caller that discards the same session twice must not corrupt the count and
            // strand capacity forever.
            live = Math.max(0, live - 1);
            slotFreed.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Removes and returns idle sessions past {@code maxIdle}. Caller closes them, unlocked. */
    private List<S> takeExpiredLocked() {
        List<S> expired = null;
        long now = System.nanoTime();
        // Oldest sits at the head, so this stops at the first live one.
        while (!idle.isEmpty() && now - idle.peekFirst().parkedAtNanos() >= maxIdleNanos) {
            Parked<S> stale = idle.pollFirst();
            live = Math.max(0, live - 1);
            if (expired == null) {
                expired = new ArrayList<>();
            }
            expired.add(stale.session());
        }
        return expired == null ? List.of() : expired;
    }

    private List<S> drainIdleLocked() {
        List<S> all = new ArrayList<>(idle.size());
        for (Parked<S> parked : idle) {
            all.add(parked.session());
            live = Math.max(0, live - 1);
        }
        idle.clear();
        return all;
    }

    private static void closeQuietly(SftpSessionLifecycle session) {
        try {
            session.close();
        } catch (RuntimeException ignored) {
            // close() is contractually silent; belt and braces
        }
    }

    private record Parked<S extends SftpSessionLifecycle>(S session, long parkedAtNanos) {
    }
}
