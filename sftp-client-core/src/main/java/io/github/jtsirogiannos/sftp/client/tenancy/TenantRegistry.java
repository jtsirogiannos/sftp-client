package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tenants, created on first use from their definitions and kept until evicted — the part of a
 * multi-tenant client that does not care whether it is blocking or reactive.
 *
 * <p>A tenant's definition is asked for once and its client built from it. Built outside any lock —
 * a provider may consult a vault — at the cost that two threads asking for a new tenant at once may
 * each build one; the loser is closed before it has opened anything, since pools connect lazily.
 *
 * <p>Quiet tenants' parked connections are swept on the aggregate traffic of all of them, since a
 * pool otherwise evicts only on its own traffic and a quiet tenant has none.
 *
 * @param <C> the client type a tenant is reached through
 */
final class TenantRegistry<C> {

    private static final Logger log = LoggerFactory.getLogger(TenantRegistry.class);

    /** Builds a tenant's client from its definition. */
    @FunctionalInterface
    interface Factory<C> {
        Tenant<C> create(String tenantId, SftpTenant definition);
    }

    /** A tenant's client, and what it owns that must be closed with it. */
    static final class Tenant<C> {

        final C client;
        private final AutoCloseable owned;
        private final BoundedSftpSessionPool<?> ownPool;

        /**
         * @param owned   closed when the tenant is evicted; null when it owns nothing
         * @param ownPool swept for idle sessions; null for a tenant on the shared connection
         */
        Tenant(C client, AutoCloseable owned, BoundedSftpSessionPool<?> ownPool) {
            this.client = client;
            this.owned = owned;
            this.ownPool = ownPool;
        }

        boolean ownsConnections() {
            return owned != null;
        }

        void close() {
            if (owned != null) {
                try {
                    owned.close();
                } catch (Exception e) {
                    log.debug("Closing an evicted SFTP tenant failed", e);
                }
            }
        }
    }

    private final SftpTenantProvider definitions;
    private final SftpConnectionConfig sharedConfig;
    private final Factory<C> factory;
    private final ConcurrentMap<String, Tenant<C>> tenants = new ConcurrentHashMap<>();
    private final long sweepIntervalNanos;
    private final AtomicLong lastSweep = new AtomicLong(System.nanoTime());
    private volatile boolean closed;

    TenantRegistry(SftpTenantProvider definitions, SftpConnectionConfig sharedConfig,
            PoolSettings tenantPool, Factory<C> factory) {
        this.definitions = definitions;
        this.sharedConfig = sharedConfig;
        this.factory = factory;
        // Often enough that a quiet tenant's parked sessions outlive maxIdle by at most half again.
        this.sweepIntervalNanos = Math.max(tenantPool.maxIdle().toNanos() / 2, 1);
    }

    /** The tenant, created from its definition on first use. */
    Tenant<C> get(String tenantId) {
        if (closed) {
            throw new IllegalStateException("this multi-tenant SFTP client is closed");
        }
        sweepIfDue();
        Tenant<C> existing = tenants.get(tenantId);
        if (existing != null) {
            return existing;
        }
        SftpTenant definition = definitions.resolve(tenantId, sharedConfig);
        if (definition == null) {
            throw new SftpException("unknown SFTP tenant '" + tenantId
                    + "': the tenant provider has no definition for it");
        }
        Tenant<C> created = factory.create(tenantId, definition);
        log.debug("Created SFTP tenant '{}': {}", tenantId, definition);
        Tenant<C> raced = tenants.putIfAbsent(tenantId, created);
        if (raced != null) {
            created.close();
            return raced;
        }
        return created;
    }

    /**
     * Whether {@code tenant} has been evicted and replaced since it was handed out — the one case in
     * which a call that failed against it may be retried against the tenant as now defined.
     */
    boolean wasEvicted(String tenantId, Tenant<C> tenant) {
        return !closed && tenant.ownsConnections() && tenants.get(tenantId) != tenant;
    }

    void evict(String tenantId) {
        Tenant<C> evicted = tenants.remove(tenantId);
        if (evicted != null) {
            log.debug("Evicting SFTP tenant '{}'", tenantId);
            evicted.close();
        }
    }

    void close() {
        closed = true;
        for (String tenantId : List.copyOf(tenants.keySet())) {
            evict(tenantId);
        }
    }

    private void sweepIfDue() {
        long last = lastSweep.get();
        long now = System.nanoTime();
        if (now - last < sweepIntervalNanos || !lastSweep.compareAndSet(last, now)) {
            return;
        }
        // Only tenants' own pools: every confined tenant's traffic is the shared pool's traffic, and
        // that already evicts as it goes.
        for (Tenant<C> tenant : tenants.values()) {
            if (tenant.ownPool != null) {
                tenant.ownPool.evictExpired();
            }
        }
    }
}
