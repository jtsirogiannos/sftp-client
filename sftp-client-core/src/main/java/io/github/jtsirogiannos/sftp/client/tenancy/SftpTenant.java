package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;

import java.util.Objects;
import java.util.Optional;

/**
 * How one tenant is isolated: a connection of its own, a root directory, or both.
 *
 * <pre>{@code
 * SftpTenant.connection(acmeConfig)                         // own host/credentials and pool
 * SftpTenant.rootDirectory("/tenants/acme")                 // confined on the shared connection
 * SftpTenant.connection(acmeConfig).withRootDirectory("/data")   // both
 * }</pre>
 *
 * <p>What an {@link SftpTenantProvider} answers with — the counterpart of the connection Hibernate's
 * {@code MultiTenantConnectionProvider} hands out for a tenant identifier.
 */
public final class SftpTenant {

    private final SftpConnectionConfig connection;
    private final String rootDirectory;
    private final PoolSettings pool;

    private SftpTenant(SftpConnectionConfig connection, String rootDirectory, PoolSettings pool) {
        this.connection = connection;
        this.rootDirectory = rootDirectory;
        this.pool = pool;
    }

    /**
     * A tenant with a connection — and so a pool — of its own. Isolation is whatever the server
     * enforces for those credentials.
     */
    public static SftpTenant connection(SftpConnectionConfig config) {
        return new SftpTenant(Objects.requireNonNull(config, "config"), null, null);
    }

    /**
     * A tenant confined to {@code rootDirectory} on the multi-tenant client's shared connection.
     *
     * @throws IllegalArgumentException if the directory is relative, is {@code /}, or contains a
     *                                  character a confined view refuses
     */
    public static SftpTenant rootDirectory(String rootDirectory) {
        return new SftpTenant(null, RootDirectory.of(rootDirectory).path(), null);
    }

    /** The same tenant, also confined to {@code rootDirectory} on its connection. */
    public SftpTenant withRootDirectory(String rootDirectory) {
        return new SftpTenant(connection, RootDirectory.of(rootDirectory).path(), pool);
    }

    /**
     * Pool sizing for this tenant's own connection, overriding the multi-tenant client's per-tenant
     * default. Meaningless, and refused, for a tenant on the shared connection.
     */
    public SftpTenant withPool(PoolSettings pool) {
        if (connection == null) {
            throw new IllegalStateException(
                    "a tenant on the shared connection has no pool of its own to size");
        }
        return new SftpTenant(connection, rootDirectory, Objects.requireNonNull(pool, "pool"));
    }

    public Optional<SftpConnectionConfig> connection() {
        return Optional.ofNullable(connection);
    }

    public Optional<String> rootDirectory() {
        return Optional.ofNullable(rootDirectory);
    }

    public Optional<PoolSettings> pool() {
        return Optional.ofNullable(pool);
    }

    /** Redacted, as the connection config's own is. */
    @Override
    public String toString() {
        return "SftpTenant[" + (connection == null ? "shared connection" : connection)
                + (rootDirectory == null ? "" : ", root=" + rootDirectory) + "]";
    }
}
