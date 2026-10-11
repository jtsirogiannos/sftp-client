package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.core.ThreadLocalSftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Collects a multi-tenant client's configuration. Reached through
 * {@code SftpClients.multiTenant(...)}; mutable and not thread-safe, one per client.
 *
 * <pre>{@code
 * MultiTenantSftpClient sftp = SftpClients.multiTenant("mina", sharedConfig)
 *         .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
 *         .tenantResolver(() -> Optional.ofNullable(currentRequest().tenant()))
 *         .build();
 * }</pre>
 *
 * <p>Only {@link #tenants} is required. The tenant resolver defaults to {@link TenantContext}; the
 * session context to the thread-local one; the decorator to none.
 */
public final class MultiTenantSftpClientBuilder {

    /**
     * Two sessions, not eight: a process with a pool per tenant multiplies whatever this is by the
     * number of tenants, and most tenants are quiet most of the time.
     */
    private static final PoolSettings DEFAULT_TENANT_POOL = PoolSettings.defaults().withMaxSize(2);

    private final SftpProvider provider;
    private final SftpConnectionConfig sharedConfig;
    private SftpTenantProvider tenants;
    private TenantResolver resolver = TenantContext.resolver();
    private PoolSettings sharedPool = PoolSettings.defaults();
    private PoolSettings tenantPool = DEFAULT_TENANT_POOL;
    private SftpSessionContext sessionContext = ThreadLocalSftpSessionContext.instance();
    private BiFunction<String, SftpClient, SftpClient> decorator = (tenantId, client) -> client;

    private MultiTenantSftpClientBuilder(SftpProvider provider, SftpConnectionConfig sharedConfig) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.sharedConfig = sharedConfig;
    }

    /**
     * The entry point behind {@code SftpClients.multiTenant}, for callers holding a provider instance
     * rather than a name.
     *
     * @param sharedConfig the connection confined tenants share, or null if every tenant brings its
     *                     own
     */
    public static MultiTenantSftpClientBuilder create(SftpProvider provider,
            SftpConnectionConfig sharedConfig) {
        return new MultiTenantSftpClientBuilder(provider, sharedConfig);
    }

    /** How each tenant is isolated. Required. */
    public MultiTenantSftpClientBuilder tenants(SftpTenantProvider tenants) {
        this.tenants = Objects.requireNonNull(tenants, "tenants");
        return this;
    }

    /** Which tenant a call is for when it does not say. Defaults to {@link TenantContext}. */
    public MultiTenantSftpClientBuilder tenantResolver(TenantResolver resolver) {
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        return this;
    }

    /** The shared connection's pool, which every confined tenant draws on. */
    public MultiTenantSftpClientBuilder pool(PoolSettings pool) {
        this.sharedPool = Objects.requireNonNull(pool, "pool");
        return this;
    }

    /**
     * The pool each tenant with its own connection gets, unless its definition says otherwise.
     * Defaults to two sessions; also sets how often quiet tenants' parked sessions are swept, at
     * half its {@code maxIdle}.
     */
    public MultiTenantSftpClientBuilder tenantPool(PoolSettings pool) {
        this.tenantPool = Objects.requireNonNull(pool, "pool");
        return this;
    }

    /** Where the current session is remembered. Defaults to the calling thread. */
    public MultiTenantSftpClientBuilder sessionContext(SftpSessionContext sessionContext) {
        this.sessionContext = Objects.requireNonNull(sessionContext, "sessionContext");
        return this;
    }

    /**
     * Wraps each tenant's client as it is created — the place for per-tenant metrics or tracing.
     *
     * <pre>{@code
     * .decorate((tenant, client) -> MicrometerSftpClient.wrap(client, registry, Tags.of("tenant", tenant)))
     * }</pre>
     *
     * <p>Mind the cardinality: a tag per tenant is a time series per tenant.
     */
    public MultiTenantSftpClientBuilder decorate(BiFunction<String, SftpClient, SftpClient> decorator) {
        this.decorator = Objects.requireNonNull(decorator, "decorator");
        return this;
    }

    public MultiTenantSftpClient build() {
        if (tenants == null) {
            throw new IllegalStateException(
                    "no tenant provider: call tenants(...) to say how each tenant is isolated");
        }
        return new DefaultMultiTenantSftpClient(provider, sharedConfig, sharedPool, tenantPool,
                sessionContext, resolver, tenants, decorator);
    }
}
