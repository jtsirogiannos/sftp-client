package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Bootstrap entry point — the {@code DriverManager} of this library.
 *
 * <p>Discovers providers via {@link ServiceLoader}, so adding {@code sftp-provider-mina} to the
 * classpath is all it takes to make {@code "mina"} resolvable. Usable with no framework at all:
 *
 * <pre>{@code
 * var config = SftpConnectionConfig.builder()
 *         .host("sftp.example.com").username("svc")
 *         .password("...".toCharArray())
 *         .build();
 * try (var client = SftpClients.blocking("mina", config)) {
 *     client.list("/inbound").forEach(System.out::println);
 * }
 * }</pre>
 */
public final class SftpClients {

    private static final Logger log = LoggerFactory.getLogger(SftpClients.class);

    private SftpClients() {
    }

    public static List<String> availableProviders() {
        return ServiceLoader.load(SftpProvider.class).stream()
            .map(p -> p.get().name())
            .sorted()
            .collect(Collectors.toList());
    }

    public static SftpProvider provider(String name) {
        return ServiceLoader.load(SftpProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> p.name().equals(name))
            .findFirst()
            // This message is the most-read line of your docs. Say what's missing and what to add.
            .orElseThrow(() -> new SftpException(
                "No SFTP provider named '" + name + "' on the classpath. Available: "
                    + availableProviders() + ". Add sftp-provider-" + name
                    + " as a dependency.",
                null));
    }

    /**
     * Everything a client can be configured with, for when the shortcuts below are not enough.
     *
     * <pre>{@code
     * SftpClient client = SftpClients.builder("mina")
     *         .config(config)
     *         .pool(PoolSettings.defaults().withMaxSize(24))
     *         .sessionContext(myContext)
     *         .blocking();
     * }</pre>
     */
    public static Builder builder(String providerName) {
        return new Builder(providerName);
    }

    /**
     * A client for many tenants, each on a connection of its own.
     *
     * <pre>{@code
     * MultiTenantSftpClient sftp = SftpClients.multiTenant("mina")
     *         .tenants((tenant, base) -> SftpTenant.connection(vault.configFor(tenant)))
     *         .build();
     * }</pre>
     */
    public static io.github.jtsirogiannos.sftp.client.tenancy.MultiTenantSftpClientBuilder multiTenant(
            String providerName) {
        return io.github.jtsirogiannos.sftp.client.tenancy.MultiTenantSftpClientBuilder.create(
                provider(providerName), null);
    }

    /**
     * A client for many tenants sharing {@code sharedConfig}'s connection, each confined to a
     * directory — or, tenant by tenant, given a connection of its own.
     *
     * <pre>{@code
     * MultiTenantSftpClient sftp = SftpClients.multiTenant("mina", sharedConfig)
     *         .tenants(SftpTenantProvider.rootDirectoryTemplate("/tenants/{tenant}"))
     *         .build();
     * }</pre>
     */
    public static io.github.jtsirogiannos.sftp.client.tenancy.MultiTenantSftpClientBuilder multiTenant(
            String providerName, SftpConnectionConfig sharedConfig) {
        return io.github.jtsirogiannos.sftp.client.tenancy.MultiTenantSftpClientBuilder.create(
                provider(providerName), java.util.Objects.requireNonNull(sharedConfig, "sharedConfig"));
    }

    public static SftpClient blocking(String providerName, SftpConnectionConfig config) {
        return blocking(providerName, config, PoolSettings.defaults());
    }

    /** As {@link #blocking(String, SftpConnectionConfig)}, with the pool tuned rather than default. */
    public static SftpClient blocking(String providerName, SftpConnectionConfig config,
            PoolSettings pool) {
        return builder(providerName).config(config).pool(pool).blocking();
    }

    /**
     * A reactive client, backed by whichever session contract the provider can actually honour.
     *
     * <p>This {@code instanceof} is the only one in the reactive path, and it is here rather than in
     * the client because it is a question about the <em>provider</em>, asked once. A provider that
     * can open {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession}s gets a client and a
     * pool typed to them, and every operation afterwards is statically async — no capability flag
     * consulted, no session type re-tested per call.
     */
    public static ReactiveSftpClient reactive(String providerName, SftpConnectionConfig config,
                                              Executor worker) {
        return reactive(providerName, config, worker, PoolSettings.defaults());
    }

    /** As {@link #reactive(String, SftpConnectionConfig, Executor)}, with the pool tuned. */
    public static ReactiveSftpClient reactive(String providerName, SftpConnectionConfig config,
                                              Executor worker, PoolSettings pool) {
        return builder(providerName).config(config).pool(pool).reactive(worker);
    }

    private static ReactiveSftpClient buildReactive(String providerName, SftpConnectionConfig config,
            Executor worker, PoolSettings pool) {
        SftpProvider provider = provider(providerName);
        if (provider instanceof AsyncSftpProvider asyncProvider) {
            log.debug("Creating an async reactive client for {} using provider '{}'", config,
                providerName);
            return new AsyncReactiveSftpClient(asyncProvider,
                BoundedSftpSessionPool.async(asyncProvider, config, pool.maxSize(), pool.maxIdle(),
                    pool.acquireTimeout()), worker);
        }
        log.debug("Creating a reactive client for {} using provider '{}'", config, providerName);
        return new DefaultReactiveSftpClient(provider,
            BoundedSftpSessionPool.blocking(provider, config, pool.maxSize(), pool.maxIdle(),
                pool.acquireTimeout()), worker);
    }

    /**
     * Collects a client's configuration. Mutable and not thread-safe; one per client.
     *
     * <p>Only the connection config is required. The pool defaults to {@link PoolSettings#defaults()}
     * and the session context to {@link ThreadLocalSftpSessionContext}, which is right for blocking
     * code; a framework whose requests hop threads supplies its own.
     */
    public static final class Builder {

        private final String providerName;
        private SftpConnectionConfig config;
        private PoolSettings pool = PoolSettings.defaults();
        private SftpSessionContext sessionContext = ThreadLocalSftpSessionContext.instance();

        private Builder(String providerName) {
            this.providerName = Objects.requireNonNull(providerName, "providerName");
        }

        public Builder config(SftpConnectionConfig config) {
            this.config = Objects.requireNonNull(config, "config");
            return this;
        }

        public Builder pool(PoolSettings pool) {
            this.pool = Objects.requireNonNull(pool, "pool");
            return this;
        }

        /** Where the client looks for, and {@code withSession} records, the current session. */
        public Builder sessionContext(SftpSessionContext sessionContext) {
            this.sessionContext = Objects.requireNonNull(sessionContext, "sessionContext");
            return this;
        }

        public SftpClient blocking() {
            SftpConnectionConfig config = requireConfig();
            SftpProvider provider = provider(providerName);
            log.debug("Creating a blocking client for {} using provider '{}' (available: {})", config,
                    providerName, availableProviders());
            return new DefaultSftpClient(provider, BoundedSftpSessionPool.blocking(provider, config,
                    pool.maxSize(), pool.maxIdle(), pool.acquireTimeout()), sessionContext);
        }

        public ReactiveSftpClient reactive(Executor worker) {
            return buildReactive(providerName, requireConfig(), Objects.requireNonNull(worker, "worker"),
                    pool);
        }

        private SftpConnectionConfig requireConfig() {
            if (config == null) {
                throw new IllegalStateException("no connection config: call config(...) before building");
            }
            return config;
        }
    }
}
