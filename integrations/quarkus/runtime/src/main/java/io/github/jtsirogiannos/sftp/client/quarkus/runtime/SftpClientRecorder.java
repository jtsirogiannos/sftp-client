package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tenancy.MultiTenantSftpClientBuilder;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider;
import io.github.jtsirogiannos.sftp.client.tenancy.TenantContext;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableInstance;
import io.quarkus.arc.SyntheticCreationalContext;
import io.quarkus.runtime.RuntimeValue;
import io.quarkus.runtime.annotations.Recorder;

import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Builds the per-endpoint clients at runtime init.
 *
 * <p>A recorder exists because the build step runs during augmentation, where the real objects
 * cannot be constructed: what it produces instead is a recipe replayed when the application starts.
 * Everything here is therefore deliberately dull — read config, call the same
 * {@code SftpClients} bootstrap a plain-Java user would.
 *
 * <p>Note what is <em>not</em> here: no transfer logic, no threading, no reactive plumbing. Same
 * test as the Spring starter — if this file grows past config translation, the core is not actually
 * framework-neutral.
 */
@Recorder
public class SftpClientRecorder {

    private final RuntimeValue<SftpRuntimeConfig> config;

    /**
     * Run-time config arrives through the constructor, not as a build-step parameter: since Quarkus
     * 3.x a build step cannot consume run-time config at all, because augmentation happens before
     * that config exists. The {@link RuntimeValue} is a placeholder during the build and holds the
     * real mapping by the time any method below runs.
     */
    public SftpClientRecorder(RuntimeValue<SftpRuntimeConfig> config) {
        this.config = config;
    }

    public Function<SyntheticCreationalContext<SftpClient>, SftpClient> blockingClient(
            String name, String provider) {
        return context -> {
            SftpRuntimeConfig.EndpointRuntimeConfig endpoint = endpoint(config.getValue(), name);
            // The current session lives on the request's duplicated context, as Hibernate Reactive's
            // does, falling back to a thread local off it.
            return SftpClients.builder(provider)
                    .config(connectionConfig(endpoint, name))
                    .pool(poolSettings(endpoint))
                    .sessionContext(VertxSftpSessionContext.instance())
                    .blocking();
        };
    }

    /**
     * The Mutiny client wraps a neutral reactive one. The wrapper owns the delegate and closes it,
     * so the endpoint gets exactly one pool rather than one per facade.
     */
    public Function<SyntheticCreationalContext<MutinySftpClient>, MutinySftpClient> mutinyClient(
            String name, String provider, Executor worker) {
        return context -> {
            SftpRuntimeConfig.EndpointRuntimeConfig endpoint = endpoint(config.getValue(), name);
            ReactiveSftpClient delegate = SftpClients.builder(provider)
                    .config(connectionConfig(endpoint, name))
                    .pool(poolSettings(endpoint))
                    .sessionContext(VertxSftpSessionContext.instance())
                    .reactive(worker);
            return new MutinySftpClient(delegate);
        };
    }

    /**
     * A multi-tenant client for an endpoint whose tenancy the build enabled. Isolation comes from an
     * {@link SftpTenantProvider} bean, qualified {@code @NamedSftp(name)} or the default one, else
     * from {@code tenancy.root-directory}; the current tenant from a {@link TenantResolver} bean found
     * the same way, else {@code TenantContext}.
     */
    public Function<SyntheticCreationalContext<MultiTenantSftpClient>, MultiTenantSftpClient> multiTenantClient(
            String name, String provider) {
        return context -> multiTenantBuilder(name, provider).build();
    }

    public Function<SyntheticCreationalContext<MutinySftpClient>, MutinySftpClient> multiTenantMutinyClient(
            String name, String provider, Executor worker) {
        return context -> new MutinySftpClient(multiTenantBuilder(name, provider).buildReactive(worker));
    }

    private MultiTenantSftpClientBuilder multiTenantBuilder(String name, String provider) {
        SftpRuntimeConfig.EndpointRuntimeConfig endpoint = endpoint(config.getValue(), name);
        SftpRuntimeConfig.EndpointRuntimeConfig.TenancyConfig.TenantPoolConfig tenantPool =
                endpoint.tenancy().pool();
        return SftpClients.multiTenant(provider, connectionConfig(endpoint, name))
                .pool(poolSettings(endpoint))
                .tenantPool(new io.github.jtsirogiannos.sftp.client.pool.PoolSettings(
                        tenantPool.maxSize(), tenantPool.maxIdle(), tenantPool.acquireTimeout()))
                .sessionContext(VertxSftpSessionContext.instance())
                .tenants(tenantProvider(name, endpoint))
                .tenantResolver(tenantResolver(name));
    }

    private static SftpTenantProvider tenantProvider(String name,
            SftpRuntimeConfig.EndpointRuntimeConfig endpoint) {
        SftpTenantProvider bean = bean(SftpTenantProvider.class, name);
        if (bean != null) {
            return bean;
        }
        return endpoint.tenancy().rootDirectory()
                .map(SftpTenantProvider::rootDirectoryTemplate)
                .orElseThrow(() -> new IllegalStateException("sftp.endpoints." + name
                        + ".tenancy is enabled but nothing says how tenants are isolated: set "
                        + "sftp.endpoints." + name + ".tenancy.root-directory, or provide an "
                        + "SftpTenantProvider bean"));
    }

    private static TenantResolver tenantResolver(String name) {
        TenantResolver bean = bean(TenantResolver.class, name);
        return bean != null ? bean : TenantContext.resolver();
    }

    /** The bean qualified for this endpoint, else the default one, else null. */
    private static <T> T bean(Class<T> type, String name) {
        InjectableInstance<T> named = Arc.container().select(type, new NamedSftp.Literal(name));
        if (named.isResolvable()) {
            return named.get();
        }
        InjectableInstance<T> unqualified = Arc.container().select(type);
        return unqualified.isResolvable() ? unqualified.get() : null;
    }

    private static SftpRuntimeConfig.EndpointRuntimeConfig endpoint(SftpRuntimeConfig config,
            String name) {
        SftpRuntimeConfig.EndpointRuntimeConfig endpoint = config.endpoints().get(name);
        if (endpoint == null) {
            // The build fixed the endpoint names, so a mismatch means run-time config dropped one.
            throw new IllegalStateException("no runtime configuration for sftp endpoint '" + name
                    + "'; it was present at build time. Check that sftp.endpoints." + name
                    + ".* is set in this environment.");
        }
        return endpoint;
    }

    private static io.github.jtsirogiannos.sftp.client.pool.PoolSettings poolSettings(
            SftpRuntimeConfig.EndpointRuntimeConfig endpoint) {
        SftpRuntimeConfig.EndpointRuntimeConfig.PoolConfig pool = endpoint.pool();
        return new io.github.jtsirogiannos.sftp.client.pool.PoolSettings(
                pool.maxSize(), pool.maxIdle(), pool.acquireTimeout());
    }

    private static SftpConnectionConfig connectionConfig(
            SftpRuntimeConfig.EndpointRuntimeConfig endpoint, String name) {

        SftpConnectionConfig.Builder builder = SftpConnectionConfig.builder()
                .host(endpoint.host())
                .port(endpoint.port())
                .username(endpoint.username())
                .hostKeyStrategy(endpoint.hostKeyStrategy())
                .connectTimeout(endpoint.connectTimeout())
                .requestTimeout(endpoint.requestTimeout());

        endpoint.password().filter(p -> !p.isEmpty()).map(String::toCharArray).ifPresent(builder::password);
        endpoint.privateKey().ifPresent(builder::privateKey);
        endpoint.passphrase().filter(p -> !p.isEmpty()).map(String::toCharArray).ifPresent(builder::passphrase);
        // Only override when set: passing null would wipe the builder's ~/.ssh/known_hosts default,
        // and STRICT (the default strategy) rejects a null known-hosts file.
        endpoint.knownHosts().ifPresent(builder::knownHosts);

        try {
            return builder.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "sftp.endpoints." + name + " is misconfigured: " + e.getMessage(), e);
        }
    }
}
