package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.MultiTenantReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.MultiTenantSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.TenantResolver;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider;
import io.github.jtsirogiannos.sftp.client.tenancy.TenantContext;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.env.Environment;

import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Registers one {@link SftpClient} and one {@link ReactiveSftpClient} per configured endpoint,
 * bean-named after the endpoint key — the Spring mirror of the Quarkus {@code @NamedSftp} qualifier.
 *
 * <pre>{@code
 * sftp.endpoints.bank-a.host=...   ->  bean "sftpClient-bank-a", "reactiveSftpClient-bank-a"
 * }</pre>
 *
 * <p>Injection therefore looks like {@code @Qualifier("sftpClient-bank-a") SftpClient client}. When
 * exactly one endpoint is configured its beans are also marked primary, so the overwhelmingly common
 * single-endpoint case is a plain {@code @Autowired SftpClient} with no qualifier at all.
 *
 * <p><strong>Tenancy.</strong> With {@code sftp.endpoints.<name>.tenancy.enabled=true} the two beans
 * are a {@link MultiTenantSftpClient} and a {@link MultiTenantReactiveSftpClient} instead — still
 * injectable as the plain types. How tenants are isolated comes from an {@code SftpTenantProvider}
 * bean, named {@code sftpTenantProvider-<name>} or the only one, else from
 * {@code tenancy.root-directory}; which tenant is current from a {@code TenantResolver} bean, named
 * {@code tenantResolver-<name>} or the only one, else {@link TenantContext}.
 *
 * <p><strong>Why a {@link BeanDefinitionRegistryPostProcessor} and not a {@code @Bean} method.</strong>
 * The number of beans depends on configuration, so the definitions have to be contributed before the
 * container decides what exists. The cost is that this runs before {@code @ConfigurationProperties}
 * binding, so it binds {@link SftpClientProperties} straight off the {@link Environment} with
 * {@code Binder} rather than injecting the bound object. The properties bean is still registered for
 * everyone else — see {@link SftpClientAutoConfiguration}.
 *
 * <p><strong>Configuration is validated here, at startup.</strong> Both the provider lookup and the
 * config mapping happen while definitions are being registered, so a missing
 * {@code sftp-provider-*} artifact or an endpoint with no credentials stops the context from
 * starting, with the error naming the endpoint. The alternative — discovering it on the first
 * request, in production, at 3am — is the failure mode ROADMAP Phase 3 explicitly calls out.
 * Connecting is still lazy: building a client resolves the provider and creates a pool, but opens no
 * session until something borrows one.
 */
public class SftpClientRegistrar implements BeanDefinitionRegistryPostProcessor, BeanFactoryAware {

    static final String WORKER_EXECUTOR_BEAN = "sftpWorkerExecutor";

    private ConfigurableListableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        // BeanFactoryAware is honoured for post-processors; EnvironmentAware is not, because the
        // processor that applies it is itself a BeanPostProcessor and has not run yet. So the
        // Environment is fetched from here rather than injected.
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        SftpClientProperties properties =
                SftpClientProperties.bind(beanFactory.getBean(Environment.class));

        Map<String, SftpClientProperties.Endpoint> endpoints = properties.getEndpoints();
        boolean single = endpoints.size() == 1;

        for (Map.Entry<String, SftpClientProperties.Endpoint> entry : endpoints.entrySet()) {
            String name = entry.getKey();
            SftpClientProperties.Endpoint endpoint = entry.getValue();

            String providerName = endpoint.getProvider();
            SftpConnectionConfig config;
            io.github.jtsirogiannos.sftp.client.pool.PoolSettings pool;
            try {
                // Fail now, naming the endpoint. A bare "host must not be blank" from deep in a
                // builder is useless when six endpoints are configured.
                config = Mappers.toConfig(endpoint);
                pool = Mappers.toPoolSettings(endpoint);
                SftpClients.provider(providerName);
            } catch (RuntimeException e) {
                throw new IllegalStateException(
                        "sftp.endpoints." + name + " is misconfigured: " + e.getMessage(), e);
            }

            if (endpoint.getTenancy().isEnabled()) {
                Tenancy tenancy = tenancy(name, endpoint);
                registry.registerBeanDefinition("sftpClient-" + name,
                        multiTenantDefinition(providerName, config, pool, tenancy, single));
                registry.registerBeanDefinition("reactiveSftpClient-" + name,
                        multiTenantReactiveDefinition(providerName, config, pool, tenancy, single));
                continue;
            }
            registry.registerBeanDefinition("sftpClient-" + name,
                    blockingDefinition(providerName, config, pool, single));
            registry.registerBeanDefinition("reactiveSftpClient-" + name,
                    reactiveDefinition(providerName, config, pool, single));
        }
    }

    /** What an endpoint's tenants need, gathered and checked while definitions are registered. */
    private record Tenancy(String endpoint, String rootDirectoryTemplate,
            io.github.jtsirogiannos.sftp.client.pool.PoolSettings tenantPool) {
    }

    /**
     * Checked now, so that a tenancy that could never work stops the context from starting: either a
     * root directory template, or an {@code SftpTenantProvider} bean, must say how tenants are
     * isolated. User beans are already defined when this runs, since configuration classes are parsed
     * first.
     */
    private Tenancy tenancy(String name, SftpClientProperties.Endpoint endpoint) {
        String template = endpoint.getTenancy().getRootDirectory();
        try {
            if (template != null) {
                SftpTenantProvider.rootDirectoryTemplate(template);
            } else if (beanFactory.getBeanNamesForType(SftpTenantProvider.class, true, false).length == 0) {
                throw new IllegalArgumentException("tenancy is enabled but nothing says how tenants are "
                        + "isolated: set sftp.endpoints." + name + ".tenancy.root-directory, or define an "
                        + "SftpTenantProvider bean (named sftpTenantProvider-" + name + " if there are several)");
            }
            return new Tenancy(name, template, Mappers.toTenantPoolSettings(endpoint));
        } catch (RuntimeException e) {
            throw new IllegalStateException(
                    "sftp.endpoints." + name + " is misconfigured: " + e.getMessage(), e);
        }
    }

    /** The endpoint's own provider bean, else the only one, else its root directory template. */
    private SftpTenantProvider tenantProvider(Tenancy tenancy) {
        String named = "sftpTenantProvider-" + tenancy.endpoint();
        if (beanFactory.containsBean(named)) {
            return beanFactory.getBean(named, SftpTenantProvider.class);
        }
        SftpTenantProvider unique = beanFactory.getBeanProvider(SftpTenantProvider.class).getIfUnique();
        return unique != null ? unique : SftpTenantProvider.rootDirectoryTemplate(tenancy.rootDirectoryTemplate());
    }

    /** The endpoint's own resolver bean, else the only one, else {@link TenantContext}. */
    private TenantResolver tenantResolver(Tenancy tenancy) {
        String named = "tenantResolver-" + tenancy.endpoint();
        if (beanFactory.containsBean(named)) {
            return beanFactory.getBean(named, TenantResolver.class);
        }
        TenantResolver unique = beanFactory.getBeanProvider(TenantResolver.class).getIfUnique();
        return unique != null ? unique : TenantContext.resolver();
    }

    private RootBeanDefinition multiTenantDefinition(String provider, SftpConnectionConfig config,
            io.github.jtsirogiannos.sftp.client.pool.PoolSettings pool, Tenancy tenancy, boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(MultiTenantSftpClient.class);
        definition.setInstanceSupplier(() -> SftpClients.multiTenant(provider, config)
                .pool(pool)
                .tenantPool(tenancy.tenantPool())
                .tenants(tenantProvider(tenancy))
                .tenantResolver(tenantResolver(tenancy))
                .build());
        definition.setDestroyMethodName("close");
        definition.setPrimary(primary);
        return definition;
    }

    private RootBeanDefinition multiTenantReactiveDefinition(String provider, SftpConnectionConfig config,
            io.github.jtsirogiannos.sftp.client.pool.PoolSettings pool, Tenancy tenancy, boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(MultiTenantReactiveSftpClient.class);
        definition.setInstanceSupplier(() -> SftpClients.multiTenant(provider, config)
                .pool(pool)
                .tenantPool(tenancy.tenantPool())
                .tenants(tenantProvider(tenancy))
                .tenantResolver(tenantResolver(tenancy))
                .buildReactive(beanFactory.getBean(WORKER_EXECUTOR_BEAN, Executor.class)));
        definition.setDestroyMethodName("close");
        definition.setPrimary(primary);
        return definition;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        // Nothing to do: everything happens at definition-registration time.
    }

    private RootBeanDefinition blockingDefinition(String provider, SftpConnectionConfig config,
                                                  io.github.jtsirogiannos.sftp.client.pool.PoolSettings pool,
                                                  boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(SftpClient.class);
        definition.setInstanceSupplier(() -> SftpClients.blocking(provider, config, pool));
        // Closing the client closes its pool, so the context shutting down releases the connections.
        definition.setDestroyMethodName("close");
        definition.setPrimary(primary);
        return definition;
    }

    private RootBeanDefinition reactiveDefinition(String provider, SftpConnectionConfig config,
                                                  io.github.jtsirogiannos.sftp.client.pool.PoolSettings pool,
                                                  boolean primary) {
        RootBeanDefinition definition = new RootBeanDefinition(ReactiveSftpClient.class);
        // Resolved lazily inside the supplier: the executor is an ordinary bean, and this
        // post-processor runs long before it exists.
        definition.setInstanceSupplier(() -> SftpClients.reactive(provider, config,
                beanFactory.getBean(WORKER_EXECUTOR_BEAN, Executor.class), pool));
        definition.setDestroyMethodName("close");
        definition.setPrimary(primary);
        return definition;
    }
}
