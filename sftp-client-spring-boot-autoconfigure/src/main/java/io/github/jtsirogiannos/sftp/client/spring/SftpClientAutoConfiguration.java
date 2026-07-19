package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Wires the library into Spring Boot: one {@link SftpClient} (and reactive variant) per configured
 * {@code sftp.endpoints.*} entry, plus the worker executor the reactive facade needs.
 *
 * <p>The registration itself lives in {@link SftpClientRegistrar}, because the set of beans depends
 * on configuration and must be contributed before the container decides what exists.
 *
 * <p>This module staying small is the point. It is a config mapping and a bean registration: no
 * transfer logic, no threading, no reactive plumbing. If it ever needs more than that, the core is
 * not actually framework-neutral and Phase 0 has regressed.
 */
@AutoConfiguration
@ConditionalOnClass(SftpProvider.class)
@EnableConfigurationProperties(SftpClientProperties.class)
public class SftpClientAutoConfiguration {

    /**
     * Worker threads for the reactive facade's transfers.
     *
     * <p>Bounded on purpose: an SFTP transfer occupies a thread for its duration, so an unbounded
     * pool would hide that cost until the box runs out of threads. Declared
     * {@link ConditionalOnMissingBean} so an application that already has a managed executor can
     * supply its own under this bean name.
     *
     * <p>Returned as {@link ExecutorService} rather than {@code Executor} so Spring can find the
     * {@code shutdown} destroy method by type.
     */
    @Bean(name = SftpClientRegistrar.WORKER_EXECUTOR_BEAN, destroyMethod = "shutdown")
    @ConditionalOnMissingBean(name = SftpClientRegistrar.WORKER_EXECUTOR_BEAN)
    public ExecutorService sftpWorkerExecutor() {
        return Executors.newFixedThreadPool(8);
    }

    /**
     * Static, and deliberately so: a {@code BeanDefinitionRegistryPostProcessor} must be
     * instantiated before the rest of the container, and a non-static factory method would drag this
     * whole configuration class into that early phase — which Spring warns about and which would
     * silently disable the annotations on it.
     */
    @Bean
    public static SftpClientRegistrar sftpClientRegistrar() {
        return new SftpClientRegistrar();
    }
}
