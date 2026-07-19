package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.spring.SftpClientAutoConfiguration;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

import reactor.core.publisher.Flux;

/**
 * Adds the Reactor facade on top of the shared auto-configuration.
 *
 * <p>There is deliberately no {@code @ConditionalOnClass(Flux.class)}. This class ships in the
 * reactive starter, so a servlet application never has it on the classpath and there is no condition
 * to evaluate — or to get wrong. Spring Boot's own starters reach the same end by pairing a
 * condition with a dependency; skipping the condition is available to us because the auto-configured
 * classes are split across artifacts rather than sharing one.
 *
 * <p>Ordered after {@link SftpClientAutoConfiguration} because the beans registered here wrap the
 * ones it contributes.
 */
@AutoConfiguration(after = SftpClientAutoConfiguration.class)
public class ReactorSftpClientAutoConfiguration {

    /**
     * Static for the usual reason: a {@code BeanDefinitionRegistryPostProcessor} is instantiated
     * before the rest of the container, and a non-static factory method would drag this
     * configuration class into that early phase.
     */
    @Bean
    public static ReactorSftpClientRegistrar reactorSftpClientRegistrar() {
        return new ReactorSftpClientRegistrar();
    }

    /** Referenced so the dependency on Reactor is explicit rather than incidental. */
    static Class<?> reactorMarker() {
        return Flux.class;
    }
}
