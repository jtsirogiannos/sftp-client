package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.spring.SftpClientProperties;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.env.Environment;

/**
 * Registers one {@link ReactorSftpClient} per configured endpoint, named {@code reactorSftpClient-<key>}.
 *
 * <p>Each wraps the neutral {@code reactiveSftpClient-<key>} bean the shared auto-configuration
 * already registered, rather than building a client of its own — so an endpoint keeps exactly one
 * connection pool however many facades are injected. The wrapper does not own that delegate's
 * lifecycle here: the bean it wraps has its own {@code close} destroy method, and closing the pool
 * twice would be the only thing a second one achieved.
 *
 * <p>Same post-processor shape as the servlet registrar, and for the same reason: the number of
 * beans depends on configuration, so the definitions must be contributed before the container
 * decides what exists.
 */
public class ReactorSftpClientRegistrar implements BeanDefinitionRegistryPostProcessor, BeanFactoryAware {

    private ConfigurableListableBeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        // BeanFactoryAware is honoured for post-processors where EnvironmentAware is not; see the
        // note on the servlet registrar.
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        SftpClientProperties properties =
                SftpClientProperties.bind(beanFactory.getBean(Environment.class));

        boolean single = properties.getEndpoints().size() == 1;
        for (String name : properties.getEndpoints().keySet()) {
            String delegateBean = "reactiveSftpClient-" + name;

            RootBeanDefinition definition = new RootBeanDefinition(ReactorSftpClient.class);
            definition.setInstanceSupplier(() -> new ReactorSftpClient(
                    beanFactory.getBean(delegateBean, ReactiveSftpClient.class)));
            definition.setPrimary(single);
            registry.registerBeanDefinition("reactorSftpClient-" + name, definition);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        // Everything happens at definition-registration time.
    }
}
