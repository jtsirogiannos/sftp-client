package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;

import org.springframework.aop.framework.autoproxy.AbstractBeanFactoryAwareAdvisingPostProcessor;
import org.springframework.aop.support.ComposablePointcut;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

/**
 * Puts {@link WithSftpSession} methods behind a session, and refuses a misuse of it when the context
 * starts rather than on first call.
 *
 * <p>Modelled on Spring's own {@code MethodValidationPostProcessor}: it proxies the beans that need
 * it and nothing else, and needs no auto-proxy creator of its own. Class-level and method-level
 * annotations are both matched.
 *
 * <p>Refused at startup, naming the bean and the method:
 * <ul>
 *   <li>an endpoint that is not configured under {@code sftp.endpoints};</li>
 *   <li>an empty value when more or fewer than one endpoint is configured;</li>
 *   <li>a return type nothing here can demarcate — a {@code CompletionStage}, a publisher other than
 *       Reactor's, a Kotlin {@code suspend} function, or a {@code Mono}/{@code Flux} without the
 *       reactive starter on the classpath.</li>
 * </ul>
 */
public class WithSftpSessionPostProcessor extends AbstractBeanFactoryAwareAdvisingPostProcessor {

    private BeanFactory beanFactory;
    private WithSftpSessionInterceptor interceptor;

    public WithSftpSessionPostProcessor() {
        // The Spring Boot default: subclass-based proxies, so a bean is still injectable by its class.
        setProxyTargetClass(true);
        setBeforeExistingAdvisors(true);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        super.setBeanFactory(beanFactory);
        this.beanFactory = beanFactory;
        this.interceptor = new WithSftpSessionInterceptor(beanFactory);
        ComposablePointcut pointcut = new ComposablePointcut(
                new AnnotationMatchingPointcut(WithSftpSession.class, true))
                .union(new AnnotationMatchingPointcut(null, WithSftpSession.class, true));
        this.advisor = new DefaultPointcutAdvisor(pointcut, interceptor);
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> type = ClassUtils.getUserClass(bean);
        List<String> problems = problems(type);
        if (!problems.isEmpty()) {
            throw new BeanCreationException(beanName,
                    "@WithSftpSession cannot be honoured on " + type.getName() + ":\n  "
                            + String.join("\n  ", problems));
        }
        return super.postProcessAfterInitialization(bean, beanName);
    }

    private List<String> problems(Class<?> type) {
        boolean onClass = AnnotatedElementUtils.hasAnnotation(type, WithSftpSession.class);
        List<Method> annotated = new ArrayList<>();
        ReflectionUtils.doWithMethods(type, method -> {
            if (Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers())
                    && method.getDeclaringClass() != Object.class
                    && (onClass || AnnotatedElementUtils.hasAnnotation(method, WithSftpSession.class))) {
                annotated.add(method);
            }
        });
        if (annotated.isEmpty()) {
            return List.of();
        }
        Set<String> endpoints = SftpClientProperties.bind(beanFactory.getBean(Environment.class))
                .getEndpoints().keySet();
        List<String> problems = new ArrayList<>();
        for (Method method : annotated) {
            WithSftpSession annotation = WithSftpSessionInterceptor.find(method, type);
            String endpoint = annotation.value();
            String where = method.getName() + "()";
            if (endpoint.isEmpty() && endpoints.size() != 1) {
                problems.add(where + " names no endpoint, and " + endpoints.size() + " are configured "
                        + endpoints + "; say which, as @WithSftpSession(\"name\")");
            } else if (!endpoint.isEmpty() && !endpoints.contains(endpoint)) {
                problems.add(where + " names endpoint '" + endpoint + "', which is not configured; "
                        + "configured: " + endpoints);
            }
            String unsupported = unsupportedReturn(method);
            if (unsupported != null) {
                problems.add(where + " " + unsupported);
            }
        }
        return problems;
    }

    private String unsupportedReturn(Method method) {
        Class<?> returnType = method.getReturnType();
        Class<?>[] parameters = method.getParameterTypes();
        if (parameters.length > 0
                && "kotlin.coroutines.Continuation".equals(parameters[parameters.length - 1].getName())) {
            return "is a Kotlin suspend function; use SuspendingSftpClient.withSession { } inside it";
        }
        boolean reactive = "reactor.core.publisher.Mono".equals(returnType.getName())
                || "reactor.core.publisher.Flux".equals(returnType.getName());
        if (reactive) {
            return interceptor.demarcationFor(returnType) instanceof BlockingSessionDemarcation
                    ? "returns " + returnType.getSimpleName() + ", which needs "
                            + "sftp-client-spring-boot-starter-reactive on the classpath"
                    : null;
        }
        if (CompletionStage.class.isAssignableFrom(returnType)
                || java.util.concurrent.Flow.Publisher.class.isAssignableFrom(returnType)
                || "org.reactivestreams.Publisher".equals(returnType.getName())
                || hasInterface(returnType, "org.reactivestreams.Publisher")) {
            return "returns " + returnType.getSimpleName() + ", which completes after the method "
                    + "returns, and a session cannot be demarcated around it; return Mono or Flux, or "
                    + "use the client's withSession";
        }
        return null;
    }

    private static boolean hasInterface(Class<?> type, String name) {
        for (Class<?> candidate : ClassUtils.getAllInterfacesForClassAsSet(type)) {
            if (candidate.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
