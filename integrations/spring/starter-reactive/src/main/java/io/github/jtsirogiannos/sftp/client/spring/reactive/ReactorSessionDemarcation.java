package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.spring.SftpSessionDemarcation;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.beans.factory.BeanFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Runs a {@code @WithSftpSession} method returning {@link Mono} or {@link Flux} inside a session of
 * the endpoint's {@link ReactorSftpClient}.
 *
 * <p>The method itself is invoked when the returned publisher is subscribed, inside the session's
 * pipeline: that is when its work runs, and the only way its calls on the client can see the
 * session. A method that does something at assembly time therefore does it at subscription — the
 * same trade Spring's reactive transaction support makes.
 */
final class ReactorSessionDemarcation implements SftpSessionDemarcation {

    private final BeanFactory beanFactory;

    ReactorSessionDemarcation(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public boolean supports(Class<?> returnType) {
        return Mono.class.isAssignableFrom(returnType) || Flux.class.isAssignableFrom(returnType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object invokeWithin(String endpoint, MethodInvocation invocation) {
        ReactorSftpClient client = endpoint.isEmpty()
                ? beanFactory.getBean(ReactorSftpClient.class)
                : beanFactory.getBean("reactorSftpClient-" + endpoint, ReactorSftpClient.class);
        if (Mono.class.isAssignableFrom(invocation.getMethod().getReturnType())) {
            return client.withSession(operations -> (Mono<Object>) proceed(invocation, Mono::error));
        }
        return client.withSessionMany(operations -> (Flux<Object>) proceed(invocation, Flux::error));
    }

    /** The method's publisher, or one that fails with whatever the method threw. */
    private static Object proceed(MethodInvocation invocation,
            java.util.function.Function<Throwable, Object> failed) {
        try {
            Object result = invocation.proceed();
            return result != null ? result : failed.apply(
                    new NullPointerException(invocation.getMethod() + " returned null"));
        } catch (Throwable e) {
            return failed.apply(e);
        }
    }
}
