package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Runs an annotated method inside a session of the endpoint the annotation names — the advice
 * behind {@link WithSftpSession}.
 *
 * <p>The clients are looked up when a method is called rather than when this is built: it is
 * created as early as any post-processor, long before the clients exist, and pulling them forward
 * would initialise them before the rest of the context was ready.
 */
final class WithSftpSessionInterceptor implements MethodInterceptor {

    private final BeanFactory beanFactory;
    private final BlockingSessionDemarcation blocking;

    WithSftpSessionInterceptor(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        this.blocking = new BlockingSessionDemarcation(beanFactory);
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        Class<?> targetClass = invocation.getThis() == null ? invocation.getMethod().getDeclaringClass()
                : AopUtils.getTargetClass(invocation.getThis());
        WithSftpSession annotation = find(invocation.getMethod(), targetClass);
        String endpoint = annotation == null ? "" : annotation.value();
        return demarcationFor(invocation.getMethod().getReturnType()).invokeWithin(endpoint, invocation);
    }

    /** The method's own annotation if it has one, else its class's. */
    static WithSftpSession find(Method method, Class<?> targetClass) {
        Method specific = AopUtils.getMostSpecificMethod(method, targetClass);
        WithSftpSession onMethod = AnnotatedElementUtils.findMergedAnnotation(specific, WithSftpSession.class);
        return onMethod != null ? onMethod
                : AnnotatedElementUtils.findMergedAnnotation(targetClass, WithSftpSession.class);
    }

    SftpSessionDemarcation demarcationFor(Class<?> returnType) {
        List<SftpSessionDemarcation> contributed = beanFactory.getBeanProvider(SftpSessionDemarcation.class)
                .orderedStream().toList();
        for (SftpSessionDemarcation demarcation : contributed) {
            if (demarcation.supports(returnType)) {
                return demarcation;
            }
        }
        return blocking;
    }
}
