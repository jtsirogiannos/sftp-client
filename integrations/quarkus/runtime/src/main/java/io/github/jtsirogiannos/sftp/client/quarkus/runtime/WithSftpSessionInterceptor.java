package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;

import io.quarkus.arc.Arc;
import io.smallrye.mutiny.Uni;

import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Default;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;

import java.lang.annotation.Annotation;

/**
 * Runs a {@link WithSftpSession} method inside a session of the endpoint it names — Hibernate
 * Reactive Panache's {@code @WithSession}, for SFTP.
 *
 * <p>A method returning {@link Uni} runs through the endpoint's {@link MutinySftpClient}: the method
 * is invoked when its {@code Uni} is subscribed, inside the session's pipeline, and the session is
 * kept on the request's Vert.x context until the {@code Uni} completes. Any other method runs through
 * the blocking {@link SftpClient}, its checked exceptions passed through as themselves. Return types
 * that cannot be demarcated are refused when the application is built.
 *
 * <p>The annotation lives in the dependency-free API and is not a CDI interceptor binding there; the
 * extension registers it as one, with {@code value} non-binding, so this one interceptor serves every
 * endpoint.
 */
@WithSftpSession
@Interceptor
@Priority(Interceptor.Priority.PLATFORM_BEFORE + 200)
public class WithSftpSessionInterceptor {

    @AroundInvoke
    Object aroundInvoke(InvocationContext context) throws Exception {
        WithSftpSession binding = context.getInterceptorBinding(WithSftpSession.class);
        String endpoint = binding == null ? "" : binding.value();
        if (Uni.class.isAssignableFrom(context.getMethod().getReturnType())) {
            return lookup(MutinySftpClient.class, endpoint).withSession(operations -> proceed(context));
        }
        try {
            return lookup(SftpClient.class, endpoint).withSession(operations -> {
                try {
                    return context.proceed();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception checked) {
                    throw new Carried(checked);
                }
            });
        } catch (Carried carried) {
            throw (Exception) carried.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private static Uni<Object> proceed(InvocationContext context) {
        try {
            Object result = context.proceed();
            return result != null ? (Uni<Object>) result : Uni.createFrom().failure(
                    new NullPointerException(context.getMethod() + " returned null"));
        } catch (Exception e) {
            return Uni.createFrom().failure(e);
        }
    }

    private static <T> T lookup(Class<T> type, String endpoint) {
        Annotation qualifier = endpoint.isEmpty() ? Default.Literal.INSTANCE : new NamedSftp.Literal(endpoint);
        return Arc.container().select(type, qualifier).get();
    }

    /** A checked exception on its way out of a lambda; never seen by anyone else. */
    private static final class Carried extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Carried(Exception cause) {
            super(null, cause, false, false);
        }
    }
}
