package io.github.jtsirogiannos.sftp.client.spring;

import org.aopalliance.intercept.MethodInvocation;

/**
 * How a method annotated {@link io.github.jtsirogiannos.sftp.client.api.WithSftpSession} is run
 * inside a session, for the return types one module knows about.
 *
 * <p>The blocking case is built in. The reactive starter contributes one for {@code Mono} and
 * {@code Flux} as a bean, which is how this module supports them without depending on Reactor.
 */
public interface SftpSessionDemarcation {

    /** Whether this demarcation can run a method returning {@code returnType}. */
    boolean supports(Class<?> returnType);

    /** Runs {@code invocation} inside a session of the endpoint named {@code endpoint}. */
    Object invokeWithin(String endpoint, MethodInvocation invocation) throws Throwable;
}
