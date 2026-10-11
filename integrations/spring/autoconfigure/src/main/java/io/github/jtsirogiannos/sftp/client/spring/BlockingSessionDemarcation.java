package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.beans.factory.BeanFactory;

/**
 * Runs a blocking method inside {@link SftpClient#withSession}, its checked exceptions and all.
 *
 * <p>{@code withSession} takes a {@code Function}, which cannot throw a checked exception, and a
 * method body can. The exception is carried out of the block in a private wrapper and rethrown as
 * itself, so the caller sees exactly what the method threw.
 */
final class BlockingSessionDemarcation implements SftpSessionDemarcation {

    private final BeanFactory beanFactory;

    BlockingSessionDemarcation(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public boolean supports(Class<?> returnType) {
        return true;
    }

    @Override
    public Object invokeWithin(String endpoint, MethodInvocation invocation) throws Throwable {
        SftpClient client = endpoint.isEmpty()
                ? beanFactory.getBean(SftpClient.class)
                : beanFactory.getBean("sftpClient-" + endpoint, SftpClient.class);
        try {
            return client.withSession(operations -> {
                try {
                    return invocation.proceed();
                } catch (RuntimeException | Error e) {
                    throw e;
                } catch (Throwable checked) {
                    throw new Carried(checked);
                }
            });
        } catch (Carried carried) {
            throw carried.getCause();
        }
    }

    /** A checked exception on its way out of a lambda; never seen by anyone else. */
    private static final class Carried extends RuntimeException {

        private static final long serialVersionUID = 1L;

        Carried(Throwable cause) {
            super(null, cause, false, false);
        }
    }
}
