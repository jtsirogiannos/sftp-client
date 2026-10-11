package io.github.jtsirogiannos.sftp.client.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Runs the annotated method — or every method of the annotated class — as one unit of work over one
 * connection: the declarative form of {@link SftpClient#withSession}.
 *
 * <pre>{@code
 * @WithSftpSession("bank-a")
 * public void settle(Batch batch) {
 *     client.mkdirs(batch.directory());          // the injected client: same connection
 *     batch.files().forEach(f -> client.upload(f.path(), f.data()));
 *     auditor.record(batch);                     // and anything it calls, on this thread
 * }
 * }</pre>
 *
 * <p>Hibernate's {@code @Transactional}-scoped session, for SFTP — and, as there, not a transaction:
 * SFTP has no rollback. A nested annotated method joins the session already current. The method's
 * own exceptions propagate unchanged.
 *
 * <p>Honoured by the Spring Boot starter and the Quarkus extension, which validate it when the
 * application starts: an unknown endpoint, an empty value with more than one endpoint configured, and
 * a return type the framework cannot demarcate all fail there rather than on first call.
 *
 * <ul>
 *   <li>Blocking methods everywhere.</li>
 *   <li>{@code Mono} and {@code Flux} in Spring, with the reactive starter.</li>
 *   <li>{@code Uni} in Quarkus — as Hibernate Reactive Panache's {@code @WithSession}.</li>
 * </ul>
 *
 * <p>In Spring, demarcation happens on the proxy, so a method calling another of its own class's
 * annotated methods does not start a session — the usual Spring AOP caveat. Quarkus intercepts those
 * calls too.
 *
 * <p>Deliberately not a CDI interceptor binding here: this module has no dependencies, so the
 * Quarkus extension registers it as one.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface WithSftpSession {

    /**
     * The endpoint to demarcate, as named under {@code sftp.endpoints}. Empty means the only
     * endpoint there is, and is refused when there is more than one.
     */
    String value() default "";
}
