package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Selects which configured endpoint to inject.
 *
 * <pre>{@code
 * @Inject @NamedSftp("bank-a") MutinySftpClient client;
 * }</pre>
 *
 * <p>The value matches the key in {@code sftp.endpoints.<name>.*}. When exactly one endpoint is
 * configured its beans are also registered as {@code @Default}, so the common case needs no
 * qualifier at all.
 */
@Qualifier
@Retention(RUNTIME)
@Target({FIELD, METHOD, PARAMETER, TYPE})
public @interface NamedSftp {

    String value();

    /** For programmatic lookup: {@code Arc.container().instance(SftpClient.class, new NamedSftp.Literal("bank-a"))}. */
    final class Literal extends AnnotationLiteral<NamedSftp> implements NamedSftp {

        private static final long serialVersionUID = 1L;

        private final String value;

        public Literal(String value) {
            this.value = value;
        }

        @Override
        public String value() {
            return value;
        }
    }
}
