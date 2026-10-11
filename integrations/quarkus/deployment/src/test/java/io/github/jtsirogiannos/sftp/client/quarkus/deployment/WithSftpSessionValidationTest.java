package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A {@code @WithSftpSession} the application could not honour fails the build, naming the method and
 * the reason, rather than the first request in production.
 */
class WithSftpSessionValidationTest {

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar.addClasses(Misused.class))
            .overrideConfigKey("sftp.endpoints.solo.provider", "mina")
            .overrideConfigKey("sftp.endpoints.solo.host", "localhost")
            .overrideConfigKey("sftp.endpoints.solo.username", "nobody")
            .overrideConfigKey("sftp.endpoints.solo.password", "unused")
            .overrideConfigKey("sftp.endpoints.solo.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .assertException(failure -> {
                Throwable root = failure;
                StringBuilder messages = new StringBuilder();
                while (root != null) {
                    messages.append(root.getMessage()).append('\n');
                    root = root.getCause();
                }
                assertThat(messages.toString())
                        .contains("Misused.unknownEndpoint() names endpoint 'nowhere', which is not configured")
                        .contains("Misused.manyItems() returns io.smallrye.mutiny.Multi");
            });

    @ApplicationScoped
    public static class Misused {

        @WithSftpSession("nowhere")
        public void unknownEndpoint() {
        }

        @WithSftpSession
        public Multi<String> manyItems() {
            return Multi.createFrom().empty();
        }
    }

    @Test
    void theBuildFails() {
        fail("the application should not have been built");
    }
}
