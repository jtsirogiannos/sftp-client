package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.quarkus.test.QuarkusUnitTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** A multi-tenant endpoint with nothing to say how tenants are isolated fails the build. */
class SftpTenancyValidationTest {

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .overrideConfigKey("sftp.endpoints.partners.provider", "mina")
            .overrideConfigKey("sftp.endpoints.partners.host", "localhost")
            .overrideConfigKey("sftp.endpoints.partners.username", "nobody")
            .overrideConfigKey("sftp.endpoints.partners.password", "unused")
            .overrideConfigKey("sftp.endpoints.partners.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .overrideConfigKey("sftp.endpoints.partners.tenancy.enabled", "true")
            .assertException(failure -> {
                StringBuilder messages = new StringBuilder();
                for (Throwable t = failure; t != null; t = t.getCause()) {
                    messages.append(t.getMessage()).append('\n');
                }
                assertThat(messages.toString())
                        .contains("sftp.endpoints.partners.tenancy is enabled but nothing says how tenants are isolated");
            });

    @Test
    void theBuildFails() {
        fail("the application should not have been built");
    }
}
