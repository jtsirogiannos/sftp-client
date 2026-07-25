package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;

import io.github.jtsirogiannos.sftp.client.quarkus.runtime.NamedSftp;
import io.quarkus.test.QuarkusUnitTest;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots a Quarkus application whose one endpoint declares no host, and asserts the injected client
 * transfers a file. Nothing here supplies an address — if the round trip works, Dev Services started
 * a real SFTP container and contributed its host, port and credentials, which is the whole feature.
 *
 * <p>Skipped without Docker, the same contract every container-backed test in this project follows.
 * The condition gates the class before {@link QuarkusUnitTest} boots, so no application is built when
 * there is no daemon to build it against.
 */
@EnabledIf("dockerAvailable")
class SftpDevServicesTest {

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .overrideConfigKey("sftp.endpoints.dev.provider", "mina")
            .overrideConfigKey("sftp.endpoints.dev.username", "devuser")
            .overrideConfigKey("sftp.endpoints.dev.password", "devpass");
            // Deliberately no sftp.endpoints.dev.host — Dev Services must supply it.

    @Inject
    @NamedSftp("dev")
    SftpClient client;

    @Test
    void devServicesProvisionsAWorkingServer() {
        client.upload("/upload/dev.txt",
                new ByteArrayInputStream("hello dev services".getBytes(StandardCharsets.UTF_8)));
        assertThat(client.exists("/upload/dev.txt")).isTrue();

        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        long copied = client.download("/upload/dev.txt", sink);

        assertThat(copied).isEqualTo(18);
        assertThat(sink.toString(StandardCharsets.UTF_8)).isEqualTo("hello dev services");
    }

    static boolean dockerAvailable() {
        try {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }
}
