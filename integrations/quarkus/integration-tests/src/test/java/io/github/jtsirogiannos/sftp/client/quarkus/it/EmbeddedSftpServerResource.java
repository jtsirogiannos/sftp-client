package io.github.jtsirogiannos.sftp.client.quarkus.it;

import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.Map;

/**
 * Starts the fixture SFTP server in the <em>test</em> JVM and tells the application which port it
 * landed on.
 *
 * <p>This indirection is what makes the native run possible. The server cannot live inside the
 * application, because under {@code @QuarkusIntegrationTest} the application is a separate process —
 * a runner jar or a native binary — with no way to hand a port back. Running it out here and passing
 * the port in as configuration works identically for both.
 *
 * <p>Only the port is overridden; the rest of the endpoint is in {@code application.properties},
 * because the extension needs the endpoint's <em>name</em> at build time to create any beans at all.
 */
public class EmbeddedSftpServerResource implements QuarkusTestResourceLifecycleManager {

    private final EmbeddedSftpServer server = new EmbeddedSftpServer();

    @Override
    public Map<String, String> start() {
        server.start();
        String port = String.valueOf(server.port());
        return Map.of("sftp.endpoints.it.port", port, "sftp.endpoints.partners.port", port);
    }

    @Override
    public void stop() {
        server.close();
    }
}
