package io.github.jtsirogiannos.sftp.client.tck;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;

/**
 * Runs the whole conformance suite against a containerised OpenSSH server.
 *
 * <p>A provider supplies one method:
 *
 * <pre>{@code
 * class MinaOpenSshConformanceTest extends OpenSshConformanceTestBase {
 *     protected SftpProvider createProvider() { return new MinaSftpProvider(); }
 * }
 * }</pre>
 *
 * <p>The container starts once per class rather than per test — it costs seconds, not milliseconds,
 * and the suite is written so tests do not depend on a pristine filesystem. Sessions are still
 * per-test, since several cases deliberately provoke failures.
 *
 * <p>Skipped, not failed, when Docker is unavailable: this fixture is a second opinion on top of the
 * embedded server, so a laptop without Docker should still get a meaningful build.
 */
public abstract class OpenSshConformanceTestBase extends SftpProviderConformanceTest {

    private static OpenSshSftpServer server;

    private SftpProvider provider;
    private SftpSession session;

    /** The provider under test. */
    protected abstract SftpProvider createProvider();

    @BeforeAll
    static void startContainer() {
        Assumptions.assumeTrue(OpenSshSftpServer.isAvailable(),
                "Docker is not available; skipping the OpenSSH conformance run");
        server = new OpenSshSftpServer();
        server.start();
    }

    @AfterAll
    static void stopContainer() {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @BeforeEach
    void openSession() {
        provider = createProvider();
        session = provider.connect(server.config());
    }

    @AfterEach
    void closeSession() {
        if (session != null) {
            session.close();
            session = null;
        }
    }

    @Override
    protected SftpProvider provider() {
        return provider;
    }

    @Override
    protected SftpSession session() {
        return session;
    }
}
