package io.github.jtsirogiannos.sftp.client.provider.mina;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tck.SftpProviderConformanceTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Runs the shared conformance suite against the MINA provider. The suite itself lives in the TCK
 * module; all a provider supplies is a fixture and a session.
 *
 * <p>A fresh server and session per test: the suite deliberately includes failure cases, and shared
 * state between them would make a leak in one test look like a bug in the next.
 */
class MinaConformanceTest extends SftpProviderConformanceTest {

    private EmbeddedSftpServer server;
    private SftpProvider provider;
    private SftpSession session;

    @BeforeEach
    void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        provider = new MinaSftpProvider();
        session = provider.connect(server.config());
    }

    @AfterEach
    void stopServer() {
        if (session != null) {
            session.close();
        }
        if (server != null) {
            server.close();
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
