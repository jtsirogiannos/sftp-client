package io.github.jtsirogiannos.sftp.client.provider.jsch;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tck.SftpProviderConformanceTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * The same suite the MINA provider runs, against a v3 stop-and-wait backend.
 *
 * <p>Note what the fixture is: MINA SSHD's <em>server</em>. Testing JSch against a real SSH server
 * with no Docker is the point of that choice (ROADMAP Phase 2) — the client side here is entirely
 * JSch, so anything the two implementations disagree about shows up.
 */
class JschConformanceTest extends SftpProviderConformanceTest {

    private EmbeddedSftpServer server;
    private SftpProvider provider;
    private SftpSession session;

    @BeforeEach
    void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        provider = new JschSftpProvider();
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
