package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.tck.SftpProviderConformanceTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * The same suite the real providers run — which is the point.
 *
 * <p>JSch and MINA can share a blind spot: both wrap a real SSH client, and both implementations
 * here were written against the same drafts in the same sitting. A backend with no sockets at all
 * passes only if the contract is about SFTP semantics rather than about how those two libraries
 * happen to behave. It is also the cheapest of the three to run: no server, no container, no keys.
 */
class InMemoryConformanceTest extends SftpProviderConformanceTest {

    private SftpProvider provider;
    private SftpSession session;

    @BeforeEach
    void open() {
        // A provider per test, so each case gets a filesystem of its own.
        provider = new InMemorySftpProvider();
        session = provider.connect(config());
    }

    @AfterEach
    void close() {
        if (session != null) {
            session.close();
        }
    }

    private static io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig config() {
        return io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig.builder()
                .host("in-memory")
                .username("test")
                .password("ignored".toCharArray())
                .hostKeyStrategy(io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build();
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
