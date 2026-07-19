package io.github.jtsirogiannos.sftp.client.provider.inmemory;

import io.github.jtsirogiannos.sftp.client.api.HostKeyStrategy;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import io.github.jtsirogiannos.sftp.client.tck.SftpProviderConformanceTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * The suite again, against a fake that claims only what an SFTP v3 server offers.
 *
 * <p>This is the run that proves the capability declaration is not decoration. The conformance
 * suite branches on capabilities — rename replacing an existing file, hard links, statvfs — so a
 * provider that advertised v3 while behaving like v6 would fail here. It also gives consumers
 * something otherwise awkward to obtain: a way to test their code against v3 semantics without
 * finding a v3 server.
 */
class InMemoryVersion3ConformanceTest extends SftpProviderConformanceTest {

    private SftpProvider provider;
    private SftpSession session;

    @BeforeEach
    void open() {
        provider = InMemorySftpProvider.withCapabilities(InMemorySftpProvider.VERSION_3);
        session = provider.connect(SftpConnectionConfig.builder()
                .host("in-memory")
                .username("test")
                .password("ignored".toCharArray())
                .hostKeyStrategy(HostKeyStrategy.INSECURE_ACCEPT_ANY)
                .build());
    }

    @AfterEach
    void close() {
        if (session != null) {
            session.close();
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
