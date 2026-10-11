package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.NamedSftp;
import io.quarkus.test.QuarkusUnitTest;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Boots a real Quarkus application against the TCK's in-process SFTP server and injects the beans
 * the extension is supposed to create.
 *
 * <p>The server has to start before Quarkus does, because its port is only known once it is
 * listening — hence the static block and the system property the config expression reads. That is
 * also why there is a single test class covering both endpoints: {@link QuarkusUnitTest} builds one
 * application per class, and building two to assert the same wiring twice would double the slowest
 * part of the suite for nothing.
 */
class SftpClientExtensionTest {

    static final EmbeddedSftpServer SERVER = new EmbeddedSftpServer();

    static {
        SERVER.start();
        System.setProperty("sftp-test.port", String.valueOf(SERVER.port()));
        System.setProperty("sftp-test.user", EmbeddedSftpServer.USERNAME);
        System.setProperty("sftp-test.password", EmbeddedSftpServer.PASSWORD);
    }

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .overrideConfigKey("sftp.endpoints.bank-a.provider", "mina")
            .overrideConfigKey("sftp.endpoints.bank-a.host", "localhost")
            .overrideConfigKey("sftp.endpoints.bank-a.port", "${sftp-test.port}")
            .overrideConfigKey("sftp.endpoints.bank-a.username", "${sftp-test.user}")
            .overrideConfigKey("sftp.endpoints.bank-a.password", "${sftp-test.password}")
            .overrideConfigKey("sftp.endpoints.bank-a.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .overrideConfigKey("sftp.endpoints.bank-b.provider", "mina")
            .overrideConfigKey("sftp.endpoints.bank-b.host", "localhost")
            .overrideConfigKey("sftp.endpoints.bank-b.port", "${sftp-test.port}")
            .overrideConfigKey("sftp.endpoints.bank-b.username", "${sftp-test.user}")
            .overrideConfigKey("sftp.endpoints.bank-b.password", "${sftp-test.password}")
            .overrideConfigKey("sftp.endpoints.bank-b.host-key-strategy", "INSECURE_ACCEPT_ANY")
            // bank-b doubles as the pool-tuning fixture: a single session and a short acquire
            // timeout make "the settings reached the pool" observable as a second borrow failing.
            // Its other tests run one operation at a time, which a one-session pool serves fine.
            .overrideConfigKey("sftp.endpoints.bank-b.pool.max-size", "1")
            .overrideConfigKey("sftp.endpoints.bank-b.pool.acquire-timeout", "150ms");

    @Inject
    @NamedSftp("bank-a")
    SftpClient bankA;

    @Inject
    @NamedSftp("bank-b")
    SftpClient bankB;

    @Inject
    @NamedSftp("bank-a")
    MutinySftpClient reactiveBankA;

    /**
     * The pool config must actually reach the pool, not merely bind: with max-size 1, holding
     * bank-b's only connection makes a second one time out. Two openSession calls rather than
     * nested withSession blocks, which now share one connection.
     */
    @Test
    void poolConfigReachesThePool() {
        try (var held = bankB.openSession()) {
            org.assertj.core.api.Assertions.assertThatThrownBy(bankB::openSession)
                    .hasMessageContaining("all 1 are in use");
        }
    }

    @Test
    void injectsADistinctClientPerConfiguredEndpoint() {
        assertThat(bankA).isNotNull();
        assertThat(bankB).isNotNull();
        assertThat(bankA).isNotSameAs(bankB);
    }

    @Test
    void theInjectedBlockingClientTransfersAFile() {
        bankA.upload("/upload/quarkus.txt",
                new ByteArrayInputStream("via quarkus".getBytes(StandardCharsets.UTF_8)));
        assertThat(bankA.exists("/upload/quarkus.txt")).isTrue();

        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        long copied = bankA.download("/upload/quarkus.txt", sink);

        assertThat(copied).isEqualTo(11);
        assertThat(sink.toString(StandardCharsets.UTF_8)).isEqualTo("via quarkus");
    }

    /** The Mutiny facade is the whole reactive integration; if it works, the type shim is right. */
    @Test
    void theInjectedMutinyClientTransfersAFile() {
        reactiveBankA.mkdir("/upload/mutiny").await().indefinitely();
        assertThat(reactiveBankA.exists("/upload/mutiny").await().indefinitely()).isTrue();
        assertThat(reactiveBankA.list("/upload").await().indefinitely())
                .extracting(f -> f.name())
                .contains("mutiny");
    }

    @Test
    void capabilitiesComeFromTheConfiguredProvider() {
        assertThat(bankA.capabilities().maxSftpVersion()).isEqualTo(6);
        assertThat(bankA.capabilities().pipelinedStreaming()).isTrue();
    }
}
