package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpConnectionConfig;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tck.OpenSshSftpServer;
import io.github.jtsirogiannos.sftp.client.tenancy.RootedSftpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A confined view over real servers, where the in-memory provider cannot reach: a server's
 * {@code realpath} resolves symlinks, so a root that is itself a link comes back under its target's
 * name, and the view has to recognise it there too.
 *
 * <p>Run against the embedded MINA server and, where Docker is available, OpenSSH — whose
 * {@code realpath} is the one most deployments will actually meet.
 */
class RootedViewIntegrationTest {

    private static final AtomicInteger UNIQUE = new AtomicInteger();
    private static EmbeddedSftpServer embedded;
    private static OpenSshSftpServer openSsh;

    @BeforeAll
    static void startServers() {
        embedded = new EmbeddedSftpServer();
        embedded.start();
        if (OpenSshSftpServer.isAvailable()) {
            openSsh = new OpenSshSftpServer();
            openSsh.start();
        }
    }

    @AfterAll
    static void stopServers() {
        embedded.close();
        if (openSsh != null) {
            openSsh.close();
        }
    }

    static Stream<Arguments> targets() {
        return Stream.of(
                Arguments.of("mina", "embedded"), Arguments.of("jsch", "embedded"),
                Arguments.of("mina", "openssh"), Arguments.of("jsch", "openssh"));
    }

    @ParameterizedTest(name = "{0} against {1}")
    @MethodSource("targets")
    void aRootThatIsASymlinkStillTranslatesCanonicalAnswers(String provider, String server) {
        try (SftpClient client = SftpClients.blocking(provider, config(server))) {
            int id = UNIQUE.incrementAndGet();
            String real = "/upload/real-" + id;
            String link = "/upload/link-" + id;
            client.mkdir(real);
            client.symlink(link, real);

            if ("openssh".equals(server)) {
                assertThat(client.realpath(link))
                        .describedAs("OpenSSH resolves the link, so the canonical branch is exercised")
                        .isEqualTo(real);
            }

            SftpClient view = RootedSftpClient.of(client, link);
            view.mkdirs("/inbound/2026");
            view.upload("/inbound/2026/orders.csv", text("a,b"));

            assertThat(client.exists(real + "/inbound/2026/orders.csv")).isTrue();
            assertThat(view.realpath("/inbound/2026"))
                    .describedAs("the server answers under " + real + "; the view under its own /")
                    .isEqualTo("/inbound/2026");
            assertThat(view.realpath("/")).isEqualTo("/");
            assertThat(view.list("/inbound/2026")).extracting(RemoteFile::path)
                    .containsExactly("/inbound/2026/orders.csv");
        }
    }

    @ParameterizedTest(name = "{0} against {1}")
    @MethodSource("targets")
    void refusesTheBackslashEscapeBeforeItReachesTheServer(String provider, String server) {
        try (SftpClient client = SftpClients.blocking(provider, config(server))) {
            String root = "/upload/backslash-root-" + UNIQUE.incrementAndGet();
            client.mkdir(root);
            client.upload(root + "-sibling.txt", text("outside"));

            SftpClient view = RootedSftpClient.of(client, root);

            assertThatThrownBy(() -> view.exists("/.\\./" + root.substring("/upload/".length())
                    + "-sibling.txt"))
                    .isInstanceOfSatisfying(SftpException.class,
                            e -> assertThat(e.isPermissionDenied()).isTrue());
        }
    }

    @ParameterizedTest(name = "{0} against {1}")
    @MethodSource("targets")
    void aLinkPlantedFromOutsideIsNotRevealed(String provider, String server) {
        try (SftpClient client = SftpClients.blocking(provider, config(server))) {
            String root = "/upload/planted-root-" + UNIQUE.incrementAndGet();
            client.mkdir(root);
            client.upload(root + "-secret.txt", text("outside"));
            client.symlink(root + "/planted", root + "-secret.txt");

            SftpClient view = RootedSftpClient.of(client, root);

            assertThatThrownBy(() -> view.readlink("/planted"))
                    .isInstanceOfSatisfying(SftpException.class,
                            e -> assertThat(e.isPermissionDenied()).isTrue());
        }
    }

    private static SftpConnectionConfig config(String server) {
        if ("openssh".equals(server)) {
            Assumptions.assumeTrue(openSsh != null, "Docker is not available; skipping OpenSSH");
            return openSsh.config();
        }
        return embedded.config();
    }

    private static ByteArrayInputStream text(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
