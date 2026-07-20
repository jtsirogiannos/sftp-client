package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client facades, driven through real providers, against a real server.
 *
 * <p>Every test here exists because something in this exact combination broke and nothing caught it.
 * The unit tests in core drive the same code against an in-memory fake, and a fake agrees with
 * whatever the code believes: it happily removed directories that no server will remove via
 * {@code SSH_FXP_REMOVE}, and it reported a symlink as a symlink through a {@code stat} that on a
 * real server resolves the link. Both bugs passed a green build.
 *
 * <p>Parameterised across providers because the facade sits on the SPI, and the two backends differ
 * more than their shared interface suggests — opposite symlink argument orders, different SFTP
 * versions negotiated with the same server.
 */
class SftpClientIntegrationTest {

    private static EmbeddedSftpServer server;
    private static final AtomicInteger UNIQUE = new AtomicInteger();

    @BeforeAll
    static void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    private SftpClient client(String provider) {
        return SftpClients.blocking(provider, server.config());
    }

    /** Isolated per test: the server is shared for speed, so paths must not collide. */
    private String scratch(String label) {
        return "/upload/it-" + label + "-" + UNIQUE.incrementAndGet();
    }

    @Test
    void serviceLoaderFindsEveryProviderOnTheClasspath() {
        assertThat(SftpClients.availableProviders()).contains("mina", "jsch");
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void mkdirsCreatesNestedDirectories(String provider) {
        String base = scratch("mkdirs");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base + "/2026/07/18");

            assertThat(client.exists(base + "/2026")).isTrue();
            assertThat(client.exists(base + "/2026/07/18")).isTrue();

            client.mkdirs(base + "/2026/07/18"); // idempotent against a real server
        }
    }

    /**
     * Covers the {@code remove()} bug directly: clearing a tree has to delete directories, which is
     * a different protocol request from deleting files.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void deleteRecursivelyClearsATreeIncludingDirectories(String provider) {
        String base = scratch("tree");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base + "/nested/deeper");
            client.upload(base + "/a.txt", text("a"));
            client.upload(base + "/nested/b.txt", text("b"));
            client.upload(base + "/nested/deeper/c.txt", text("c"));

            int removed = client.deleteRecursively(base);

            assertThat(removed).describedAs("3 files + 3 directories").isEqualTo(6);
            assertThat(client.exists(base)).isFalse();
            assertThat(client.exists("/upload")).describedAs("the parent must survive").isTrue();
        }
    }

    /**
     * Ownership must be readable where the wire carries it, and honestly unknown where it does not.
     *
     * <p>JSch negotiates v3, whose stat replies carry numeric uid/gid — real values, verifiable
     * after a chown. MINA negotiates v6 against this server, and v6 replaced numeric ids with owner
     * <em>names</em>; the first version of this mapping reported MINA's zero-default as uid 0,
     * claiming every file belonged to root. UNKNOWN_ID is the honest answer there.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void ownershipIsReadableWhereTheWireCarriesIt(String provider) {
        String base = scratch("ownership");
        String path = base + "/f.txt";
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            client.upload(path, text("x"));

            RemoteFile stat = client.stat(path);
            if (client.capabilities().maxSftpVersion() >= 4) {
                assertThat(stat.ownerId())
                        .describedAs("v4+ carries owner names, not numeric ids; a number here is invented")
                        .isEqualTo(RemoteFile.UNKNOWN_ID);
            } else {
                assertThat(stat.ownerId())
                        .describedAs("v3 stat replies carry a real uid")
                        .isNotEqualTo(RemoteFile.UNKNOWN_ID)
                        .isNotEqualTo(0);
                assertThat(stat.groupId()).isNotEqualTo(RemoteFile.UNKNOWN_ID);
            }
        }
    }

    /**
     * The one that would have been an incident. {@code stat} resolves a symlink, so a traversal that
     * uses it treats a link to a directory as a directory and deletes the contents of somewhere
     * else entirely. Only {@code lstat} sees the link.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void deleteRecursivelyRemovesSymlinksWithoutFollowingThem(String provider) {
        String outside = scratch("outside");
        String tree = scratch("linked-tree");
        try (SftpClient client = client(provider)) {
            client.mkdirs(outside);
            client.upload(outside + "/precious.txt", text("do not delete"));
            client.mkdirs(tree);
            client.symlink(tree + "/link", outside);

            client.deleteRecursively(tree);

            assertThat(client.exists(tree)).isFalse();
            assertThat(client.exists(outside + "/precious.txt"))
                    .describedAs("followed the symlink and deleted outside the tree")
                    .isTrue();
        }
    }

    /**
     * Pins the argument order end to end. MINA takes (link, target) and JSch takes (target, link),
     * so a provider forwarding them unchanged creates the link backwards — and both paths exist, so
     * nothing notices until something reads it.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void symlinkTakesLinkThenTarget(String provider) {
        String base = scratch("symlink");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            client.upload(base + "/target.txt", text("pointed at"));

            client.symlink(base + "/link.txt", base + "/target.txt");

            assertThat(client.readlink(base + "/link.txt")).isEqualTo(base + "/target.txt");
            assertThat(client.stat(base + "/link.txt").symlink())
                    .describedAs("stat resolves the link")
                    .isFalse();
            assertThat(client.lstat(base + "/link.txt").symlink())
                    .describedAs("lstat reports the link itself")
                    .isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void transfersToAndFromLocalFiles(String provider) throws Exception {
        String base = scratch("paths");
        Path local = Files.createTempFile("sftp-it", ".txt");
        Path back = Files.createTempFile("sftp-it-back", ".txt");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            Files.writeString(local, "local file contents");

            client.upload(local, base + "/uploaded.txt");
            long copied = client.download(base + "/uploaded.txt", back);

            assertThat(copied).isEqualTo(19);
            assertThat(Files.readString(back)).isEqualTo("local file contents");
        } finally {
            Files.deleteIfExists(local);
            Files.deleteIfExists(back);
        }
    }

    /**
     * Streaming a listing, end to end. The visitor is the SPI primitive both backends implement
     * natively — JSch's selector returns continue-or-break, MINA pages a directory handle — so this
     * is the case where "it compiles" and "it stops" are genuinely different claims.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void visitStreamsEveryEntryAndStopsOnRequest(String provider) {
        String base = scratch("visit");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            for (int i = 0; i < 25; i++) {
                client.upload(base + "/file-" + i + ".txt", text("x"));
            }

            AtomicInteger fullWalk = new AtomicInteger();
            client.visit(base, entry -> {
                fullWalk.incrementAndGet();
                return true;
            });
            assertThat(fullWalk.get()).isEqualTo(25);

            AtomicInteger stopped = new AtomicInteger();
            client.visit(base, entry -> {
                stopped.incrementAndGet();
                return false;
            });
            assertThat(stopped.get())
                    .describedAs("the walk kept going after the visitor said stop")
                    .isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void findFirstReturnsAMatchAndAnEmptyOptionalOtherwise(String provider) {
        String base = scratch("find");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            client.upload(base + "/needle.csv", text("1"));
            client.upload(base + "/haystack.txt", text("2"));

            assertThat(client.findFirst(base, f -> f.name().endsWith(".csv")))
                    .isPresent()
                    .get()
                    .extracting(RemoteFile::name)
                    .isEqualTo("needle.csv");
            assertThat(client.findFirst(base, f -> f.name().endsWith(".zip"))).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void listFiltersEntries(String provider) {
        String base = scratch("filter");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);
            client.upload(base + "/keep.csv", text("1"));
            client.upload(base + "/skip.txt", text("2"));

            List<RemoteFile> csv = client.list(base, f -> f.name().endsWith(".csv"));

            assertThat(csv).extracting(RemoteFile::name).containsExactly("keep.csv");
        }
    }

    /**
     * Guards the pool win: a client is meant to reuse one connection, not reconnect per call.
     * Measured rather than asserted structurally — connect-per-operation cost ~21x more, and the
     * regression would be silent.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void manyOperationsReuseOneConnection(String provider) {
        String base = scratch("reuse");
        try (SftpClient client = client(provider)) {
            client.mkdirs(base);

            long start = System.nanoTime();
            for (int i = 0; i < 30; i++) {
                client.exists(base);
            }
            long perOperationMicros = (System.nanoTime() - start) / 30 / 1_000;

            // A fresh TCP connect plus SSH handshake is milliseconds even on loopback; a pooled
            // round trip is a fraction of one. The threshold is deliberately loose - this is here to
            // catch "the pool stopped pooling", not to benchmark.
            assertThat(perOperationMicros)
                    .describedAs("~%dus per op suggests every call is reconnecting", perOperationMicros)
                    .isLessThan(5_000);
        }
    }

    private static ByteArrayInputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
