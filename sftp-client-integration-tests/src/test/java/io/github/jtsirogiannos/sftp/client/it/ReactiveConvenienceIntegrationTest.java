package io.github.jtsirogiannos.sftp.client.it;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The convenience operations on the reactive facade, against real servers.
 *
 * <p>These are default methods composed from the reactive primitives, so neither reactive client
 * implements them — but "composes correctly" is not the same as "composes correctly when each step
 * is a separate round trip", which is what these check.
 *
 * <p>The load-bearing one is {@link #deleteRecursivelyRemovesSymlinksWithoutFollowingThem}. The
 * blocking implementation originally walked with {@code stat}, which resolves a link, so a link to a
 * directory looked like a directory and the walk deleted the contents of somewhere else. Adding a
 * reactive equivalent before {@code lstat} existed on this facade would have reintroduced exactly
 * that, which is why the primitive came first.
 */
class ReactiveConvenienceIntegrationTest {

    private static EmbeddedSftpServer server;
    private static ExecutorService worker;
    private static final AtomicInteger UNIQUE = new AtomicInteger();

    @BeforeAll
    static void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
        worker = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void stopServer() {
        worker.shutdownNow();
        server.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void mkdirsCreatesTheWholeChain(String provider) throws Exception {
        String deep = scratch("rx-mkdirs") + "/a/b/c";
        try (ReactiveSftpClient client = reactive(provider)) {
            await(client.mkdirs(deep));

            assertThat(await(client.exists(deep))).isTrue();
            // Idempotent: an existing chain is not an error.
            await(client.mkdirs(deep));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void deleteRecursivelyClearsANestedTree(String provider) throws Exception {
        String base = scratch("rx-delete");
        try (ReactiveSftpClient client = reactive(provider);
             SftpClient setup = blocking(provider)) {
            setup.mkdirs(base + "/one/two");
            setup.upload(base + "/top.txt", text("a"));
            setup.upload(base + "/one/mid.txt", text("b"));
            setup.upload(base + "/one/two/deep.txt", text("c"));

            int removed = await(client.deleteRecursively(base));

            assertThat(removed).describedAs("three files plus three directories").isEqualTo(6);
            assertThat(await(client.exists(base))).isFalse();
        }
    }

    /** The one that would have been an incident. Only {@code lstat} sees the link. */
    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void deleteRecursivelyRemovesSymlinksWithoutFollowingThem(String provider) throws Exception {
        String outside = scratch("rx-outside");
        String tree = scratch("rx-linked-tree");
        try (ReactiveSftpClient client = reactive(provider);
             SftpClient setup = blocking(provider)) {
            setup.mkdirs(outside);
            setup.upload(outside + "/precious.txt", text("do not delete"));
            setup.mkdirs(tree);
            setup.symlink(tree + "/link", outside);

            await(client.deleteRecursively(tree));

            assertThat(await(client.exists(tree))).isFalse();
            assertThat(setup.exists(outside + "/precious.txt"))
                .describedAs("followed the symlink and deleted outside the tree")
                .isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void refusesToDeleteTheRoot(String provider) {
        try (ReactiveSftpClient client = reactive(provider)) {
            assertThatThrownBy(() -> await(client.deleteRecursively("/")))
                .describedAs("a typo'd variable expanding to \"/\" must not clear the server")
                .hasMessageContaining("refusing");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void lstatSeesTheLinkWhereStatSeesTheTarget(String provider) throws Exception {
        String base = scratch("rx-lstat");
        try (ReactiveSftpClient client = reactive(provider);
             SftpClient setup = blocking(provider)) {
            setup.mkdirs(base);
            setup.upload(base + "/target.txt", text("payload"));
            setup.symlink(base + "/link", base + "/target.txt");

            assertThat(await(client.lstat(base + "/link")).symlink())
                .describedAs("lstat must report the link itself").isTrue();
            assertThat(await(client.stat(base + "/link")).symlink())
                .describedAs("stat resolves the link, so it reports the target").isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void filtersAndFindsEntries(String provider) throws Exception {
        String base = scratch("rx-filter");
        try (ReactiveSftpClient client = reactive(provider);
             SftpClient setup = blocking(provider)) {
            setup.mkdirs(base);
            setup.upload(base + "/a.csv", text("1"));
            setup.upload(base + "/b.txt", text("2"));
            setup.upload(base + "/c.csv", text("3"));

            List<RemoteFile> csv = await(client.list(base, f -> f.name().endsWith(".csv")));
            Optional<RemoteFile> first = await(client.findFirst(base, f -> f.name().equals("b.txt")));

            assertThat(csv).extracting(RemoteFile::name).containsExactlyInAnyOrder("a.csv", "c.csv");
            assertThat(first).isPresent();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void chmodSetsPermissions(String provider) throws Exception {
        String base = scratch("rx-chmod");
        try (ReactiveSftpClient client = reactive(provider);
             SftpClient setup = blocking(provider)) {
            setup.mkdirs(base);
            setup.upload(base + "/f.txt", text("x"));

            await(client.chmod(base + "/f.txt", 0600));

            assertThat(setup.stat(base + "/f.txt").permissions() & 0777).isEqualTo(0600);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mina", "jsch"})
    void realpathCanonicalises(String provider) throws Exception {
        try (ReactiveSftpClient client = reactive(provider)) {
            assertThat(await(client.realpath("/upload/../upload"))).endsWith("/upload");
        }
    }

    // --- helpers ---

    private static ReactiveSftpClient reactive(String provider) {
        return SftpClients.reactive(provider, server.config(), worker);
    }

    private static SftpClient blocking(String provider) {
        return SftpClients.blocking(provider, server.config());
    }

    private static <T> T await(java.util.concurrent.CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static ByteArrayInputStream text(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String scratch(String tag) {
        return "/upload/" + tag + "-" + UNIQUE.incrementAndGet();
    }
}
