package io.github.jtsirogiannos.sftp.client.tenancy;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.core.DefaultSftpClient;
import io.github.jtsirogiannos.sftp.client.core.ObservedProvider;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A client confined to one directory, against the in-memory provider: every path goes in rewritten
 * and comes out in the view's own terms, and nothing that could name a path outside gets through.
 */
public class RootedSftpClientTest {

    private ObservedProvider provider;
    private BoundedSftpSessionPool<SftpSession> pool;
    private SftpClient server;
    private SftpClient acme;

    @BeforeMethod
    public void setUp() {
        provider = new ObservedProvider();
        pool = provider.pool(4);
        server = new DefaultSftpClient(provider, pool);
        server.mkdirs("/tenants/acme");
        server.mkdirs("/tenants/globex");
        server.upload("/tenants/globex/secret.txt", text("globex only"));
        acme = RootedSftpClient.of(server, "/tenants/acme");
    }

    @AfterMethod
    public void tearDown() {
        server.close();
    }

    @Test
    public void writesInsideTheRootAndReportsPathsInTheViewsTerms() {
        acme.mkdirs("/inbound/2026");
        acme.upload("/inbound/2026/orders.csv", text("a,b"));

        assertThat(server.exists("/tenants/acme/inbound/2026/orders.csv")).isTrue();
        assertThat(server.exists("/inbound")).describedAs("nothing at the server root").isFalse();

        assertThat(acme.list("/inbound/2026")).extracting(RemoteFile::path)
                .containsExactly("/inbound/2026/orders.csv");
        RemoteFile root = acme.stat("/");
        assertThat(root.path()).isEqualTo("/");
        assertThat(root.name()).describedAs("named like a provider names '/'").isEmpty();
        assertThat(acme.realpath("inbound/./2026/..")).isEqualTo("/inbound");
    }

    @Test
    public void relativePathsResolveAgainstTheRootNotTheLoginDirectory() throws Exception {
        acme.mkdirs("inbound");
        acme.upload("inbound/relative.txt", text("rel"));

        assertThat(server.exists("/tenants/acme/inbound/relative.txt")).isTrue();
        ByteArrayOutputStream read = new ByteArrayOutputStream();
        acme.download("inbound/relative.txt", read);
        assertThat(read.toString(StandardCharsets.UTF_8)).isEqualTo("rel");
    }

    @Test
    public void refusesEveryWayOutAndTouchesNothing() {
        for (String escape : List.of("/../globex/secret.txt", "../globex/secret.txt",
                "/inbound/../../globex", "/.\\./globex", "~/x", "")) {
            assertThatThrownBy(() -> acme.exists(escape))
                    .describedAs(escape)
                    .isInstanceOfSatisfying(SftpException.class,
                            e -> assertThat(e.isPermissionDenied()).isTrue());
        }
        assertThatThrownBy(() -> acme.rename("/x", "/../globex/stolen.txt"))
                .isInstanceOf(SftpException.class);
        assertThatThrownBy(() -> acme.download("/../globex/secret.txt"))
                .isInstanceOf(SftpException.class);

        assertThat(server.list("/tenants/globex")).extracting(RemoteFile::name)
                .containsExactly("secret.txt");
    }

    @Test
    public void linksMadeThroughTheViewCannotPointOutOfIt() {
        acme.upload("/target.txt", text("t"));

        acme.symlink("/absolute-link", "/target.txt");
        assertThat(server.readlink("/tenants/acme/absolute-link"))
                .describedAs("an absolute target is rewritten into the root")
                .isEqualTo("/tenants/acme/target.txt");
        assertThat(acme.readlink("/absolute-link")).isEqualTo("/target.txt");

        acme.mkdir("/dir");
        acme.symlink("/dir/relative-link", "../target.txt");
        assertThat(acme.readlink("/dir/relative-link"))
                .describedAs("a relative target is stored as written")
                .isEqualTo("../target.txt");

        assertThatThrownBy(() -> acme.symlink("/dir/escape", "../../globex/secret.txt"))
                .isInstanceOfSatisfying(SftpException.class,
                        e -> assertThat(e.isPermissionDenied()).isTrue());
        assertThatThrownBy(() -> acme.symlink("/escape", "/../globex/secret.txt"))
                .isInstanceOf(SftpException.class);
    }

    @Test
    public void aLinkPlantedFromOutsideIsNotRevealed() {
        server.symlink("/tenants/acme/planted", "/tenants/globex/secret.txt");

        assertThatThrownBy(() -> acme.readlink("/planted"))
                .describedAs("its target lies outside, so it is refused rather than shown")
                .isInstanceOfSatisfying(SftpException.class,
                        e -> assertThat(e.isPermissionDenied()).isTrue());
    }

    @Test
    public void deleteRecursivelyClearsInsideButNeverTheRootItself() {
        acme.mkdirs("/inbound/deep");
        acme.upload("/inbound/deep/a.txt", text("a"));

        assertThatThrownBy(() -> acme.deleteRecursively("/")).isInstanceOf(SftpException.class);
        assertThatThrownBy(() -> acme.deleteRecursively("/inbound/..")).isInstanceOf(SftpException.class);
        assertThat(acme.deleteRecursively("/inbound")).isEqualTo(3);

        assertThat(server.exists("/tenants/acme")).isTrue();
        assertThat(server.exists("/tenants/globex/secret.txt")).isTrue();
    }

    @Test
    public void uploadAtomicallyKeepsItsScratchFileInsideTheRoot() {
        acme.mkdir("/inbound");

        acme.uploadAtomically("/inbound/orders.csv", text("published"));

        assertThat(server.list("/tenants/acme/inbound")).extracting(RemoteFile::name)
                .containsExactly("orders.csv");
        for (String outside : List.of("/", "/tenants")) {
            assertThat(server.list(outside)).extracting(RemoteFile::name)
                    .describedAs("no scratch file left in " + outside)
                    .noneMatch(name -> name.endsWith(".part"));
        }
    }

    @Test
    public void visitStreamsWithPathsInTheViewsTerms() {
        acme.mkdir("/walk");
        acme.upload("/walk/a.txt", text("a"));
        acme.upload("/walk/b.txt", text("b"));

        List<String> seen = new ArrayList<>();
        acme.visit("/walk", entry -> seen.add(entry.path()));

        assertThat(seen).containsExactlyInAnyOrder("/walk/a.txt", "/walk/b.txt");
        assertThat(acme.findFirst("/walk", f -> f.name().startsWith("b")))
                .map(RemoteFile::path).contains("/walk/b.txt");
    }

    @Test
    public void sessionsOnTheViewAreConfinedAndJoinedByTheView() {
        int before = provider.sessions.size();
        acme.withSessionWithoutResult(ops -> {
            ops.mkdir("/in-session");
            acme.upload("/in-session/joined.txt", text("j")); // the view joins its own session
            assertThat(ops.list("/in-session")).extracting(RemoteFile::path)
                    .containsExactly("/in-session/joined.txt");
        });
        assertThat(provider.sessions.size() - before)
                .describedAs("one connection for the block; the view's own call joined it")
                .isLessThanOrEqualTo(1);

        try (SftpClientSession session = acme.openSession()) {
            assertThat(session.stat("/in-session").path()).isEqualTo("/in-session");
            assertThatThrownBy(() -> session.stat("/../globex")).isInstanceOf(SftpException.class);
        }
    }

    /**
     * The view must never run a call over the underlying client's unconfined session with its own
     * paths. Inside a block on the underlying client, a call on the view is rewritten first and only
     * then reaches the underlying client — which may route it to the block's session, rewritten.
     */
    @Test
    public void callsOnTheViewInsideTheUnderlyingClientsBlockAreStillConfined() {
        server.withSessionWithoutResult(ops -> acme.mkdir("/from-outer-block"));

        assertThat(server.exists("/tenants/acme/from-outer-block")).isTrue();
        assertThat(server.exists("/from-outer-block")).isFalse();
    }

    @Test
    public void closingTheViewLeavesTheUnderlyingClientOpen() {
        acme.close();

        assertThat(server.exists("/tenants/acme")).isTrue();
    }

    private static InputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
