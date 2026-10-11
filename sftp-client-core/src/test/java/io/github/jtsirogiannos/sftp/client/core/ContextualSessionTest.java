package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SessionBindings;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The current session: inside {@code withSession}, calls on the client itself travel over the
 * block's connection. Measured by how many connections the provider was asked for — a call that
 * borrowed instead of joining needs a second one, because the block's is checked out.
 */
public class ContextualSessionTest {

    @Test
    public void callsOnTheClientInsideABlockJoinItsSession() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops -> archive(client));

            assertThat(provider.sessions)
                    .describedAs("the service only had the client, and still used the block's connection")
                    .hasSize(1);
            assertThat(client.exists("/archive/2026/report.csv")).isTrue();
        }
    }

    /** What a service written against the injected client looks like: it knows nothing of sessions. */
    private static void archive(SftpClient client) {
        client.mkdirs("/archive/2026");
        client.upload("/archive/2026/report.csv", text("a,b"));
        client.setAttributes("/archive/2026/report.csv",
                io.github.jtsirogiannos.sftp.client.api.AttributeUpdate.builder().permissions(0640).build());
    }

    @Test
    public void aNestedBlockJoinsTheOuterOneInsteadOfWaitingForItself() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            AtomicReference<SftpOperations> inner = new AtomicReference<>();
            SftpOperations outer = client.withSession(o -> {
                client.withSessionWithoutResult(inner::set);
                return o;
            });

            assertThat(inner.get()).describedAs("joined, not a second connection").isSameAs(outer);
            assertThat(pool.idleSessions()).describedAs("closed once, by the block that opened it")
                    .isEqualTo(1);
        }
    }

    @Test
    public void theBindingEndsWithTheBlock() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops -> ops.mkdir("/a"));
            client.mkdir("/b"); // must borrow the recycled session, not touch a closed one

            assertThat(provider.sessions).hasSize(1);
            assertThat(ThreadLocalSftpSessionContext.instance().capture().isEmpty()).isTrue();
        }
    }

    @Test
    public void theBindingEndsWithTheBlockEvenWhenItThrows() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            assertThatThrownBy(() -> client.withSessionWithoutResult(ops -> {
                throw new IllegalStateException("boom");
            })).isInstanceOf(IllegalStateException.class);

            assertThat(ThreadLocalSftpSessionContext.instance().capture().isEmpty()).isTrue();
        }
    }

    @Test
    public void downloadReturningAStreamIsNeverRoutedAndOutlivesTheBlock() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {
            client.upload("/src.txt", text("payload"));
            int before = provider.sessions.size();

            InputStream escaped = client.withSession(ops -> client.download("/src.txt"));

            try (escaped) {
                assertThat(new String(escaped.readAllBytes(), StandardCharsets.UTF_8))
                        .describedAs("still readable: it was never on the block's session")
                        .isEqualTo("payload");
            }
            assertThat(provider.sessions.size() - before)
                    .describedAs("the block's connection plus the stream's own")
                    .isEqualTo(2);
        }
    }

    @Test
    public void aSessionInTheMiddleOfAReadIsNotJoined() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {
            client.upload("/src.txt", text("payload"));

            client.withSessionWithoutResult(ops -> {
                try (InputStream in = ops.download("/src.txt")) {
                    // The obvious copy: through the client, while the block's session is reading.
                    client.upload("/dst.txt", in);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });

            assertThat(client.exists("/dst.txt")).isTrue();
        }
    }

    @Test
    public void anOpenedSessionIsNeverCurrent() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool);
             SftpClientSession opened = client.openSession()) {

            opened.mkdir("/opened");
            client.mkdir("/separately");

            assertThat(provider.sessions).describedAs("the client did not join it").hasSize(2);
        }
    }

    @Test
    public void openSessionInsideABlockIsStillANewConnection() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops -> {
                try (SftpClientSession second = client.openSession()) {
                    assertThat(second).isNotSameAs(ops);
                }
            });

            assertThat(provider.sessions).hasSize(2);
        }
    }

    /**
     * The deadlock caveat, pinned: a borrow made inside a block on a full pool waits for its slot and
     * then fails with the pool's message. It must not hang, and it must not quietly join.
     */
    @Test
    public void aBorrowInsideABlockOnAFullPoolTimesOutRatherThanHanging() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops ->
                    assertThatThrownBy(client::openSession)
                            .isInstanceOf(SftpException.class)
                            .hasMessageContaining("all 1 are in use"));
        }
    }

    @Test
    public void aSessionClosedOutFromUnderTheBindingIsNotJoined() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops -> {
                ((SftpClientSession) ops).close(); // as if closed from another thread
                client.mkdir("/after"); // must borrow, not use a connection now back in the pool
            });

            assertThat(client.exists("/after")).isTrue();
        }
    }

    @Test
    public void theBindingBelongsToTheThreadThatMadeIt() throws Exception {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultSftpClient client = new DefaultSftpClient(provider, pool)) {

            client.withSessionWithoutResult(ops -> {
                ops.mkdir("/main");
                try {
                    CompletableFuture.runAsync(() -> client.mkdir("/elsewhere"))
                            .get(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });

            assertThat(provider.sessions)
                    .describedAs("a single-owner session must not be shared with another thread")
                    .hasSize(2);
        }
    }

    @Test
    public void twoClientsNeverShareASession() {
        ObservedProvider provider = new ObservedProvider();
        try (BoundedSftpSessionPool<SftpSession> poolA = provider.pool(1);
             BoundedSftpSessionPool<SftpSession> poolB = provider.pool(1);
             DefaultSftpClient a = new DefaultSftpClient(provider, poolA);
             DefaultSftpClient b = new DefaultSftpClient(provider, poolB)) {

            a.withSessionWithoutResult(ops -> b.mkdir("/b"));

            assertThat(provider.sessions).hasSize(2);
            assertThat(poolB.idleSessions()).isEqualTo(1);
        }
    }

    @Test
    public void theBuilderWiresTheContextTheClientUses() {
        AtomicInteger scopes = new AtomicInteger();
        SftpSessionContext counting = new SftpSessionContext() {
            @Override
            public SessionBindings capture() {
                return ThreadLocalSftpSessionContext.instance().capture();
            }

            @Override
            public <R> R callWith(SessionBindings bindings, Supplier<R> work) {
                scopes.incrementAndGet();
                return ThreadLocalSftpSessionContext.instance().callWith(bindings, work);
            }
        };

        try (SftpClient client = SftpClients.builder("inmemory")
                .config(ObservedProvider.config())
                .pool(PoolSettings.defaults().withAcquireTimeout(Duration.ofSeconds(1)))
                .sessionContext(counting)
                .blocking()) {

            client.withSessionWithoutResult(ops -> client.mkdir("/via-builder"));

            assertThat(client.sessionContext()).isSameAs(counting);
            assertThat(scopes.get()).isEqualTo(1);
            assertThat(client.exists("/via-builder")).isTrue();
        }
    }

    private static InputStream text(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
