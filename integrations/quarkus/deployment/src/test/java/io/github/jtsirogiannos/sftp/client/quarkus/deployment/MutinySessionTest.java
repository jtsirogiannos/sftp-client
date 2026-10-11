package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpSession;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.VertxSftpSessionContext;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Units of work in Quarkus: the current session kept on the request's Vert.x duplicated context,
 * where it survives every thread a Mutiny pipeline passes through.
 *
 * <p>The endpoint has a pool of one, so a call on the injected client inside a unit of work only
 * completes if it joins the work's session — a call that borrowed instead would wait for the
 * connection the work holds, and time out.
 */
class MutinySessionTest {

    static final EmbeddedSftpServer SERVER = new EmbeddedSftpServer();
    static final AtomicInteger UNIQUE = new AtomicInteger();

    static {
        SERVER.start();
        System.setProperty("sftp-session-test.port", String.valueOf(SERVER.port()));
    }

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .overrideConfigKey("sftp.endpoints.solo.provider", "mina")
            .overrideConfigKey("sftp.endpoints.solo.host", "localhost")
            .overrideConfigKey("sftp.endpoints.solo.port", "${sftp-session-test.port}")
            .overrideConfigKey("sftp.endpoints.solo.username", EmbeddedSftpServer.USERNAME)
            .overrideConfigKey("sftp.endpoints.solo.password", EmbeddedSftpServer.PASSWORD)
            .overrideConfigKey("sftp.endpoints.solo.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .overrideConfigKey("sftp.endpoints.solo.pool.max-size", "1")
            .overrideConfigKey("sftp.endpoints.solo.pool.acquire-timeout", "500ms");

    @Inject
    MutinySftpClient mutiny;

    @Inject
    SftpClient blocking;

    @Inject
    Vertx vertx;

    private static String scratch() {
        return "/upload/mutiny-session-" + UNIQUE.incrementAndGet();
    }

    /** Runs {@code uni} as a request would: subscribed on a fresh duplicated context. */
    private <T> T onRequestContext(Supplier<Uni<T>> uni) throws Exception {
        Context request = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        CompletableFuture<T> result = new CompletableFuture<>();
        request.runOnContext(ignored -> uni.get().subscribe().with(result::complete, result::completeExceptionally));
        return result.get(10, TimeUnit.SECONDS);
    }

    @Test
    void callsOnTheClientAnywhereInTheWorkJoinItsSession() throws Exception {
        String dir = scratch();

        Boolean joined = onRequestContext(() -> mutiny.withSession(ops -> ops.mkdir(dir)
                .chain(() -> mutiny.mkdir(dir + "/joined"))
                .chain(() -> mutiny.mkdir(dir + "/joined/again"))
                .chain(() -> mutiny.exists(dir + "/joined/again"))));

        assertThat(joined).isTrue();
    }

    /**
     * Quarkus' own worker executor carries the request's context onto its threads, so the injected
     * client would keep it either way. A client built over any other executor completes on threads
     * that know nothing of the request, and only returning on the caller's context keeps the work's
     * session visible to the next step.
     */
    @Test
    void aClientOverAPlainExecutorStillReturnsToTheRequestAndJoins() throws Exception {
        java.util.concurrent.ExecutorService plain = java.util.concurrent.Executors.newFixedThreadPool(2);
        try (MutinySftpClient own = new MutinySftpClient(
                io.github.jtsirogiannos.sftp.client.core.SftpClients.builder("mina")
                        .config(SERVER.config())
                        .pool(io.github.jtsirogiannos.sftp.client.pool.PoolSettings.defaults()
                                .withMaxSize(1).withAcquireTimeout(Duration.ofMillis(500)))
                        .sessionContext(VertxSftpSessionContext.instance())
                        .reactive(plain))) {
            String dir = scratch();

            Boolean joined = onRequestContext(() -> own.withSession(ops -> ops.mkdir(dir)
                    .chain(() -> own.mkdir(dir + "/joined"))
                    .chain(() -> own.exists(dir + "/joined"))));

            assertThat(joined).isTrue();
        } finally {
            plain.shutdownNow();
        }
    }

    @Test
    void theWorkResumesOnTheRequestsContext() throws Exception {
        String dir = scratch();

        Boolean onRequestContext = onRequestContext(() -> mutiny.mkdir(dir)
                .map(ignored -> Context.isOnVertxThread() && VertxContext.isOnDuplicatedContext()));

        assertThat(onRequestContext)
                .describedAs("a result emitted off the context would lose the request's session")
                .isTrue();
    }

    @Test
    void theBindingEndsWhenTheWorkCompletes() throws Exception {
        String dir = scratch();
        Context request = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        CompletableFuture<Boolean> afterwards = new CompletableFuture<>();

        request.runOnContext(ignored -> mutiny.withSession(ops -> ops.mkdir(dir))
                .subscribe().with(
                        done -> afterwards.complete(VertxSftpSessionContext.instance().capture().isEmpty()),
                        afterwards::completeExceptionally));

        assertThat(afterwards.get(10, TimeUnit.SECONDS))
                .describedAs("nothing left on the request's context")
                .isTrue();
    }

    @Test
    void aNestedWithSessionJoins() throws Exception {
        String dir = scratch();

        onRequestContext(() -> mutiny.withSession(outer -> outer.mkdir(dir)
                .chain(() -> mutiny.withSession(inner -> inner.mkdir(dir + "/nested")))));

        assertThat(mutiny.exists(dir + "/nested").await().atMost(Duration.ofSeconds(10))).isTrue();
    }

    @Test
    void anOpenedSessionIsTheCallersToClose() {
        String dir = scratch();

        MutinySftpSession session = mutiny.openSession().await().atMost(Duration.ofSeconds(10));
        session.mkdirs(dir + "/opened").await().atMost(Duration.ofSeconds(10));
        session.close().await().atMost(Duration.ofSeconds(10));

        assertThat(session.isOpen()).isFalse();
        assertThat(mutiny.exists(dir + "/opened").await().atMost(Duration.ofSeconds(10))).isTrue();
    }

    /** The blocking client in a Quarkus request keeps its session on the same context. */
    @Test
    void theBlockingClientJoinsOnAWorkerWithTheRequestsContext() throws Exception {
        String dir = scratch();
        Context request = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        request.executeBlocking(() -> blocking.withSession(ops -> {
            ops.mkdir(dir);
            blocking.mkdir(dir + "/joined"); // pool of one: completes only if it joins
            return blocking.exists(dir + "/joined");
        })).onComplete(outcome -> {
            if (outcome.succeeded()) {
                result.complete(outcome.result());
            } else {
                result.completeExceptionally(outcome.cause());
            }
        });

        assertThat(result.get(10, TimeUnit.SECONDS)).isTrue();
    }
}
