package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.core.SftpClients;
import io.github.jtsirogiannos.sftp.client.pool.PoolSettings;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.github.jtsirogiannos.sftp.client.tenancy.SftpTenantProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Units of work through Reactor, against a real server. Reactor's context is invisible to the
 * library, so the claim worth testing is that a call made on the client deep in a unit of work's
 * pipeline — after hopping schedulers — still lands on the work's connection. A pool of one proves
 * it: a call that failed to join would wait for the connection the work holds, and time out.
 */
class ReactorSessionTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    private EmbeddedSftpServer server;
    private ExecutorService worker;
    private final AtomicInteger unique = new AtomicInteger();

    @BeforeEach
    void start() {
        server = new EmbeddedSftpServer();
        server.start();
        worker = Executors.newFixedThreadPool(4);
    }

    @AfterEach
    void stop() {
        worker.shutdownNow();
        server.close();
    }

    private ReactorSftpClient client(int maxSize) {
        return new ReactorSftpClient(SftpClients.builder("mina")
            .config(server.config())
            .pool(PoolSettings.defaults().withMaxSize(maxSize).withAcquireTimeout(Duration.ofMillis(500)))
            .reactive(worker));
    }

    private String scratch() {
        return "/upload/reactor-session-" + unique.incrementAndGet();
    }

    private static Flux<ByteBuffer> text(String content) {
        return Flux.just(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void callsOnTheClientInsideTheWorkJoinItsSessionAcrossSchedulers() {
        try (ReactorSftpClient client = client(1)) {
            String dir = scratch();

            Boolean written = client.withSession(ops -> ops.mkdir(dir)
                    .then(Mono.defer(() -> client.mkdir(dir + "/hopped")).subscribeOn(Schedulers.boundedElastic()))
                    .then(client.upload(dir + "/hopped/f.txt", text("x")).publishOn(Schedulers.parallel()))
                    .then(client.exists(dir + "/hopped/f.txt")))
                .block(WAIT);

            assertThat(written).isTrue();
        }
    }

    @Test
    void aNestedWithSessionJoins() {
        try (ReactorSftpClient client = client(1)) {
            String dir = scratch();

            client.withSession(outer -> outer.mkdir(dir)
                    .then(client.withSession(inner -> inner.mkdir(dir + "/nested"))))
                .block(WAIT);

            assertThat(client.exists(dir + "/nested").block(WAIT)).isTrue();
        }
    }

    @Test
    void theWorksOwnErrorArrivesUnchangedAndTheConnectionIsReused() {
        try (ReactorSftpClient client = client(1)) {
            IllegalStateException rejected = new IllegalStateException("order rejected");

            assertThatThrownBy(() -> client.withSession(ops -> ops.exists("/upload")
                    .then(Mono.error(rejected))).block(WAIT))
                .isSameAs(rejected);
            assertThat(client.exists("/upload").block(WAIT)).describedAs("handed back").isTrue();
        }
    }

    @Test
    void cancellingTheWorkHandsTheConnectionBack() {
        try (ReactorSftpClient client = client(1)) {
            Disposable running = client.withSession(ops -> ops.exists("/upload").then(Mono.never()))
                .subscribe();
            Mono.delay(Duration.ofMillis(100)).block(WAIT);

            running.dispose();

            assertThat(client.exists("/upload").block(WAIT))
                .describedAs("only possible on a pool of one if the cancelled work let go")
                .isTrue();
        }
    }

    @Test
    void withSessionManyRelaysEveryItem() {
        try (ReactorSftpClient client = client(1)) {
            String dir = scratch();

            List<String> names = client.withSessionMany(ops -> ops.mkdirs(dir + "/a")
                    .then(client.mkdir(dir + "/b"))
                    .thenMany(ops.list(dir).flatMapIterable(entries -> entries))
                    .map(RemoteFile::name))
                .collectList()
                .block(WAIT);

            assertThat(names).containsExactlyInAnyOrder("a", "b");
        }
    }

    @Test
    void anOpenedSessionIsTheCallersToClose() {
        try (ReactorSftpClient client = client(2)) {
            String dir = scratch();

            ReactorSftpSession session = client.openSession().block(WAIT);
            session.mkdirs(dir + "/opened").block(WAIT);
            assertThat(session.isOpen()).isTrue();
            session.close().block(WAIT);

            assertThat(session.isOpen()).isFalse();
            assertThat(client.exists(dir + "/opened").block(WAIT)).isTrue();
        }
    }

    @Test
    void theTenantComesFromTheSubscriberContext() {
        String root = scratch();
        try (SftpClient setup = SftpClients.blocking("mina", server.config())) {
            setup.mkdirs(root + "/acme");
            setup.mkdirs(root + "/globex");
        }
        try (ReactorSftpClient client = new ReactorSftpClient(SftpClients.multiTenant("mina", server.config())
                .tenants(SftpTenantProvider.rootDirectoryTemplate(root + "/{tenant}"))
                .buildReactive(worker))) {

            client.upload("/orders.csv", text("acme's"))
                .then(client.withSession(ops -> ops.mkdir("/in-session")))
                .contextWrite(SftpReactorContext.tenant("acme"))
                .block(WAIT);

            try (SftpClient check = SftpClients.blocking("mina", server.config())) {
                assertThat(check.exists(root + "/acme/orders.csv")).isTrue();
                assertThat(check.exists(root + "/acme/in-session")).isTrue();
                assertThat(check.list(root + "/globex")).isEmpty();
            }
            assertThatThrownBy(() -> client.list("/").block(WAIT))
                .describedAs("no tenant in the context, and no resolver that knows one")
                .hasMessageContaining("No current SFTP tenant");
        }
    }
}
