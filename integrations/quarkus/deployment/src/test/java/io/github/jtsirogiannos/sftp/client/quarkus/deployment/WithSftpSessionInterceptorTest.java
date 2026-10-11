package io.github.jtsirogiannos.sftp.client.quarkus.deployment;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.MutinySftpClient;
import io.github.jtsirogiannos.sftp.client.quarkus.runtime.VertxSftpSessionContext;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code @WithSftpSession} in a Quarkus application: a session current for the annotated method's
 * extent — the blocking client's for a blocking method, the Mutiny client's, kept on the request's
 * Vert.x context, for one returning {@code Uni}.
 */
class WithSftpSessionInterceptorTest {

    static final EmbeddedSftpServer SERVER = new EmbeddedSftpServer();
    static final AtomicInteger UNIQUE = new AtomicInteger();

    static {
        SERVER.start();
        System.setProperty("sftp-annotated-test.port", String.valueOf(SERVER.port()));
    }

    @RegisterExtension
    static final QuarkusUnitTest TEST = new QuarkusUnitTest()
            .withApplicationRoot(jar -> jar.addClasses(Archiver.class, Auditor.class))
            .overrideConfigKey("sftp.endpoints.solo.provider", "mina")
            .overrideConfigKey("sftp.endpoints.solo.host", "localhost")
            .overrideConfigKey("sftp.endpoints.solo.port", "${sftp-annotated-test.port}")
            .overrideConfigKey("sftp.endpoints.solo.username", EmbeddedSftpServer.USERNAME)
            .overrideConfigKey("sftp.endpoints.solo.password", EmbeddedSftpServer.PASSWORD)
            .overrideConfigKey("sftp.endpoints.solo.host-key-strategy", "INSECURE_ACCEPT_ANY")
            .overrideConfigKey("sftp.endpoints.solo.pool.max-size", "1")
            .overrideConfigKey("sftp.endpoints.solo.pool.acquire-timeout", "500ms");

    @ApplicationScoped
    public static class Archiver {

        @Inject
        SftpClient client;

        @Inject
        MutinySftpClient mutiny;

        @Inject
        Auditor auditor;

        @WithSftpSession
        public boolean archive(String dir) {
            client.mkdir(dir);
            auditor.record(dir); // another bean, its own annotation: joins
            return client.exists(dir + "/audit");
        }

        @WithSftpSession("solo")
        public boolean sessionIsCurrent() {
            return !client.sessionContext().capture().isEmpty();
        }

        public boolean sessionIsCurrentWithoutAnnotation() {
            return !client.sessionContext().capture().isEmpty();
        }

        @WithSftpSession
        public String tryASecondConnection() {
            try (var second = client.openSession()) {
                return "got one";
            } catch (RuntimeException e) {
                return e.getMessage();
            }
        }

        @WithSftpSession
        public void failsWithAChecked() throws IOException {
            throw new IOException("the caller's own, checked");
        }

        /** Its calls join only if the session is current along the whole pipeline: a pool of one. */
        @WithSftpSession
        public Uni<Boolean> publish(String dir) {
            return mutiny.mkdir(dir)
                    .chain(() -> mutiny.mkdir(dir + "/joined"))
                    .chain(() -> Uni.createFrom().item(
                            () -> !VertxSftpSessionContext.instance().capture().isEmpty()));
        }
    }

    @ApplicationScoped
    public static class Auditor {

        @Inject
        SftpClient client;

        @WithSftpSession
        public void record(String dir) {
            client.mkdir(dir + "/audit");
        }
    }

    @Inject
    Archiver archiver;

    @Inject
    Vertx vertx;

    private static String scratch() {
        return "/upload/annotated-" + UNIQUE.incrementAndGet();
    }

    @Test
    void aBlockingMethodRunsWithASessionCurrentAndOnlyIt() {
        assertThat(archiver.sessionIsCurrent()).isTrue();
        assertThat(archiver.sessionIsCurrentWithoutAnnotation()).isFalse();
        assertThat(archiver.archive(scratch())).isTrue();
    }

    @Test
    void theSessionIsHeldForTheWholeMethod() {
        assertThat(archiver.tryASecondConnection()).contains("all 1 are in use");
    }

    @Test
    void theMethodsOwnCheckedExceptionArrivesUnchanged() {
        assertThatThrownBy(() -> archiver.failsWithAChecked())
                .isInstanceOf(IOException.class)
                .hasMessage("the caller's own, checked");
        assertThat(archiver.sessionIsCurrent()).describedAs("the connection came back").isTrue();
    }

    @Test
    void aUniMethodKeepsItsSessionAlongThePipeline() throws Exception {
        assertThat(onRequestContext(() -> archiver.publish(scratch()))).isTrue();
    }

    private <T> T onRequestContext(Supplier<Uni<T>> uni) throws Exception {
        Context request = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        CompletableFuture<T> result = new CompletableFuture<>();
        request.runOnContext(ignored -> uni.get().subscribe().with(result::complete, result::completeExceptionally));
        return result.get(10, TimeUnit.SECONDS);
    }
}
