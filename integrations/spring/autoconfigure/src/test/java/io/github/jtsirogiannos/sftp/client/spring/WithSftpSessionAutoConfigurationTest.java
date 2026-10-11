package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code @WithSftpSession} in a Spring application: the annotated method's calls on the injected
 * client share one connection, and a misuse stops the context from starting.
 *
 * <p>Whether calls join is the core's business and is tested there; what this proves is that the
 * annotation makes a session current for the method's extent — and only for it.
 */
class WithSftpSessionAutoConfigurationTest {

    private static final AtomicInteger UNIQUE = new AtomicInteger();

    private EmbeddedSftpServer server;

    @BeforeEach
    void startServer() {
        server = new EmbeddedSftpServer();
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SftpClientAutoConfiguration.class))
                .withPropertyValues(endpoint("bank-a"))
                .withPropertyValues("sftp.endpoints.bank-a.pool.max-size=1",
                        "sftp.endpoints.bank-a.pool.acquire-timeout=300ms");
    }

    private String[] endpoint(String name) {
        return new String[] {
                "sftp.endpoints." + name + ".provider=mina",
                "sftp.endpoints." + name + ".host=localhost",
                "sftp.endpoints." + name + ".port=" + server.port(),
                "sftp.endpoints." + name + ".username=" + EmbeddedSftpServer.USERNAME,
                "sftp.endpoints." + name + ".password=" + EmbeddedSftpServer.PASSWORD,
                "sftp.endpoints." + name + ".host-key-strategy=insecure_accept_any",
        };
    }

    private static String scratch() {
        return "/upload/with-session-" + UNIQUE.incrementAndGet();
    }

    // --- the beans an application would write ---

    public static class Archiver {

        private final SftpClient client;
        private final Auditor auditor;

        public Archiver(SftpClient client, Auditor auditor) {
            this.client = client;
            this.auditor = auditor;
        }

        @WithSftpSession
        public boolean archive(String dir) {
            client.mkdir(dir);
            auditor.record(dir); // another bean, its own annotation: joins
            return client.exists(dir + "/audit");
        }

        @WithSftpSession
        public boolean sessionIsCurrent() {
            return !client.sessionContext().capture().isEmpty();
        }

        /** The same question, asked without the annotation. */
        public boolean sessionIsCurrentWithoutAnnotation() {
            return !client.sessionContext().capture().isEmpty();
        }

        @WithSftpSession("bank-a")
        public void failsWithAChecked(String dir) throws IOException {
            client.mkdir(dir);
            throw new IOException("the caller's own, checked");
        }
    }

    public static class Auditor {

        private final SftpClient client;

        public Auditor(SftpClient client) {
            this.client = client;
        }

        @WithSftpSession
        public void record(String dir) {
            client.mkdir(dir + "/audit");
        }
    }

    @WithSftpSession("bank-a")
    public static class ClassLevel {

        private final SftpClient client;

        public ClassLevel(SftpClient client) {
            this.client = client;
        }

        public boolean twoCalls(String dir) {
            client.mkdir(dir);
            return client.exists(dir);
        }

        public String tryASecondConnection() {
            try (var second = client.openSession()) {
                return "got one";
            } catch (RuntimeException e) {
                return e.getMessage();
            }
        }
    }

    public static class UnknownEndpoint {
        @WithSftpSession("nowhere")
        public void work() {
        }
    }

    public static class AsyncResult {
        @WithSftpSession
        public CompletionStage<Void> work() {
            return CompletableFuture.completedFuture(null);
        }
    }

    // --- behaviour ---

    @Test
    void anAnnotatedMethodRunsWithASessionCurrentAndOnlyIt() {
        runner().withBean(Auditor.class).withBean(Archiver.class).run(context -> {
            assertThat(context).hasNotFailed();
            Archiver archiver = context.getBean(Archiver.class);
            assertThat(archiver.sessionIsCurrent()).isTrue();
            assertThat(archiver.sessionIsCurrentWithoutAnnotation()).isFalse();
            assertThat(archiver.archive(scratch())).isTrue();
        });
    }

    /**
     * A pool of one held by the annotated method: a second, independent connection cannot be had
     * while it runs — the method really does hold one for its whole extent.
     */
    @Test
    void theSessionIsHeldForTheWholeMethod() {
        runner().withBean(ClassLevel.class).run(context -> {
            SftpClient client = context.getBean(SftpClient.class);
            ClassLevel bean = context.getBean(ClassLevel.class);
            assertThat(bean.tryASecondConnection())
                    .describedAs("the method's own session occupies the only slot")
                    .contains("all 1 are in use");
            assertThat(client.exists("/upload")).describedAs("and gives it back after").isTrue();
        });
    }

    @Test
    void aClassLevelAnnotationCoversEveryPublicMethod() {
        runner().withBean(ClassLevel.class).run(context ->
                assertThat(context.getBean(ClassLevel.class).twoCalls(scratch())).isTrue());
    }

    @Test
    void theMethodsOwnCheckedExceptionArrivesUnchanged() {
        runner().withBean(Auditor.class).withBean(Archiver.class).run(context -> {
            Archiver archiver = context.getBean(Archiver.class);
            assertThatThrownBy(() -> archiver.failsWithAChecked(scratch()))
                    .isInstanceOf(IOException.class)
                    .hasMessage("the caller's own, checked");
            // A pool of one: this only completes if the connection was handed back.
            assertThat(archiver.archive(scratch())).isTrue();
        });
    }

    // --- refused at startup ---

    @Test
    void anUnknownEndpointStopsTheContext() {
        runner().withBean(UnknownEndpoint.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("'nowhere', which is not configured");
        });
    }

    @Test
    void anUnnamedEndpointWithSeveralConfiguredStopsTheContext() {
        runner().withPropertyValues(endpoint("bank-b")).withBean(Auditor.class, () -> new Auditor(null))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining("names no endpoint, and 2 are configured");
                });
    }

    @Test
    void aReturnTypeThatCompletesLaterStopsTheContext() {
        runner().withBean(AsyncResult.class).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasMessageContaining("returns CompletionStage");
        });
    }
}
