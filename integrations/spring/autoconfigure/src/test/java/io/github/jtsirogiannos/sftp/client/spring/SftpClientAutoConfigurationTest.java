package io.github.jtsirogiannos.sftp.client.spring;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the starter the way an application would: build a context from properties, pull the beans
 * out, and use them against a real in-process SFTP server.
 *
 * <p>Asserting that bean definitions exist would be much cheaper and would have passed against the
 * previous version of this module, which registered nothing at runtime — the registrar existed but
 * nothing ever invoked it. So these tests transfer a file.
 */
class SftpClientAutoConfigurationTest {

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
                .withConfiguration(AutoConfigurations.of(SftpClientAutoConfiguration.class));
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

    @Test
    void registersNoClientsWhenNothingIsConfigured() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(SftpClient.class)).isEmpty();
            // The executor is unconditional, so an application can supply its own either way.
            assertThat(context).hasSingleBean(ExecutorService.class);
        });
    }

    @Test
    void registersABeanPerEndpointNamedAfterTheKey() {
        runner().withPropertyValues(endpoint("bank-a"))
                .withPropertyValues(endpoint("bank-b"))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasBean("sftpClient-bank-a");
                    assertThat(context).hasBean("sftpClient-bank-b");
                    assertThat(context).hasBean("reactiveSftpClient-bank-a");
                    assertThat(context).hasBean("reactiveSftpClient-bank-b");
                    assertThat(context.getBeansOfType(SftpClient.class)).hasSize(2);
                });
    }

    /** The single-endpoint case should not force a qualifier on anyone. */
    @Test
    void marksTheSoleEndpointPrimary() {
        runner().withPropertyValues(endpoint("only")).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SftpClient.class); // resolvable without a qualifier
            assertThat(context).hasSingleBean(ReactiveSftpClient.class);
        });
    }

    @Test
    void theRegisteredClientActuallyTransfersAFile() {
        runner().withPropertyValues(endpoint("bank-a")).run(context -> {
            assertThat(context).hasNotFailed();
            SftpClient client = context.getBean("sftpClient-bank-a", SftpClient.class);

            client.upload("/upload/spring.txt",
                    new ByteArrayInputStream("via spring".getBytes(StandardCharsets.UTF_8)));
            assertThat(client.exists("/upload/spring.txt")).isTrue();

            ByteArrayOutputStream sink = new ByteArrayOutputStream();
            long copied = client.download("/upload/spring.txt", sink);

            assertThat(copied).isEqualTo(10);
            assertThat(sink.toString(StandardCharsets.UTF_8)).isEqualTo("via spring");
            assertThat(client.capabilities().maxSftpVersion()).isEqualTo(6);
        });
    }

    @Test
    void theReactiveClientAlsoWorks() {
        runner().withPropertyValues(endpoint("bank-a")).run(context -> {
            assertThat(context).hasNotFailed();
            ReactiveSftpClient client =
                    context.getBean("reactiveSftpClient-bank-a", ReactiveSftpClient.class);

            assertThat(client.exists("/upload").toCompletableFuture().get()).isTrue();
            assertThat(client.list("/upload").toCompletableFuture().get()).isNotNull();
        });
    }

    /**
     * A missing provider artifact or an endpoint with no credentials must stop the context, naming
     * the endpoint — not surface on the first request in production.
     */
    @Test
    void failsAtStartupWhenAnEndpointNamesAnUnknownProvider() {
        runner().withPropertyValues(
                        "sftp.endpoints.broken.provider=nonexistent",
                        "sftp.endpoints.broken.host=localhost",
                        "sftp.endpoints.broken.username=u",
                        "sftp.endpoints.broken.password=p",
                        "sftp.endpoints.broken.host-key-strategy=insecure_accept_any")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .hasMessageContaining("sftp.endpoints.broken")
                        .hasMessageContaining("nonexistent"));
    }

    @Test
    void failsAtStartupWhenAnEndpointHasNoCredentials() {
        runner().withPropertyValues(
                        "sftp.endpoints.broken.provider=mina",
                        "sftp.endpoints.broken.host=localhost",
                        "sftp.endpoints.broken.username=u",
                        "sftp.endpoints.broken.host-key-strategy=insecure_accept_any")
                .run(context -> assertThat(context)
                        .hasFailed()
                        .getFailure()
                        .hasMessageContaining("sftp.endpoints.broken")
                        .hasMessageContaining("no credentials"));
    }

    /** Closing the context must release the pooled connections, not leak them. */
    @Test
    void closingTheContextClosesTheClients() {
        SftpClient[] captured = new SftpClient[1];
        runner().withPropertyValues(endpoint("bank-a")).run(context -> {
            captured[0] = context.getBean("sftpClient-bank-a", SftpClient.class);
            captured[0].exists("/upload"); // force a real connection into the pool
        });
        // The runner has closed the context by here, so the destroy method has closed the pool and
        // further borrows are refused. The surfaced type is SftpException, not the pool's
        // IllegalStateException: DefaultSftpClient translates every unchecked failure into the
        // library's single failure type, so the cause is where the detail lives.
        assertThat(captured[0]).isNotNull();
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> captured[0].exists("/upload"))
                .isInstanceOf(io.github.jtsirogiannos.sftp.client.api.SftpException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("pool is closed");
    }
}
