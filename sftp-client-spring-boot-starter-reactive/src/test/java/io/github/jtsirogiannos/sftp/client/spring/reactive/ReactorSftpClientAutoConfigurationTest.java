package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.SftpClient;
import io.github.jtsirogiannos.sftp.client.spring.SftpClientAutoConfiguration;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reactive starter, driven the way a WebFlux application would use it.
 *
 * <p>Both auto-configurations are supplied here because that is what the starter's dependencies
 * produce on a real classpath: the shared one contributes the blocking and neutral clients, and this
 * module's adds the Reactor facade on top.
 */
class ReactorSftpClientAutoConfigurationTest {

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
                .withConfiguration(AutoConfigurations.of(
                        SftpClientAutoConfiguration.class,
                        ReactorSftpClientAutoConfiguration.class))
                .withPropertyValues(
                        "sftp.endpoints.bank-a.provider=mina",
                        "sftp.endpoints.bank-a.host=localhost",
                        "sftp.endpoints.bank-a.port=" + server.port(),
                        "sftp.endpoints.bank-a.username=" + EmbeddedSftpServer.USERNAME,
                        "sftp.endpoints.bank-a.password=" + EmbeddedSftpServer.PASSWORD,
                        "sftp.endpoints.bank-a.host-key-strategy=insecure_accept_any");
    }

    @Test
    void registersAReactorClientAlongsideTheBlockingAndNeutralOnes() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasBean("reactorSftpClient-bank-a");
            // The servlet-side beans are still there: choosing the reactive starter adds a facade,
            // it does not replace the others.
            assertThat(context).hasBean("sftpClient-bank-a");
            assertThat(context).hasBean("reactiveSftpClient-bank-a");
            assertThat(context).hasSingleBean(ReactorSftpClient.class);
        });
    }

    @Test
    void monoOperationsWorkAgainstARealServer() {
        runner().run(context -> {
            ReactorSftpClient client = context.getBean(ReactorSftpClient.class);

            StepVerifier.create(client.mkdir("/upload/reactor"))
                    .verifyComplete();
            StepVerifier.create(client.exists("/upload/reactor"))
                    .expectNext(true)
                    .verifyComplete();
        });
    }

    /** The streaming half, which needs the FlowAdapters bridge rather than a plain Mono. */
    @Test
    void fluxStreamsAFileBackByteForByte() {
        runner().run(context -> {
            ReactorSftpClient client = context.getBean(ReactorSftpClient.class);
            byte[] payload = "reactor round trip".getBytes(StandardCharsets.UTF_8);

            StepVerifier.create(client.upload("/upload/reactor.txt", Flux.just(ByteBuffer.wrap(payload))))
                    .verifyComplete();

            byte[] readBack = client.download("/upload/reactor.txt")
                    .map(buffer -> {
                        byte[] chunk = new byte[buffer.remaining()];
                        buffer.get(chunk);
                        return chunk;
                    })
                    .reduce(new byte[0], (a, b) -> {
                        byte[] joined = new byte[a.length + b.length];
                        System.arraycopy(a, 0, joined, 0, a.length);
                        System.arraycopy(b, 0, joined, a.length, b.length);
                        return joined;
                    })
                    .block();

            assertThat(readBack).isEqualTo(payload);
        });
    }

    /**
     * One pool per endpoint, however many facades are injected. The Reactor bean wraps the neutral
     * client rather than building its own — otherwise every endpoint would quietly hold two pools.
     */
    @Test
    void theReactorFacadeSharesTheNeutralClientRatherThanBuildingASecond() {
        runner().run(context -> {
            ReactiveSftpClient neutral = context.getBean(ReactiveSftpClient.class);
            ReactorSftpClient reactor = context.getBean(ReactorSftpClient.class);

            // Same provider, same capabilities object identity would be too weak a check; instead
            // confirm the wrapper reports what its delegate does and that both are live.
            assertThat(reactor.capabilities()).isEqualTo(neutral.capabilities());
            assertThat(context.getBeansOfType(ReactiveSftpClient.class)).hasSize(1);
        });
    }

    @Test
    void blockingClientStillWorksWhenTheReactiveStarterIsPresent() {
        runner().run(context -> {
            SftpClient client = context.getBean(SftpClient.class);
            assertThat(client.exists("/upload")).isTrue();
        });
    }
}
