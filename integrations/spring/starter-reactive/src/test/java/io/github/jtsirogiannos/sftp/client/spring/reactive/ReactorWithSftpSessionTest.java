package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.WithSftpSession;
import io.github.jtsirogiannos.sftp.client.spring.SftpClientAutoConfiguration;
import io.github.jtsirogiannos.sftp.client.tck.EmbeddedSftpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code @WithSftpSession} on methods returning {@code Mono} and {@code Flux}: the session is current
 * for the whole pipeline the method returns, and a call on the injected client anywhere in it — on any
 * scheduler — joins. The endpoint has a pool of one, so a call that failed to join would time out.
 */
class ReactorWithSftpSessionTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
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
            .withConfiguration(AutoConfigurations.of(
                SftpClientAutoConfiguration.class, ReactorSftpClientAutoConfiguration.class))
            .withPropertyValues(
                "sftp.endpoints.bank-a.provider=mina",
                "sftp.endpoints.bank-a.host=localhost",
                "sftp.endpoints.bank-a.port=" + server.port(),
                "sftp.endpoints.bank-a.username=" + EmbeddedSftpServer.USERNAME,
                "sftp.endpoints.bank-a.password=" + EmbeddedSftpServer.PASSWORD,
                "sftp.endpoints.bank-a.host-key-strategy=insecure_accept_any",
                "sftp.endpoints.bank-a.pool.max-size=1",
                "sftp.endpoints.bank-a.pool.acquire-timeout=300ms")
            .withBean(Publisher.class);
    }

    public static class Publisher {

        private final ReactorSftpClient client;

        public Publisher(ReactorSftpClient client) {
            this.client = client;
        }

        @WithSftpSession
        public Mono<Boolean> publish(String dir) {
            return client.mkdir(dir)
                .then(Mono.defer(() -> client.mkdir(dir + "/hopped")).subscribeOn(Schedulers.boundedElastic()))
                .then(client.exists(dir + "/hopped"));
        }

        @WithSftpSession("bank-a")
        public Flux<String> listAfterMaking(String dir) {
            return client.mkdirs(dir + "/a")
                .then(client.mkdir(dir + "/b"))
                .thenMany(client.list(dir).flatMapIterable(entries -> entries))
                .map(entry -> entry.name());
        }

        @WithSftpSession
        public Mono<Boolean> sessionIsCurrent() {
            return Mono.deferContextual(context ->
                Mono.just(!SftpReactorContext.bindings(context).isEmpty()));
        }

        @WithSftpSession
        public Mono<Void> fails() {
            return Mono.error(new IllegalStateException("the method's own"));
        }
    }

    @Test
    void aMonoMethodsCallsJoinItsSessionAcrossSchedulers() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            Publisher publisher = context.getBean(Publisher.class);
            assertThat(publisher.sessionIsCurrent().block(WAIT)).isTrue();
            assertThat(publisher.publish("/upload/reactor-annotated-" + UNIQUE.incrementAndGet())
                .block(WAIT)).isTrue();
        });
    }

    @Test
    void aFluxMethodRunsInASessionToo() {
        runner().run(context -> {
            List<String> names = context.getBean(Publisher.class)
                .listAfterMaking("/upload/reactor-annotated-" + UNIQUE.incrementAndGet())
                .collectList().block(WAIT);
            assertThat(names).containsExactlyInAnyOrder("a", "b");
        });
    }

    @Test
    void theMethodsOwnErrorArrivesUnchanged() {
        runner().run(context -> {
            Publisher publisher = context.getBean(Publisher.class);
            assertThat(publisher.fails().onErrorResume(e -> Mono.empty()).hasElement().block(WAIT))
                .isFalse();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.fails().block(WAIT))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("the method's own");
        });
    }
}
