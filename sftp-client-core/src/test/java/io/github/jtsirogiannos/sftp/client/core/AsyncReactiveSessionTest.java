package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.provider.inmemory.InMemorySftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The same session rules over a thread-free provider, where the queue — not a worker — is what keeps
 * one connection's requests in order.
 */
public class AsyncReactiveSessionTest {

    private ExecutorService worker;
    private InMemorySftpProvider provider;
    private BoundedSftpSessionPool<AsyncSftpSession> pool;
    private AsyncReactiveSftpClient client;

    @BeforeMethod
    public void setUp() {
        worker = Executors.newFixedThreadPool(2);
        provider = new InMemorySftpProvider();
        pool = BoundedSftpSessionPool.async(provider, ObservedProvider.config(), 1,
                Duration.ofMinutes(5), Duration.ofMillis(300));
        client = new AsyncReactiveSftpClient(provider, pool, worker);
    }

    @AfterMethod
    public void tearDown() {
        client.close();
        worker.shutdownNow();
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    public void aUnitOfWorkRunsOverOneConnectionAndJoinsCallsOnTheClient() throws Exception {
        // A pool of one: the client's own call inside the work only completes if it joins.
        boolean written = await(client.withSession(ops -> client.mkdir("/joined")
                .thenCompose(v -> ops.upload("/joined/f.txt", ReactiveFlows.text("x")))
                .thenCompose(v -> ops.exists("/joined/f.txt"))));

        assertThat(written).isTrue();
        assertThat(pool.idleSessions()).isEqualTo(1);
    }

    @Test
    public void operationsIssuedTogetherRunInOrder() throws Exception {
        ReactiveSftpClientSession session = await(client.openSession());

        CompletionStage<Void> made = session.mkdir("/fan");
        CompletionStage<Void> nested = session.mkdir("/fan/sub");
        CompletionStage<Void> renamed = session.rename("/fan/sub", "/fan/moved");
        CompletionStage<Boolean> moved = session.exists("/fan/moved");
        await(CompletableFuture.allOf(made.toCompletableFuture(), nested.toCompletableFuture(),
                renamed.toCompletableFuture()));

        assertThat(await(moved)).isTrue();
        await(session.close());
    }

    @Test
    public void aTransferHasTheConnectionToItself() throws Exception {
        await(client.upload("/big.txt", ReactiveFlows.text("payload")));
        ReactiveSftpClientSession session = await(client.openSession());
        ReactiveFlows.Holding holding = new ReactiveFlows.Holding();
        session.download("/big.txt").subscribe(holding);
        holding.subscribed.get(5, TimeUnit.SECONDS);

        assertThatThrownBy(() -> await(session.exists("/big.txt")))
                .hasCauseInstanceOf(IllegalStateException.class);

        holding.subscribed.get().cancel();
        assertThat(await(session.exists("/big.txt"))).isTrue();
        await(session.close());
        assertThat(pool.idleSessions()).isEqualTo(1);
    }

    @Test
    public void aFailedTransferMeansTheConnectionIsNotReused() throws Exception {
        ReactiveSftpClientSession session = await(client.openSession());

        assertThatThrownBy(() -> await(session.upload("/broken.txt",
                ReactiveFlows.failing(new IllegalStateException("source broke")))))
                .isInstanceOf(ExecutionException.class);
        await(session.close());

        assertThat(pool.idleSessions()).isZero();
        assertThat(pool.liveSessions()).isZero();
    }
}
