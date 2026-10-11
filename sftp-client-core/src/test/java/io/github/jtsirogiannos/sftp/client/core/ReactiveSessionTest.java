package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.BoundedSftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reactive sessions over a blocking provider: one connection for a unit of work, operations run in
 * order on a multi-threaded worker, a transfer holding the connection to itself, and the current
 * session joined by calls made while it is current.
 */
public class ReactiveSessionTest {

    private ExecutorService worker;
    private ObservedProvider provider;

    @BeforeMethod
    public void setUp() {
        worker = Executors.newFixedThreadPool(4);
        provider = new ObservedProvider();
    }

    @AfterMethod
    public void tearDown() {
        worker.shutdownNow();
    }

    private DefaultReactiveSftpClient client(BoundedSftpSessionPool<SftpSession> pool) {
        return new DefaultReactiveSftpClient(provider, pool, worker);
    }

    private static <T> T await(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    public void aUnitOfWorkRunsOverOneConnectionAndHandsItBackWhenDone() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {

            boolean written = await(client.withSession(ops -> ops.mkdirs("/out/2026")
                    .thenCompose(v -> ops.upload("/out/2026/report.csv", ReactiveFlows.text("a,b")))
                    .thenCompose(v -> ops.exists("/out/2026/report.csv"))));

            assertThat(written).isTrue();
            assertThat(provider.sessions)
                    .describedAs("one connection; an upload inside a session does not discard it")
                    .hasSize(1);
            assertThat(pool.idleSessions()).describedAs("handed back before the result").isEqualTo(1);
        }
    }

    @Test
    public void operationsIssuedTogetherRunOneAtATimeInOrder() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            ReactiveSftpClientSession session = await(client.openSession());

            // Issued without waiting for each other, on a four-thread worker: each depends on the one
            // before it having run, so any reordering fails.
            List<CompletableFuture<?>> issued = new ArrayList<>();
            issued.add(session.mkdir("/fan").toCompletableFuture());
            issued.add(session.mkdir("/fan/sub").toCompletableFuture());
            issued.add(session.rename("/fan/sub", "/fan/moved").toCompletableFuture());
            issued.add(session.mkdir("/fan/moved/deeper").toCompletableFuture());
            CompletableFuture<Boolean> moved = session.exists("/fan/moved/deeper").toCompletableFuture();
            issued.add(moved);
            await(CompletableFuture.allOf(issued.toArray(CompletableFuture[]::new)));

            assertThat(moved.get()).isTrue();
            await(session.close());
            assertThat(pool.idleSessions()).isEqualTo(1);
        }
    }

    /**
     * Not queued behind the transfer, refused: a call queued behind an upload and awaited by the
     * upload's own source would never run. Chaining after the upload is the way to sequence it.
     */
    @Test
    public void aCallIssuedWhileAnUploadIsInFlightIsRefusedAndChainingWorks() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            ReactiveSftpClientSession session = await(client.openSession());
            java.util.concurrent.SubmissionPublisher<java.nio.ByteBuffer> source =
                    new java.util.concurrent.SubmissionPublisher<>();

            CompletionStage<Void> uploading = session.upload("/slow.txt", source);
            assertThatThrownBy(() -> await(session.rename("/slow.txt", "/fast.txt")))
                    .hasCauseInstanceOf(IllegalStateException.class);

            source.submit(java.nio.ByteBuffer.wrap("done".getBytes()));
            source.close();
            await(uploading.thenCompose(v -> session.rename("/slow.txt", "/fast.txt")));
            assertThat(await(session.exists("/fast.txt"))).isTrue();
            await(session.close());
        }
    }

    @Test
    public void aDownloadHasTheConnectionToItselfUntilItEnds() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            await(client.upload("/big.txt", ReactiveFlows.text("payload")));
            ReactiveSftpClientSession session = await(client.openSession());

            ReactiveFlows.Holding holding = new ReactiveFlows.Holding();
            session.download("/big.txt").subscribe(holding);
            holding.subscribed.get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> await(session.stat("/big.txt")))
                    .describedAs("refused at once rather than queued behind the read")
                    .hasCauseInstanceOf(IllegalStateException.class);

            holding.subscribed.get().cancel(); // abandon the read: the connection stays usable
            assertThat(await(session.stat("/big.txt")).size()).isEqualTo(7);
            await(session.close());
            assertThat(pool.idleSessions()).isEqualTo(1);
        }
    }

    @Test
    public void aFailedTransferMeansTheConnectionIsNotReused() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            ReactiveSftpClientSession session = await(client.openSession());

            assertThatThrownBy(() -> await(session.upload("/broken.txt",
                    ReactiveFlows.failing(new IllegalStateException("source broke")))))
                    .isInstanceOf(ExecutionException.class);
            await(session.close());

            assertThat(pool.idleSessions()).isZero();
            assertThat(provider.last().closed).isTrue();
        }
    }

    @Test
    public void closingWhileADownloadStreamsAbandonsItAndDiscardsTheConnection() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            await(client.upload("/big.txt", ReactiveFlows.text("payload")));
            ReactiveSftpClientSession session = await(client.openSession());
            ReactiveFlows.Holding holding = new ReactiveFlows.Holding();
            session.download("/big.txt").subscribe(holding);
            holding.subscribed.get(5, TimeUnit.SECONDS);

            await(session.close()); // must not wait for a read nobody is pulling

            assertThat(pool.idleSessions()).isZero();
            assertThat(provider.last().closed).isTrue();
            assertThatThrownBy(() -> await(session.exists("/"))).hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void theWorksOwnFailureArrivesUnchangedAfterAHealthyConnectionIsHandedBack() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(4);
             DefaultReactiveSftpClient client = client(pool)) {
            IllegalStateException rejected = new IllegalStateException("order rejected");

            CompletableFuture<Object> outcome = client.withSession(ops -> ops.mkdir("/w")
                    .thenCompose(v -> CompletableFuture.failedFuture(rejected))).toCompletableFuture();

            assertThatThrownBy(() -> outcome.get(10, TimeUnit.SECONDS)).hasCause(rejected);
            assertThat(pool.idleSessions()).isEqualTo(1);

            CompletableFuture<Object> thrown = client.withSession(ops -> {
                throw rejected;
            }).toCompletableFuture();
            assertThatThrownBy(() -> thrown.get(10, TimeUnit.SECONDS)).hasCause(rejected);
        }
    }

    // --- the current session ---

    /**
     * On a pool of one, a client call that failed to join would wait for the connection the work is
     * holding and time out — so the work completing at all is the proof.
     */
    @Test
    public void callsOnTheClientInTheWorksSynchronousPartJoinItsSession() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultReactiveSftpClient client = client(pool)) {

            await(client.withSession(ops -> client.mkdir("/joined")));

            assertThat(await(client.exists("/joined"))).isTrue();
            assertThat(provider.sessions).hasSize(1);
        }
    }

    @Test
    public void aNestedUnitOfWorkJoinsTheOuterOne() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultReactiveSftpClient client = client(pool)) {

            await(client.withSession(outer -> client.withSession(inner -> inner.mkdir("/nested"))));

            assertThat(await(client.exists("/nested"))).isTrue();
        }
    }

    @Test
    public void downloadIsNeverRouted() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultReactiveSftpClient client = client(pool)) {
            await(client.upload("/src.txt", ReactiveFlows.text("copied")));

            // The usual reason to download inside a unit of work: to feed an upload over its session.
            await(client.withSession(ops -> ops.upload("/dst.txt", client.download("/src.txt"))));

            assertThat(await(ReactiveFlows.collect(client.download("/dst.txt")))).isEqualTo("copied");
        }
    }

    @Test
    public void openSessionFailsWhenNoConnectionCanBeHad() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(1);
             DefaultReactiveSftpClient client = client(pool)) {
            ReactiveSftpClientSession held = await(client.openSession());

            assertThatThrownBy(() -> await(client.openSession()))
                    .hasCauseInstanceOf(SftpException.class)
                    .hasMessageContaining("all 1 are in use");
            await(held.close());
        }
    }

    @Test
    public void closeIsIdempotentAndOperationsAfterItFail() throws Exception {
        try (BoundedSftpSessionPool<SftpSession> pool = provider.pool(2);
             DefaultReactiveSftpClient client = client(pool)) {
            ReactiveSftpClientSession session = await(client.openSession());

            CompletionStage<Void> first = session.close();
            assertThat(session.close()).isSameAs(first);
            await(first);

            assertThat(session.isOpen()).isFalse();
            assertThatThrownBy(() -> await(session.mkdir("/late")))
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(pool.idleSessions()).isEqualTo(1);
        }
    }
}
