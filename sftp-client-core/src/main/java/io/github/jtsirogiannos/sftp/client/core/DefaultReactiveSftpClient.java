package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.AttributeUpdate;
import io.github.jtsirogiannos.sftp.client.api.FilesystemStats;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactive facade over the blocking provider contract, in JDK types only.
 *
 * <p>Metadata ops are worker-offloaded blocking calls — nobody needs a "reactive mkdir" for
 * throughput; they exist so an event-loop caller never blocks. Only the transfer path is genuinely
 * streamed, via {@link InputStreamPublisher}, and even that has a worker parked on the socket. That
 * is the honest ceiling for a blocking provider, and it is why {@code pipelinedStreaming} is not
 * called "async".
 *
 * <p>A provider that opens {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession}s gets
 * {@link AsyncReactiveSftpClient} instead — a separate class over a separate pool, chosen once in
 * {@code SftpClients}. There is deliberately no branching here: the two session contracts are
 * siblings, so the choice is made by which client was built, not re-decided per operation.
 */
public class DefaultReactiveSftpClient implements ReactiveSftpClient {

    // MINA pipelines transfers internally, so a larger scratch buffer lets the pull thread consume
    // prefetched data in fewer hops. JSch stop-and-waits, so a smaller buffer keeps per-round-trip
    // latency from dominating. Same read path either way.
    private static final int PIPELINED_CHUNK = 256 * 1024;
    private static final int STOP_AND_WAIT_CHUNK = 32 * 1024;

    private final SftpProvider provider;
    private final SftpSessionPool<SftpSession> pool;
    private final Executor worker;

    public DefaultReactiveSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool,
            Executor worker) {
        this.provider = provider;
        this.pool = pool;
        this.worker = worker;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return onSession(s -> s.list(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return onSession(s -> s.stat(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return onSession(s -> s.exists(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return onSessionVoid(s -> s.mkdir(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return onSessionVoid(s -> s.rename(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return onSessionVoid(s -> s.remove(path));
    }

    @Override
    public CompletionStage<RemoteFile> lstat(String path) {
        return onSession(s -> s.lstat(path));
    }

    @Override
    public CompletionStage<String> realpath(String path) {
        return onSession(s -> s.realpath(path));
    }

    @Override
    public CompletionStage<Void> symlink(String linkPath, String targetPath) {
        return onSessionVoid(s -> s.symlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<String> readlink(String linkPath) {
        return onSession(s -> s.readlink(linkPath));
    }

    @Override
    public CompletionStage<Void> hardlink(String linkPath, String targetPath) {
        return onSessionVoid(s -> s.hardlink(linkPath, targetPath));
    }

    @Override
    public CompletionStage<Void> setAttributes(String path, AttributeUpdate update) {
        return onSessionVoid(s -> s.setAttributes(path, update));
    }

    @Override
    public CompletionStage<FilesystemStats> statvfs(String path) {
        return onSession(s -> s.statvfs(path));
    }

    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath, long fromOffset) {
        int chunk = provider.capabilities().pipelinedStreaming() ? PIPELINED_CHUNK : STOP_AND_WAIT_CHUNK;
        // Borrowed lazily inside the supplier so nothing leaks if the publisher is never subscribed.
        SessionHolder holder = new SessionHolder();
        return new InputStreamPublisher(
            () -> {
                SftpSession session = pool.borrow();
                holder.session = session;
                return session.openRead(remotePath, fromOffset);
            },
            chunk,
            worker,
            failure -> {
                // Mid-transfer sessions aren't safe to reuse, on success, failure or cancel.
                if (holder.session != null) {
                    pool.discard(holder.session);
                }
            });
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data,
            SftpSession.WriteMode mode) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        worker.execute(() -> {
            if (result.isDone()) {
                // Cancelled before this task ran: nothing has been borrowed, so there is nothing to
                // release, and opening a remote file for a transfer nobody wants is pure waste.
                return;
            }
            SftpSession session;
            OutputStream out;
            try {
                session = pool.borrow();
                out = session.openWrite(remotePath, mode);
            } catch (RuntimeException e) {
                result.completeExceptionally(e);
                return;
            }
            // Demand-pacing subscriber: request(1), write it, request the next. A slow remote
            // therefore throttles the source instead of buffering the whole file in memory.
            data.subscribe(new WriteSubscriber(out, session, pool, worker, result));
        });
        return result;
    }

    @Override
    public SftpCapabilities capabilities() {
        return provider.capabilities();
    }

    @Override
    public void close() {
        // The pool is ours; the worker executor is not - it was handed in and is usually shared.
        pool.close();
    }

    private <T> CompletionStage<T> onSession(Function<SftpSession, T> op) {
        CompletableFuture<T> future = new CompletableFuture<>();
        worker.execute(() -> {
            SftpSession s = null;
            try {
                s = pool.borrow();
                T value = op.apply(s);
                pool.recycle(s);
                future.complete(value);
            } catch (RuntimeException e) {
                if (s != null) {
                    pool.discard(s);
                }
                future.completeExceptionally(e instanceof SftpException ? e
                    : new SftpException("operation failed", e));
            }
        });
        return future;
    }

    private CompletionStage<Void> onSessionVoid(Consumer<SftpSession> op) {
        return onSession(s -> {
            op.accept(s);
            return null;
        });
    }

    /**
     * Lets the publisher's terminate hook see the session the supplier borrowed.
     */
    private static final class SessionHolder {
        volatile SftpSession session;
    }
}
