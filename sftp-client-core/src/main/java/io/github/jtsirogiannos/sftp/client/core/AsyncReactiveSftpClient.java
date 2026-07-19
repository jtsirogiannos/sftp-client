package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.RemoteFile;
import io.github.jtsirogiannos.sftp.client.api.SftpException;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpCapabilities;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * The reactive facade over a provider that does not park threads.
 *
 * <p>The counterpart to {@link DefaultReactiveSftpClient}, and the reason both exist as separate
 * classes rather than one class with a branch: {@link AsyncSftpSession} and {@link SftpSession} are
 * sibling contracts, so a client is built over one or the other and never has to ask at run time
 * which it got. {@code SftpClients} makes the choice once, when the client is constructed.
 *
 * <p>What that buys, concretely: no operation here offloads anything. A request is issued on
 * whichever thread asked for it and the reply completes the stage from the provider's I/O thread, so
 * a warm pool serves a whole metadata operation without a worker being involved at all. Downloads
 * come straight off the provider's own publisher via {@link AsyncDownloadPublisher}, with demand as
 * the read window rather than a buffer size the core picked.
 *
 * <p>The {@code worker} is still needed, but only for the parts that genuinely block: opening a new
 * connection and waiting for a slot when the pool is at capacity. It is passed to
 * {@link SftpSessionPool#borrowAsync}, which uses it only when it must.
 */
public class AsyncReactiveSftpClient implements ReactiveSftpClient {

    private final AsyncSftpProvider provider;
    private final SftpSessionPool<AsyncSftpSession> pool;
    private final Executor worker;

    public AsyncReactiveSftpClient(AsyncSftpProvider provider,
            SftpSessionPool<AsyncSftpSession> pool, Executor worker) {
        this.provider = provider;
        this.pool = pool;
        this.worker = worker;
    }

    @Override
    public CompletionStage<List<RemoteFile>> list(String path) {
        return onSession(session -> session.listAsync(path));
    }

    @Override
    public CompletionStage<RemoteFile> stat(String path) {
        return onSession(session -> session.statAsync(path));
    }

    @Override
    public CompletionStage<Boolean> exists(String path) {
        return onSession(session -> session.existsAsync(path));
    }

    @Override
    public CompletionStage<Void> mkdir(String path) {
        return onSession(session -> session.mkdirAsync(path));
    }

    @Override
    public CompletionStage<Void> rename(String from, String to) {
        return onSession(session -> session.renameAsync(from, to));
    }

    @Override
    public CompletionStage<Void> remove(String path) {
        return onSession(session -> session.removeAsync(path));
    }

    @Override
    public Flow.Publisher<ByteBuffer> download(String remotePath) {
        // Borrowed lazily on subscribe, so a publisher nobody subscribes to costs nothing.
        return new AsyncDownloadPublisher(remotePath, 0, () -> pool.borrowAsync(worker),
            pool::discard);
    }

    @Override
    public CompletionStage<Void> upload(String remotePath, Flow.Publisher<ByteBuffer> data) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        pool.borrowAsync(worker).whenComplete((session, borrowFailure) -> {
            if (borrowFailure != null) {
                result.completeExceptionally(unwrap(borrowFailure));
                return;
            }
            if (result.isDone()) {
                // Cancelled while the borrow was in flight. Hand the session straight back rather
                // than opening a remote file for a transfer nobody is waiting for.
                pool.discard(session);
                return;
            }
            try {
                session.writeAsync(remotePath, data, SftpSession.WriteMode.OVERWRITE)
                    .whenComplete((ignored, failure) -> {
                        // Mid-transfer on every route, so the session never goes back to the pool.
                        pool.discard(session);
                        if (failure == null) {
                            result.complete(null);
                        } else {
                            result.completeExceptionally(unwrap(failure));
                        }
                    });
            } catch (RuntimeException e) {
                pool.discard(session);
                result.completeExceptionally(unwrap(e));
            }
        });
        return result;
    }

    @Override
    public SftpCapabilities capabilities() {
        return provider.capabilities();
    }

    @Override
    public void close() {
        // The pool is ours; the worker executor is not — it was handed in and is usually shared.
        pool.close();
    }

    /**
     * Borrows, issues, and settles the session on completion.
     *
     * <p>Nothing here hops threads. Issuing does not block, so it runs wherever the borrow completed
     * — often the caller's own thread, since a warm pool answers inline.
     */
    private <T> CompletionStage<T> onSession(Function<AsyncSftpSession, CompletionStage<T>> op) {
        CompletableFuture<T> future = new CompletableFuture<>();
        pool.borrowAsync(worker).whenComplete((session, borrowFailure) -> {
            if (borrowFailure != null) {
                future.completeExceptionally(unwrap(borrowFailure));
                return;
            }
            try {
                op.apply(session).whenComplete((value, failure) -> {
                    if (failure == null) {
                        pool.recycle(session);
                        future.complete(value);
                    } else {
                        pool.discard(session);
                        future.completeExceptionally(unwrap(failure));
                    }
                });
            } catch (RuntimeException e) {
                // A provider that throws instead of returning a failed stage still must not leak
                // the session.
                pool.discard(session);
                future.completeExceptionally(unwrap(e));
            }
        });
        return future;
    }

    /**
     * Normalises a failure to {@link SftpException}, matching what the blocking client produces.
     *
     * <p>Callers must not be able to tell which kind of provider they were given from the exception
     * they catch — that would make an implementation detail part of the API. The
     * {@link CompletionException} unwrapping matters for the same reason: a stage that failed through
     * a dependent step arrives wrapped, and a directly-failed one does not.
     */
    private static Throwable unwrap(Throwable failure) {
        Throwable cause = failure instanceof CompletionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
        return cause instanceof SftpException ? cause : new SftpException("operation failed", cause);
    }
}
