package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

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
 * <p>The routing is {@link ContextualReactiveSftpClient}'s; per-call behaviour is
 * {@link PerCallAsyncReactiveSftpOperations}'; sessions are {@link QueuedReactiveSession}s over an
 * {@link AsyncSessionTarget}.
 *
 * <p>The {@code worker} is still needed, but only for the parts that genuinely block: opening a new
 * connection and waiting for a slot when the pool is at capacity. It is passed to
 * {@link SftpSessionPool#borrowAsync}, which uses it only when it must.
 */
public class AsyncReactiveSftpClient extends ContextualReactiveSftpClient {

    private final AsyncSftpProvider provider;
    private final SftpSessionPool<AsyncSftpSession> pool;
    private final Executor worker;
    private final PerCallAsyncReactiveSftpOperations perCall;

    /** A client whose current session is remembered per thread. */
    public AsyncReactiveSftpClient(AsyncSftpProvider provider,
            SftpSessionPool<AsyncSftpSession> pool, Executor worker) {
        this(provider, pool, worker, ThreadLocalSftpSessionContext.instance());
    }

    public AsyncReactiveSftpClient(AsyncSftpProvider provider,
            SftpSessionPool<AsyncSftpSession> pool, Executor worker, SftpSessionContext context) {
        super(context, null, null);
        this.provider = provider;
        this.pool = pool;
        this.worker = worker;
        this.perCall = new PerCallAsyncReactiveSftpOperations(provider, pool, worker);
    }

    @Override
    protected ReactiveSftpOperations perCall() {
        return perCall;
    }

    @Override
    protected QueuedReactiveSession newSession() {
        return new QueuedReactiveSession(
                pool.borrowAsync(worker).thenApply(session -> new AsyncSessionTarget(session, pool, worker)),
                provider.capabilities());
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        QueuedReactiveSession session = newSession();
        return session.ready().thenApply(ready -> session);
    }

    @Override
    public void close() {
        // The pool is ours; the worker executor is not — it was handed in and is usually shared.
        pool.close();
    }
}
