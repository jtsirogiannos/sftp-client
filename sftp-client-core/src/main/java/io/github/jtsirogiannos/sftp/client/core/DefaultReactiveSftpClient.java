package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Reactive facade over the blocking provider contract, in JDK types only.
 *
 * <p>Metadata ops are worker-offloaded blocking calls — nobody needs a "reactive mkdir" for
 * throughput; they exist so an event-loop caller never blocks. Only the transfer path is genuinely
 * streamed, via {@link InputStreamPublisher}, and even that has a worker parked on the socket. That
 * is the honest ceiling for a blocking provider, and it is why {@code pipelinedStreaming} is not
 * called "async".
 *
 * <p>The routing is {@link ContextualReactiveSftpClient}'s; per-call behaviour is
 * {@link PerCallReactiveSftpOperations}'; sessions are {@link QueuedReactiveSession}s over a
 * {@link BlockingSessionTarget}.
 *
 * <p>A provider that opens {@link io.github.jtsirogiannos.sftp.client.spi.AsyncSftpSession}s gets
 * {@link AsyncReactiveSftpClient} instead — a separate class over a separate pool, chosen once in
 * {@code SftpClients}. There is deliberately no branching here: the two session contracts are
 * siblings, so the choice is made by which client was built, not re-decided per operation.
 */
public class DefaultReactiveSftpClient extends ContextualReactiveSftpClient {

    private final SftpProvider provider;
    private final SftpSessionPool<SftpSession> pool;
    private final Executor worker;
    private final PerCallReactiveSftpOperations perCall;

    /** A client whose current session is remembered per thread. */
    public DefaultReactiveSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool,
            Executor worker) {
        this(provider, pool, worker, ThreadLocalSftpSessionContext.instance());
    }

    public DefaultReactiveSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool,
            Executor worker, SftpSessionContext context) {
        super(context, null, null);
        this.provider = provider;
        this.pool = pool;
        this.worker = worker;
        this.perCall = new PerCallReactiveSftpOperations(provider, pool, worker);
    }

    @Override
    protected ReactiveSftpOperations perCall() {
        return perCall;
    }

    @Override
    protected QueuedReactiveSession newSession() {
        return new QueuedReactiveSession(
                pool.borrowAsync(worker).thenApply(session ->
                        new BlockingSessionTarget(session, pool, worker, perCall.chunk())),
                provider.capabilities());
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        QueuedReactiveSession session = newSession();
        return session.ready().thenApply(ready -> session);
    }

    @Override
    public void close() {
        // The pool is ours; the worker executor is not - it was handed in and is usually shared.
        pool.close();
    }
}
