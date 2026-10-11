package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.SftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;
import io.github.jtsirogiannos.sftp.client.pool.SftpSessionPool;
import io.github.jtsirogiannos.sftp.client.spi.SftpProvider;
import io.github.jtsirogiannos.sftp.client.spi.SftpSession;

/**
 * Blocking facade over the provider contract: a call joins the current session if there is one, and
 * otherwise borrows, runs one operation, and returns.
 *
 * <p>The routing is {@link ContextualSftpClient}'s; the per-call behaviour is
 * {@link PerCallSftpOperations}'; sessions are {@link PooledSftpClientSession}s. This class only
 * wires the three to one pool.
 *
 * <p>Deliberately not implemented by delegating to {@link DefaultReactiveSftpClient} and blocking on
 * the result — that would drag an {@link java.util.concurrent.Executor} into the blocking path and
 * turn every call into a thread hand-off. The two facades share the pool, not the code.
 */
public class DefaultSftpClient extends ContextualSftpClient {

    private final SftpSessionPool<SftpSession> pool;
    private final PerCallSftpOperations perCall;

    /** A client whose current session is remembered per thread. */
    public DefaultSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool) {
        this(provider, pool, ThreadLocalSftpSessionContext.instance());
    }

    public DefaultSftpClient(SftpProvider provider, SftpSessionPool<SftpSession> pool,
            SftpSessionContext context) {
        super(context, null, null);
        this.pool = pool;
        this.perCall = new PerCallSftpOperations(provider, pool);
    }

    @Override
    protected SftpOperations perCall() {
        return perCall;
    }

    @Override
    protected SftpClientSession newSession() {
        return new PooledSftpClientSession(pool.borrow(), pool);
    }

    @Override
    public void close() {
        pool.close();
    }
}
