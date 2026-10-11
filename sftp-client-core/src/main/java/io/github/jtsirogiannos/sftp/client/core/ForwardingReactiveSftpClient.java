package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClient;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpOperations;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Forwards every operation, and the client's own methods, to another {@link ReactiveSftpClient}. A
 * base for decorators; see {@link ForwardingReactiveSftpOperations} for what is and is not forwarded.
 *
 * <p>Sessions are forwarded untouched, so a decorator built on this observes per-call traffic only.
 * One that should see a unit of work's operations too overrides {@link #withSession} and
 * {@link #openSession} to wrap the session it is handed — the decorators in the observability
 * modules do.
 */
public abstract class ForwardingReactiveSftpClient extends ForwardingReactiveSftpOperations
        implements ReactiveSftpClient {

    /** The client every call is forwarded to. Called per operation, so keep it cheap. */
    @Override
    protected abstract ReactiveSftpClient delegate();

    @Override
    public <R> CompletionStage<R> withSession(
            Function<ReactiveSftpOperations, ? extends CompletionStage<R>> work) {
        return delegate().withSession(work);
    }

    @Override
    public CompletionStage<ReactiveSftpClientSession> openSession() {
        return delegate().openSession();
    }

    @Override
    public SftpSessionContext sessionContext() {
        return delegate().sessionContext();
    }

    @Override
    public void close() {
        delegate().close();
    }
}
