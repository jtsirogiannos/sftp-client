package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;

import java.util.concurrent.CompletionStage;

/**
 * A reactive session whose operations go through a decorator and whose lifecycle stays with the
 * session underneath — {@link ForwardingSftpClientSession}, for the reactive facade.
 */
public abstract class ForwardingReactiveSftpClientSession extends ForwardingReactiveSftpOperations
        implements ReactiveSftpClientSession {

    /** The real session: what {@link #isOpen()} and {@link #close()} answer to. */
    protected abstract ReactiveSftpClientSession session();

    @Override
    public boolean isOpen() {
        return session().isOpen();
    }

    @Override
    public CompletionStage<Void> close() {
        return session().close();
    }
}
