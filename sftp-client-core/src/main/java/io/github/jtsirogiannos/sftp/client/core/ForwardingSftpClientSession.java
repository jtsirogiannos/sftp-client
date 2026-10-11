package io.github.jtsirogiannos.sftp.client.core;

import io.github.jtsirogiannos.sftp.client.api.SftpClientSession;

/**
 * A session whose operations go through a decorator and whose lifecycle stays with the session
 * underneath. A base for decorators that wrap {@code openSession()}.
 *
 * <p>The split exists because the two halves have different owners. A metering or tracing decorator
 * wraps the <em>operations</em> — that is what it observes — but closing must still reach the real
 * session, or its connection never goes back to the pool. Subclasses supply both; anything that has
 * to happen at close, such as ending a span, overrides {@link #close()} and calls {@code super}.
 *
 * <p>As with {@link ForwardingSftpOperations}, only the primitives are forwarded: the convenience
 * methods run here and their constituent calls pass through the decorator one by one.
 */
public abstract class ForwardingSftpClientSession extends ForwardingSftpOperations
        implements SftpClientSession {

    /** The real session: what {@link #isOpen()} and {@link #close()} answer to. */
    protected abstract SftpClientSession session();

    @Override
    public boolean isOpen() {
        return session().isOpen();
    }

    @Override
    public void close() {
        session().close();
    }
}
