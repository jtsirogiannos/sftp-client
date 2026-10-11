package io.github.jtsirogiannos.sftp.client.quarkus.runtime;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.smallrye.mutiny.Uni;

/**
 * An open session: every operation pinned to one connection until {@link #close()} — the Mutiny
 * view of {@link ReactiveSftpClientSession}. Operations run one at a time in the order issued; a
 * transfer has the connection to itself.
 */
public class MutinySftpSession extends MutinySftpOperations {

    private final ReactiveSftpClientSession session;

    MutinySftpSession(ReactiveSftpClientSession session) {
        super(session);
        this.session = session;
    }

    /** True until {@link #close()} has been called. */
    public boolean isOpen() {
        return session.isOpen();
    }

    /** Hands the connection back once the operations already issued have finished. Idempotent. */
    public Uni<Void> close() {
        return call(session::close);
    }
}
