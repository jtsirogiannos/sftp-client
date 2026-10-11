package io.github.jtsirogiannos.sftp.client.spring.reactive;

import io.github.jtsirogiannos.sftp.client.api.ReactiveSftpClientSession;
import io.github.jtsirogiannos.sftp.client.context.SftpSessionContext;

import reactor.core.publisher.Mono;

/**
 * An open session: every operation pinned to one connection until {@link #close()} — the Reactor
 * view of {@link ReactiveSftpClientSession}. Operations run one at a time in the order issued; a
 * transfer has the connection to itself.
 */
public class ReactorSftpSession extends ReactorSftpOperations {

    private final ReactiveSftpClientSession session;

    ReactorSftpSession(ReactiveSftpClientSession session, SftpSessionContext context) {
        super(session, context);
        this.session = session;
    }

    /** True until {@link #close()} has been called. */
    public boolean isOpen() {
        return session.isOpen();
    }

    /** Hands the connection back once the operations already issued have finished. Idempotent. */
    public Mono<Void> close() {
        return Mono.fromCompletionStage(session::close);
    }
}
